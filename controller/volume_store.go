/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/container-object-storage-interface/api/apis/cosi.sigs.k8s.io/v1alpha1"
	cosiclnt "github.com/container-object-storage-interface/api/clientset/typed/cosi.sigs.k8s.io/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

// BucketStore is an interface that's used to save Buckets to API server.
// Implementation of the interface add custom error recovery policy.
// A bucket is added via StoreBucket(). It's enough to store the bucket only once.
// It is not possible to remove a bucket, even when corresponding PVC is deleted
// and PV is not necessary any longer. PV will be always created.
// If corresponding PVC is deleted, the PV will be deleted by Kubernetes using
// standard deletion procedure. It saves us some code here.
type BucketStore interface {
	// StoreBucket makes sure a bucket is saved to Kubernetes API server.
	// If no error is returned, caller can assume that PV was saved or
	// is being saved in background.
	// In error is returned, no PV was saved and corresponding PVC needs
	// to be re-queued (so whole provisioning needs to be done again).
	StoreBucket(bucketRequest *v1alpha1.BucketRequest, bucket *v1alpha1.Bucket) error

	// Runs any background goroutines for implementation of the interface.
	Run(ctx context.Context, threadiness int)
}

// queueStore is implementation of BucketStore that re-tries saving
// PVs to API server using a workqueue running in its own goroutine(s).
// After failed save, bucket is re-qeueued with exponential backoff.
type queueStore struct {
	client                kubernetes.Interface
	queue                 workqueue.RateLimitingInterface
	eventRecorder         record.EventRecorder
	bucketRequestsIndexer cache.Indexer

	buckets sync.Map
}

var _ BucketStore = &queueStore{}

// NewBucketStoreQueue returns BucketStore that uses asynchronous workqueue to save PVs.
func NewBucketStoreQueue(
	client kubernetes.Interface,
	limiter workqueue.RateLimiter,
	bucketRequestsIndexer cache.Indexer,
	eventRecorder record.EventRecorder,
) BucketStore {

	return &queueStore{
		client:                client,
		queue:                 workqueue.NewNamedRateLimitingQueue(limiter, "unsavedpvs"),
		bucketRequestsIndexer: bucketRequestsIndexer,
		eventRecorder:         eventRecorder,
	}
}

func (q *queueStore) StoreBucket(bucketRequest *v1alpha1.BucketRequest, bucket *v1alpha1.Bucket) error {
	fmt.Println("StoreBucket ", bucketRequest, " bucket ", bucket)
	if err := q.doSaveBucket(bucket); err != nil {
		q.buckets.Store(bucket.Name, bucket)
		q.queue.Add(bucket.Name)
		klog.Errorf("Failed to save bucket %s: %s", bucket.Name, err)
	}
	// Consume any error, this Store will retry in background.
	return nil
}

func (q *queueStore) Run(ctx context.Context, threadiness int) {
	klog.Infof("BucketStore-RUN:Starting save bucket queue %d", threadiness)
	defer q.queue.ShutDown()

	for i := 0; i < threadiness; i++ {
		go wait.Until(q.saveBucketWorker, time.Second, ctx.Done())
	}
	<-ctx.Done()
	klog.Infof("BucketStore-RUN: Stopped save bucket queue")
}

func (q *queueStore) saveBucketWorker() {
	for q.processNextWorkItem() {
	}
}

func (q *queueStore) processNextWorkItem() bool {
	obj, shutdown := q.queue.Get()
	defer q.queue.Done(obj)

        fmt.Println("BucketStore: processNextWorkItem ", obj, " shutdown ", shutdown)
	if shutdown {
		return false
	}

	var bucketName string
	var ok bool
	if bucketName, ok = obj.(string); !ok {
		q.queue.Forget(obj)
		utilruntime.HandleError(fmt.Errorf("expected string in save workqueue but got %#v", obj))
		return true
	}

	bucketObj, found := q.buckets.Load(bucketName)
	if !found {
		q.queue.Forget(bucketName)
		utilruntime.HandleError(fmt.Errorf("did not find saved bucket %s", bucketName))
		return true
	}

	bucket, ok := bucketObj.(*v1alpha1.Bucket)
	if !ok {
		q.queue.Forget(bucketName)
		utilruntime.HandleError(fmt.Errorf("saved object is not bucket: %+v", bucketObj))
		return true
	}

	if err := q.doSaveBucket(bucket); err != nil {
		q.queue.AddRateLimited(bucketName)
		utilruntime.HandleError(err)
		klog.V(5).Infof("bucket %s enqueued", bucket.Name)
		return true
	}
	q.buckets.Delete(bucketName)
	q.queue.Forget(bucketName)
	return true
}

func (q *queueStore) doSaveBucket(bucket *v1alpha1.Bucket) error {
	klog.V(5).Infof("Saving bucket %s", bucket.Name)
	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Failed to create config: %v", err)
	}
	_, err = cosiclnt.NewForConfigOrDie(config).Buckets().Create(context.Background(), bucket, metav1.CreateOptions{})
	if err == nil || apierrs.IsAlreadyExists(err) {
		klog.V(5).Infof("Bucket %s saved", bucket.Name)
		q.sendSuccessEvent(bucket)
		return nil
	}
	return fmt.Errorf("error saving bucket %s: %s", bucket.Name, err)
}

func (q *queueStore) sendSuccessEvent(bucket *v1alpha1.Bucket) {
	bucketRequestObjs, err := q.bucketRequestsIndexer.ByIndex(uidIndex, string(bucket.Name))
	if err != nil {
		klog.V(2).Infof("Error sending event to bucketRequest %s: %s", bucket.Name, err)
		return
	}
	if len(bucketRequestObjs) != 1 {
		return
	}
	bucketRequest, ok := bucketRequestObjs[0].(*v1alpha1.BucketRequest)
	if !ok {
		return
	}
	msg := fmt.Sprintf("Successfully provisioned bucket %s", bucket.Name)
	q.eventRecorder.Event(bucketRequest, v1.EventTypeNormal, "ProvisioningSucceeded", msg)
}

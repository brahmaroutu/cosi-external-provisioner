/*
Copyright 2016 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	//"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	//ref "k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/client-go/rest"
	glog "k8s.io/klog"
	"github.com/brahmaroutu/cosi-external-provisioner/util"
    cosiapi "github.com/container-object-storage-interface/api/apis/cosi.sigs.k8s.io/v1alpha1"
    "github.com/container-object-storage-interface/api/client/informers/cosi.sigs.k8s.io/v1alpha1"
    cosiclnt "github.com/container-object-storage-interface/api/client/clientset/typed/cosi.sigs.k8s.io/v1alpha1"
    cosiclient "github.com/container-object-storage-interface/api/client/clientset"
)

// annClass annotation represents the bucket class associated with a resource:
// - in BucketRequest it represents required class to match.
//   Only Buckets with the same class (i.e. annotation with the same
//   value) can be bound to the BucketRequest. In case no such bucket exists, the
//   controller will provision a new one using BucketClass instance with
//   the same name as the annotation value.
// - in Bucket it represents bucketclass to which the bucket belongs.
const annClass = "cosi.beta.kubernetes.io/bucket-class"


var (
	errStopProvision = errors.New("stop provisioning")
)

// ProvisionController is a controller that provisions Bucket for BucketRequests.
type ProvisionController struct {
	client kubernetes.Interface
	cosiclient cosiclient.Interface

	// The name of the provisioner for which this controller dynamically
	// provisions buckets. 
	provisionerName string

	// The provisioner the controller will use to provision buckets.
	provisioner Provisioner

	kubeVersion *utilversion.Version

	bucketRequestInformer  cache.SharedIndexInformer
	bucketRequestIndexer   cache.Indexer
	bucketInformer         cache.SharedInformer
	bucketClasses          cache.Store
	buckets                cache.Store


	bucketRequestQueue  workqueue.RateLimitingInterface
	bucketQueue         workqueue.RateLimitingInterface

	// Identity of this controller, generated at creation time and not persisted
	// across restarts. Useful only for debugging, for seeing the source of
	// events. controller.provisioner may have its own, different notion of
	// identity which may/may not persist across restarts
	id            string
	component     string
	eventRecorder record.EventRecorder

	resyncPeriod     time.Duration
	provisionTimeout time.Duration
	deletionTimeout  time.Duration

	rateLimiter               workqueue.RateLimiter
	exponentialBackOffOnError bool
	threadiness               int

	failedProvisionThreshold, failedDeleteThreshold int

	hasRun     bool
	hasRunLock *sync.Mutex
 
    bucketRequestsInProgress sync.Map
    bucketStore BucketStore
}

const (
	// DefaultResyncPeriod is used when option function ResyncPeriod is omitted
	DefaultResyncPeriod = 15 * time.Minute
	// DefaultThreadiness is used when option function Threadiness is omitted
	DefaultThreadiness = 4
	// DefaultExponentialBackOffOnError is used when option function ExponentialBackOffOnError is omitted
	DefaultExponentialBackOffOnError = true
	// DefaultCreateProvisionedBucketRetryCount is used when option function CreateProvisionedBucketRetryCount is omitted
	DefaultCreateProvisionedBucketRetryCount = 5
	// DefaultCreateProvisionedBucketnterval is used when option function CreateProvisionedBucketInterval is omitted
	DefaultCreateProvisionedBucketInterval = 10 * time.Second
	// DefaultFailedProvisionThreshold is used when option function FailedProvisionThreshold is omitted
	DefaultFailedProvisionThreshold = 15
	// DefaultFailedDeleteThreshold is used when option function FailedDeleteThreshold is omitted
	DefaultFailedDeleteThreshold = 15
	// DefaultLeaderElection is used when option function LeaderElection is omitted
	DefaultLeaderElection = true
	// DefaultLeaseDuration is used when option function LeaseDuration is omitted
	DefaultLeaseDuration = 15 * time.Second
	// DefaultRenewDeadline is used when option function RenewDeadline is omitted
	DefaultRenewDeadline = 10 * time.Second
	// DefaultRetryPeriod is used when option function RetryPeriod is omitted
	DefaultRetryPeriod = 2 * time.Second
	// DefaultMetricsPort is used when option function MetricsPort is omitted
	DefaultMetricsPort = 0
	// DefaultMetricsAddress is used when option function MetricsAddress is omitted
	DefaultMetricsAddress = "0.0.0.0"
	// DefaultMetricsPath is used when option function MetricsPath is omitted
	DefaultMetricsPath = "/metrics"
	// DefaultAddFinalizer is used when option function AddFinalizer is omitted
	DefaultAddFinalizer = false
	
	uidIndex = "uid"
)

var errRuntime = fmt.Errorf("cannot call option functions after controller has Run")

// ResyncPeriod is how often the controller relists bucketRequests, buckets, & bucket
// classes. OnUpdate will be called even if nothing has changed, meaning failed
// operations may be retried on a bucketRequest/bucket every resyncPeriod regardless of
// whether it changed. Defaults to 15 minutes.
func ResyncPeriod(resyncPeriod time.Duration) func(*ProvisionController) error {
	return func(c *ProvisionController) error {
		if c.HasRun() {
			return errRuntime
		}
		c.resyncPeriod = resyncPeriod
		return nil
	}
}

// Threadiness is the number of bucketRequest and bucket workers each to launch.
// Defaults to 4.
func Threadiness(threadiness int) func(*ProvisionController) error {
	return func(c *ProvisionController) error {
		if c.HasRun() {
			return errRuntime
		}
		c.threadiness = threadiness
		return nil
	}
}

// RateLimiter is the workqueue.RateLimiter to use for the provisioning and
// deleting work queues. If set, ExponentialBackOffOnError is ignored.
func RateLimiter(rateLimiter workqueue.RateLimiter) func(*ProvisionController) error {
	return func(c *ProvisionController) error {
		if c.HasRun() {
			return errRuntime
		}
		c.rateLimiter = rateLimiter
		return nil
	}
}

// ExponentialBackOffOnError determines whether to exponentially back off from
// failures of Provision and Delete. Defaults to true.
func ExponentialBackOffOnError(exponentialBackOffOnError bool) func(*ProvisionController) error {
	return func(c *ProvisionController) error {
		if c.HasRun() {
			return errRuntime
		}
		c.exponentialBackOffOnError = exponentialBackOffOnError
		return nil
	}
}


// FailedProvisionThreshold is the threshold for max number of retries on
// failures of Provision. Set to 0 to retry indefinitely. Defaults to 15.
func FailedProvisionThreshold(failedProvisionThreshold int) func(*ProvisionController) error {
	return func(c *ProvisionController) error {
		if c.HasRun() {
			return errRuntime
		}
		c.failedProvisionThreshold = failedProvisionThreshold
		return nil
	}
}

// FailedDeleteThreshold is the threshold for max number of retries on failures
// of Delete. Set to 0 to retry indefinitely. Defaults to 15.
func FailedDeleteThreshold(failedDeleteThreshold int) func(*ProvisionController) error {
	return func(c *ProvisionController) error {
		if c.HasRun() {
			return errRuntime
		}
		c.failedDeleteThreshold = failedDeleteThreshold
		return nil
	}
}

// BucketRequestInformer sets the informer to use for accessing BucketRequests.
// Defaults to using a internal informer.
func BucketRequestInformer(informer cache.SharedIndexInformer) func(*ProvisionController) error {
	return func(c *ProvisionController) error {
		if c.HasRun() {
			return errRuntime
		}
		c.bucketRequestInformer = informer
		return nil
	}
}

// BucketInformer sets the informer to use for accessing Buckets.
// Defaults to using a internal informer.
func BucketInformer(informer cache.SharedInformer) func(*ProvisionController) error {
	return func(c *ProvisionController) error {
		if c.HasRun() {
			return errRuntime
		}
		c.bucketInformer = informer
		return nil
	}
}

// ProvisionTimeout sets the amount of time that provisioning a bucket may take.
// The default is unlimited.
func ProvisionTimeout(timeout time.Duration) func(*ProvisionController) error {
	return func(c *ProvisionController) error {
		if c.HasRun() {
			return errRuntime
		}
		c.provisionTimeout = timeout
		return nil
	}
}

// HasRun returns whether the controller has Run
func (ctrl *ProvisionController) HasRun() bool {
	ctrl.hasRunLock.Lock()
	defer ctrl.hasRunLock.Unlock()
	return ctrl.hasRun
}

// NewProvisionController creates a new provision controller using
// the given configuration parameters and with private (non-shared) informers.
func NewProvisionController(
	client  kubernetes.Interface,
	cosiclient  cosiclient.Interface,
	provisionerName string,
	provisioner Provisioner,
	kubeVersion string,
	options ...func(*ProvisionController) error,
) *ProvisionController {
	id, err := os.Hostname()
	if err != nil {
		glog.Fatalf("Error getting hostname: %v", err)
	}
	// add a uniquifier so that two processes on the same host don't accidentally both become active
	id = id + "_" + string(uuid.NewUUID())
	component := provisionerName + "_" + id

	v1.AddToScheme(scheme.Scheme)
	broadcaster := record.NewBroadcaster()
	broadcaster.StartLogging(glog.Infof)
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events(v1.NamespaceAll)})
	eventRecorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: component})

	controller := &ProvisionController{
		client:                    client,
		cosiclient:                cosiclient,
		provisionerName:           provisionerName,
		provisioner:               provisioner,
		kubeVersion:               utilversion.MustParseSemantic(kubeVersion),
		id:                        id,
		component:                 component,
		eventRecorder:             eventRecorder,
		resyncPeriod:              DefaultResyncPeriod,
		exponentialBackOffOnError: DefaultExponentialBackOffOnError,
		threadiness:               DefaultThreadiness,
		failedProvisionThreshold:  DefaultFailedProvisionThreshold,
		failedDeleteThreshold:     DefaultFailedDeleteThreshold,
		hasRun:                    false,
		hasRunLock:                &sync.Mutex{},
	}

	for _, option := range options {
		err := option(controller)
		if err != nil {
			glog.Fatalf("Error processing controller options: %s", err)
		}
	}

	var rateLimiter workqueue.RateLimiter
	if controller.rateLimiter != nil {
		// rateLimiter set via parameter takes precedence
		rateLimiter = controller.rateLimiter
	} else if controller.exponentialBackOffOnError {
		rateLimiter = workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(15*time.Second, 1000*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
		)
	} else {
		rateLimiter = workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(15*time.Second, 15*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
		)
	}
	controller.bucketRequestQueue = workqueue.NewNamedRateLimitingQueue(rateLimiter, "bucketrequests")
	controller.bucketQueue = workqueue.NewNamedRateLimitingQueue(rateLimiter, "buckets")

	//informer := informers.NewSharedInformerFactory(client, controller.resyncPeriod)

	// ----------------------
	// BucketRequests

	bucketRequestHandler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { controller.enqueueBucketRequest(obj) },
		UpdateFunc: func(oldObj, newObj interface{}) { controller.enqueueBucketRequest(newObj) },
		DeleteFunc: func(obj interface{}) {
			// NOOP. The bucketRequest is either in bucketRequestsInProgress and in the queue, so it will be processed as usual
			// or it's not in bucketRequestsInProgress and then we don't care
		},
	}

	if controller.bucketRequestInformer != nil {
		controller.bucketRequestInformer.AddEventHandlerWithResyncPeriod(bucketRequestHandler, controller.resyncPeriod)
	} else {
		controller.bucketRequestInformer = v1alpha1.NewBucketRequestInformer(cosiclient, "default", controller.resyncPeriod, cache.Indexers{uidIndex: func(obj interface{}) ([]string, error) {
                uid, err := getObjectUID(obj)
                if err != nil {
                        return nil, err
                }
                return []string{uid}, nil
        }})
		controller.bucketRequestInformer.AddEventHandler(bucketRequestHandler)
	}
	controller.bucketRequestInformer.AddIndexers(cache.Indexers{uidIndex: func(obj interface{}) ([]string, error) {
		uid, err := getObjectUID(obj)
		if err != nil {
			return nil, err
		}
		return []string{uid}, nil
	}})
	controller.bucketRequestIndexer = controller.bucketRequestInformer.GetIndexer()

	// -----------------
	// Bucket

	bucketHandler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { controller.enqueueBucket(obj) },
		UpdateFunc: func(oldObj, newObj interface{}) { controller.enqueueBucket(newObj) },
		//DeleteFunc: func(obj interface{}) { controller.forgetVolume(obj) },
	}

	if controller.bucketInformer != nil {
		controller.bucketInformer.AddEventHandlerWithResyncPeriod(bucketHandler, controller.resyncPeriod)
	} else {
		controller.bucketInformer = v1alpha1.NewBucketInformer(cosiclient,  controller.resyncPeriod, cache.Indexers{uidIndex: func(obj interface{}) ([]string, error) {
                uid, err := getObjectUID(obj)
                if err != nil {
                        return nil, err
                }
                return []string{uid}, nil
        }})
		controller.bucketInformer.AddEventHandler(bucketHandler)
	}
	controller.buckets = controller.bucketInformer.GetStore()

		controller.bucketStore = NewBucketStoreQueue(client, controller.rateLimiter, controller.bucketRequestIndexer, controller.eventRecorder)
//		controller.bucketStore = NewBackoffStore(client, controller.eventRecorder, controller.createProvisionedBucketBackoff, controller)

	return controller
}

func getObjectUID(obj interface{}) (string, error) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return "", fmt.Errorf("error decoding object, invalid type")
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			return "", fmt.Errorf("error decoding object tombstone, invalid type")
		}
	}
	return string(object.GetUID()), nil
}

// enqueueBucketRequest takes an obj and converts it into UID that is then put onto bucketrequest work queue.
func (ctrl *ProvisionController) enqueueBucketRequest(obj interface{}) {
	uid, err := getObjectUID(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	if ctrl.bucketRequestQueue.NumRequeues(uid) == 0 {
		ctrl.bucketRequestQueue.Add(uid)
	}
}

// enqueueBucket takes an obj and converts it into a namespace/name string which
// is then put onto the given work queue.
func (ctrl *ProvisionController) enqueueBucket(obj interface{}) {
	var key string
	var err error
	if key, err = cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	// Re-Adding is harmless but try to add it to the queue only if it is not
	// already there, because if it is already there we *must* be retrying it
	if ctrl.bucketQueue.NumRequeues(key) == 0 {
		ctrl.bucketQueue.Add(key)
	}
}

// Run starts all of this controller's control loops
func (ctrl *ProvisionController) Run(ctx context.Context) {
	run := func(ctx context.Context) {
		glog.Infof("Starting provisioner controller %s!", ctrl.component)
		defer utilruntime.HandleCrash()
		defer ctrl.bucketRequestQueue.ShutDown()
		defer ctrl.bucketQueue.ShutDown()

		ctrl.hasRunLock.Lock()
		ctrl.hasRun = true
		ctrl.hasRunLock.Unlock()


		if !cache.WaitForCacheSync(ctx.Done(), ctrl.bucketRequestInformer.HasSynced, ctrl.bucketInformer.HasSynced) {
			return
		}

		for i := 0; i < ctrl.threadiness; i++ {
			go wait.Until(func() { ctrl.runBucketRequestWorker(ctx) }, time.Second, ctx.Done())
			go wait.Until(func() { ctrl.runBucketWorker(ctx) }, time.Second, ctx.Done())
		}

		glog.Infof("Started provisioner controller %s!", ctrl.component)

		select {}
	}

	go ctrl.bucketStore.Run(ctx, DefaultThreadiness)

	run(ctx)
}

func (ctrl *ProvisionController) runBucketRequestWorker(ctx context.Context) {
	for ctrl.processNextBucketRequestWorkItem(ctx) {
	}
}

func (ctrl *ProvisionController) runBucketWorker(ctx context.Context) {
	for ctrl.processNextBucketWorkItem(ctx) {
	}
}

// processNextBucketRequestWorkItem processes items from bucketRequestQueue
func (ctrl *ProvisionController) processNextBucketRequestWorkItem(ctx context.Context) bool {
	obj, shutdown := ctrl.bucketRequestQueue.Get()

	if shutdown {
		return false
	}

	err := func() error {
		// Apply per-operation timeout.
		if ctrl.provisionTimeout != 0 {
			timeout, cancel := context.WithTimeout(ctx, ctrl.provisionTimeout)
			defer cancel()
			ctx = timeout
		}
		defer ctrl.bucketRequestQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			ctrl.bucketRequestQueue.Forget(obj)
			return fmt.Errorf("expected string in workqueue but got %#v", obj)
		}

		if err := ctrl.syncBucketRequestHandler(ctx, key); err != nil {
			if ctrl.failedProvisionThreshold == 0 {
				glog.Warningf("Retrying syncing bucketRequest %q, failure %v", key, ctrl.bucketRequestQueue.NumRequeues(obj))
				ctrl.bucketRequestQueue.AddRateLimited(obj)
			} else if ctrl.bucketRequestQueue.NumRequeues(obj) < ctrl.failedProvisionThreshold {
				glog.Warningf("Retrying syncing bucketRequest %q because failures %v < threshold %v", key, ctrl.bucketRequestQueue.NumRequeues(obj), ctrl.failedProvisionThreshold)
				ctrl.bucketRequestQueue.AddRateLimited(obj)
			} else {
				glog.Errorf("Giving up syncing bucketRequest %q because failures %v >= threshold %v", key, ctrl.bucketRequestQueue.NumRequeues(obj), ctrl.failedProvisionThreshold)
				glog.V(2).Infof("Removing BucketRequest %s from bucketRequests in progress", key)
				// Done but do not Forget: it will not be in the queue but NumRequeues
				// will be saved until the obj is deleted from kubernetes
			}
			return fmt.Errorf("error syncing bucketRequest %q: %s", key, err.Error())
		}

		ctrl.bucketRequestQueue.Forget(obj)
		return nil
	}()

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// processNextBucketWorkItem processes items from bucketQueue
func (ctrl *ProvisionController) processNextBucketWorkItem(ctx context.Context) bool {
	obj, shutdown := ctrl.bucketQueue.Get()

	if shutdown {
		return false
	}

	err := func() error {
		// Apply per-operation timeout.
		if ctrl.deletionTimeout != 0 {
			timeout, cancel := context.WithTimeout(ctx, ctrl.deletionTimeout)
			defer cancel()
			ctx = timeout
		}
		defer ctrl.bucketQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			ctrl.bucketQueue.Forget(obj)
			return fmt.Errorf("expected string in workqueue but got %#v", obj)
		}

		if err := ctrl.syncBucketHandler(ctx, key); err != nil {
			if ctrl.failedDeleteThreshold == 0 {
				glog.Warningf("Retrying syncing bucket %q, failure %v", key, ctrl.bucketQueue.NumRequeues(obj))
				ctrl.bucketQueue.AddRateLimited(obj)
			} else if ctrl.bucketQueue.NumRequeues(obj) < ctrl.failedDeleteThreshold {
				glog.Warningf("Retrying syncing bucket %q because failures %v < threshold %v", key, ctrl.bucketQueue.NumRequeues(obj), ctrl.failedDeleteThreshold)
				ctrl.bucketQueue.AddRateLimited(obj)
			} else {
				glog.Errorf("Giving up syncing bucket %q because failures %v >= threshold %v", key, ctrl.bucketQueue.NumRequeues(obj), ctrl.failedDeleteThreshold)
				// Done but do not Forget: it will not be in the queue but NumRequeues
				// will be saved until the obj is deleted from kubernetes
			}
			return fmt.Errorf("error syncing bucket %q: %s", key, err.Error())
		}

		ctrl.bucketQueue.Forget(obj)
		return nil
	}()

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// syncBucketRequestHandler gets the bucketRequest from informer's cache then calls syncBucketRequest. A non-nil error triggers requeuing of the bucketRequest.
func (ctrl *ProvisionController) syncBucketRequestHandler(ctx context.Context, key string) error {
	objs, err := ctrl.bucketRequestIndexer.ByIndex(uidIndex, key)
	if err != nil {
		return err
	}
	var bucketRequestObj interface{}
	if len(objs) > 0 {
		bucketRequestObj = objs[0]
	} else {
		obj, found := ctrl.bucketRequestsInProgress.Load(key)
		if !found {
			utilruntime.HandleError(fmt.Errorf("bucketRequest %q in work queue no longer exists", key))
			return nil
		}
		bucketRequestObj = obj
	}
	return ctrl.syncBucketRequest(ctx, bucketRequestObj)
}

// syncBucketHandler gets the bucket from informer's cache then calls syncbucket
func (ctrl *ProvisionController) syncBucketHandler(ctx context.Context, key string) error {
	bucketObj, exists, err := ctrl.buckets.GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		utilruntime.HandleError(fmt.Errorf("bucket %q in work queue no longer exists", key))
		return nil
	}

	return ctrl.syncBucket(ctx, bucketObj)
}

// syncBucketRequest checks if the bucketRequest should have a bucket provisioned for it and
// provisions one if so. Returns an error if the bucketRequest is to be requeued.
func (ctrl *ProvisionController) syncBucketRequest(ctx context.Context, obj interface{}) error {
	bucketRequest, ok := obj.(*cosiapi.BucketRequest)
	if !ok {
		return fmt.Errorf("expected bucketRequest but got %+v", obj)
	}

	should, err := ctrl.shouldProvision(ctx, bucketRequest)
	if err != nil {
		return err
	} else if should {

		status, err := ctrl.provisionBucketRequestOperation(ctx, bucketRequest)
		if err == nil || status == ProvisioningFinished {
			// Provisioning is 100% finished / not in progress.
			switch err {
			case nil:
				glog.V(5).Infof("BucketRequest processing succeeded, removing bucketRequest %s from bucketRequests in progress", bucketRequest.UID)
			case errStopProvision:
				glog.V(5).Infof("Stop provisioning, removing bucketRequest %s from bucketRequests in progress", bucketRequest.UID)
				// Our caller would requeue if we pass on this special error; return nil instead.
				err = nil
			default:
				glog.V(2).Infof("Final error received, removing buckerRequest %s from bucketRequests in progress", bucketRequest.UID)
			}
			ctrl.bucketRequestsInProgress.Delete(string(bucketRequest.UID))
			return err
		}
		if status == ProvisioningInBackground {
			// Provisioning is in progress in background.
			glog.V(2).Infof("Temporary error received, adding bucketRequest %s to bucketRequests in progress", bucketRequest.UID)
			ctrl.bucketRequestsInProgress.Store(string(bucketRequest.UID), bucketRequest)
		} else {
			// status == ProvisioningNoChange.
			// Don't change bucketRequestsInProgress:
			// - the bucketRequest is already there if previous status was ProvisioningInBackground.
			// - the bucketRequest is not there if if previous status was ProvisioningFinished.
		}
		return err
	}
	return nil
}

// syncBucket checks if the bucket should be deleted and deletes if so
func (ctrl *ProvisionController) syncBucket(ctx context.Context, obj interface{}) error {
	bucket, ok := obj.(*cosiapi.Bucket)
	if !ok {
		return fmt.Errorf("expected bucket but got %+v", obj)
	}
    fmt.Println(bucket)
	return nil
}

// knownProvisioner checks if provisioner name has been
// configured to provision buckets for
func (ctrl *ProvisionController) knownProvisioner(provisioner string) bool {
	if provisioner == ctrl.provisionerName {
		return true
	}
	return false
}

// shouldProvision returns whether a bucketRequest should have a bucket provisioned for
// it, i.e. whether a Provision is "desired"
func (ctrl *ProvisionController) shouldProvision(ctx context.Context, bucketRequest *cosiapi.BucketRequest) (bool, error) {
	if bucketRequest.Name != "" {
		return false, nil
	}

	if qualifier, ok := ctrl.provisioner.(Qualifier); ok {
		if !qualifier.ShouldProvision(ctx, bucketRequest) {
			return false, nil
		}
	}

//		bucketRequestClass := util.GetBucketRequestClass(bucketRequest)
/*		class, err := ctrl.getBucketClass(bucketRequestClass)
		if err != nil {
			glog.Errorf("Error getting bucketRequest %q's StorageClass's fields: %v", bucketRequestToBucketRequestKey(bucketRequest), err)
			return false, err
		}
		if class.Provisioner != ctrl.provisionerName {
			return false, nil
		}

*/		return true, nil
	

//	return false, nil
}

// provisionBucketRequestOperation attempts to provision a bucket for the given bucketRequest.
// Returns nil error only when the bucket was provisioned (in which case it also returns ProvisioningFinished),
// a normal error when the bucket was not provisioned and provisioning should be retried (requeue the bucketRequest),
// or the special errStopProvision when provisioning was impossible and no further attempts to provision should be tried.
func (ctrl *ProvisionController) provisionBucketRequestOperation(ctx context.Context, bucketRequest *cosiapi.BucketRequest) (ProvisioningState, error) {
	// Most code here is identical to that found in controller.go of kube's  controller...
	bucketRequestClass := util.GetBucketRequestClass(bucketRequest)
	operation := fmt.Sprintf("provision %q class %q", bucketRequestToBucketRequestKey(bucketRequest), bucketRequestClass)
	glog.Info(logOperation(operation, "started"))

	//  A previous doProvisionBucketRequest may just have finished while we were waiting for
	//  the locks. Check that bucket (with deterministic name) hasn't been provisioned
	//  yet.
	bucketName := bucketRequest.Name
	config, err := rest.InClusterConfig()
    if err != nil {
            glog.Fatalf("Failed to create config: %v", err)
    }
	bucket, err := cosiclnt.NewForConfigOrDie(config).Buckets().Get(ctx, bucketName, metav1.GetOptions{})
	if err == nil && bucket != nil {
		// bucket has been already provisioned, nothing to do.
		glog.Info(logOperation(operation, "bucket %q already exists, skipping", bucketName))
		return ProvisioningFinished, errStopProvision
	}

	// Prepare a bucketRequestRef to the bucketRequest early (to fail before a bucket is
	// provisioned)
	/*bucketRequestRef, err := ref.GetReference(scheme.Scheme, bucketRequest)
	if err != nil {
		glog.Error(logOperation(operation, "unexpected error getting bucketRequest reference: %v", err))
		return ProvisioningNoChange, err
	}
*/

	options := ProvisionOptions{
		BucketName:    bucketName,
		BucketRequest: bucketRequest,
	}

	ctrl.eventRecorder.Event(bucketRequest, v1.EventTypeNormal, "Provisioning", fmt.Sprintf("External provisioner is provisioning bucket for bucketRequest %q", bucketRequestToBucketRequestKey(bucketRequest)))

	bucket, result, err := ctrl.provisioner.Provision(ctx, options)
	if err != nil {
		if ierr, ok := err.(*IgnoredError); ok {
			// Provision ignored, do nothing and hope another provisioner will provision it.
			glog.Info(logOperation(operation, "bucket provision ignored: %v", ierr))
			return ProvisioningFinished, errStopProvision
		}
		err = fmt.Errorf("failed to provision bucket with StorageClass %q: %v", bucketRequestClass, err)
		ctrl.eventRecorder.Event(bucketRequest, v1.EventTypeWarning, "ProvisioningFailed", err.Error())

		// ProvisioningReschedule shouldn't have been returned for buckets without selected node,
		// but if we get it anyway, then treat it like ProvisioningFinished because we cannot
		// reschedule.
		if result == ProvisioningReschedule {
			result = ProvisioningFinished
		}
		return result, err
	}

	glog.Info(logOperation(operation, "bucket %q provisioned", bucket.Name))

	// Set bucketRequestRef and the bucket controller will bind and set annBoundByController for us
	//bucket.Spec.bucketRequestRef = bucketRequestRef

	glog.Info(logOperation(operation, "succeeded"))

	if err := ctrl.bucketStore.StoreBucket(bucketRequest, bucket); err != nil {
		return ProvisioningFinished, err
	}
	return ProvisioningFinished, nil
}

func logOperation(operation, format string, a ...interface{}) string {
	return fmt.Sprintf(fmt.Sprintf("%s: %s", operation, format), a...)
}

// getInClusterNamespace returns the namespace in which the controller runs.
func getInClusterNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}

	// Fall back to the namespace associated with the service account token, if available
	if data, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
			return ns
		}
	}

	return "default"
}

// getProvisioneBucketNameForBucketRequest returns bucket.Name for the provisioned bucket.
// The name must be unique.
func (ctrl *ProvisionController) getProvisionedBucketNameForBucketRequest(bucketRequest *cosiapi.BucketRequest) string {
	return "bucketrequest-" + string(bucketRequest.UID)
}

/*
// getStorageClass retrives storage class object by name.
func (ctrl *ProvisionController) getStorageClass(name string) (*storage.StorageClass, error) {
	classObj, found, err := ctrl.classes.GetByKey(name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("storageClass %q not found", name)
	}
	switch class := classObj.(type) {
	case *storage.StorageClass:
		return class, nil
	case *storagebeta.StorageClass:
		// convert storagebeta.StorageClass to storage.StorageClass
		return &storage.StorageClass{
			ObjectMeta:           class.ObjectMeta,
			Provisioner:          class.Provisioner,
			Parameters:           class.Parameters,
			ReclaimPolicy:        class.ReclaimPolicy,
			MountOptions:         class.MountOptions,
			AllowedTopologies:    class.AllowedTopologies,
		}, nil
	}
	return nil, fmt.Errorf("cannot convert object to StorageClass: %+v", classObj)
}
*/

func bucketRequestToBucketRequestKey(bucketRequest *cosiapi.BucketRequest) string {
	return fmt.Sprintf("%s/%s", bucketRequest.Namespace, bucketRequest.Name)
}

func getString(m map[string]string, key string, alts ...string) (string, bool) {
	if m == nil {
		return "", false
	}
	keys := append([]string{key}, alts...)
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v, true
		}
	}
	return "", false
}

module github.com/brahmaroutu/cosi-external-provisioner

go 1.14

require (
	github.com/container-object-storage-interface/api v0.0.0-20200708183033-b21b31b712bd
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e
	k8s.io/api v0.18.5
	k8s.io/apimachinery v0.18.5
	k8s.io/client-go v0.18.4
	k8s.io/klog v1.0.0
)

replace github.com/container-object-storage-interface/api => /home/srinib/go/src/github.com/container-object-storage-interface/api

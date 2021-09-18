package test

import (
	"testing"

	"github.com/nats-io/nats.go"
	"k8s.io/client-go/kubernetes"
)

func TestK8S(t *testing.T) {
	_, err := kubernetes.NewForConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = nats.Connect("demo.nats.io")
	if err != nil {
		t.Fatal(err)
	}
}

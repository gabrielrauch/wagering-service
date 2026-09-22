//go:build integration

package sqs

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// TestReadinessUpAndDown is what GET /health/ready answers with for the queue
// half of its promise.
func TestReadinessUpAndDown(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name})
	health, err := NewHealth(queue, 3*time.Second)
	if err != nil {
		t.Fatalf("build a readiness check: %v", err)
	}

	if err := health.Ready(t.Context()); err != nil {
		t.Fatalf("a queue that is there reported unready: %v", err)
	}

	// Taken away underneath the running process, which is the outage this
	// endpoint exists to report: the configuration is still right, the queue
	// is not there.
	url, err := sharedSDK.GetQueueUrl(t.Context(), &awssqs.GetQueueUrlInput{
		QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	if _, err := sharedSDK.DeleteQueue(t.Context(),
		&awssqs.DeleteQueueInput{QueueUrl: url.QueueUrl}); err != nil {
		t.Fatalf("delete %s: %v", name, err)
	}

	err = health.Ready(t.Context())
	if err == nil {
		t.Fatalf("a queue that has been deleted reported ready")
	}
	if !strings.Contains(err.Error(), name) {
		t.Errorf("error = %q, want it to name the queue an operator has to go and look at", err)
	}
	if got := app.ClassOf(err); got != app.Unretryable {
		t.Errorf("class = %s, want %s: the queue is gone", got, app.Unretryable)
	}
}

// TestReadinessBoundsItself points the probe at something that accepts a
// connection and then says nothing.
//
// That is the failure a readiness endpoint is least able to survive: no answer
// at all is the one thing an orchestrator cannot act on, and without a bound of
// its own the probe would inherit whatever deadline its caller happened to set
// and wait for as long as the connection stayed open.
//
// The URL is stored directly rather than resolved, because resolving is what
// would hang — and the subject here is the probe, not the start-up.
func TestReadinessBoundsItself(t *testing.T) {
	client, err := NewClient(t.Context(), ClientConfig{
		Region: region, Endpoint: silentEndpoint(t)})
	if err != nil {
		t.Fatalf("build a client: %v", err)
	}
	queue, err := NewQueue(Config{Client: client, Name: "silent.fifo"})
	if err != nil {
		t.Fatalf("build a queue: %v", err)
	}
	queue.url.Store(aws.String(
		"http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/silent.fifo"))

	health, err := NewHealth(queue, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("build a readiness check: %v", err)
	}

	// No deadline on the caller's context: the bound under test is the check's
	// own.
	started := time.Now()
	err = health.Ready(t.Context())
	took := time.Since(started)

	if err == nil {
		t.Fatalf("a queue nothing answers for reported ready")
	}
	if took > 5*time.Second {
		t.Errorf("the probe took %s, want it bounded near its 500ms timeout", took)
	}
	if got := app.ClassOf(err); got != app.Retryable {
		t.Errorf("class = %s, want %s: nothing refused anything", got, app.Retryable)
	}
}

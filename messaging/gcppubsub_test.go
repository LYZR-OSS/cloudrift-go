package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	pubsubapi "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/grpc"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// fakeIAM stands in for the IAMPolicy service, which pstest does not
// implement: it grants exactly the permission listed per resource.
type fakeIAM struct {
	iampb.UnimplementedIAMPolicyServer
	granted map[string]string // resource → permission
}

func (f fakeIAM) TestIamPermissions(_ context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	var out []string
	for _, p := range req.Permissions {
		if f.granted[req.Resource] == p {
			out = append(out, p)
		}
	}
	return &iampb.TestIamPermissionsResponse{Permissions: out}, nil
}

// startPubSub runs an in-process Pub/Sub fake, points the clients at it via
// PUBSUB_EMULATOR_HOST, and creates each topic → subscription pair in project
// "p".
func startPubSub(t *testing.T, iam fakeIAM, pairs map[string]string) {
	t.Helper()
	srv := pstest.NewServerWithCallback(0, func(s *grpc.Server) { iampb.RegisterIAMPolicyServer(s, iam) })
	t.Cleanup(func() { _ = srv.Close() })
	t.Setenv("PUBSUB_EMULATOR_HOST", srv.Addr)

	ctx := context.Background()
	topics, err := pubsubapi.NewTopicAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer topics.Close()
	subs, err := pubsubapi.NewSubscriptionAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer subs.Close()
	for topic, sub := range pairs {
		if _, err := topics.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/p/topics/" + topic}); err != nil {
			t.Fatal(err)
		}
		if _, err := subs.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:  "projects/p/subscriptions/" + sub,
			Topic: "projects/p/topics/" + topic,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func newTestPubSub(t *testing.T, cfg Config) Backend {
	t.Helper()
	cfg.Project = "p"
	b, err := New(context.Background(), "gcp_pubsub", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	return b
}

func TestGCPPubSubRoundTripAndDeadLetter(t *testing.T) {
	startPubSub(t, fakeIAM{}, map[string]string{"jobs": "jobs-sub", "jobs-dlq": "jobs-dlq-sub"})
	ctx := context.Background()
	q := newTestPubSub(t, Config{Topic: "jobs", Subscription: "jobs-sub", DeadLetterTopic: "jobs-dlq"})

	id, err := q.Send(ctx, []byte(`{"n":1}`), map[string]string{"encoding": "json"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := q.Receive(ctx, 10, 2*time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("Receive = %v, err = %v; want 1 message", msgs, err)
	}
	if m := msgs[0]; m.ID != id || string(m.Body) != `{"n":1}` || m.Attributes["encoding"] != "json" {
		t.Fatalf("received %+v; want ID %q, the sent body, and its attributes", m, id)
	}

	if err := q.DeadLetter(ctx, msgs[0].ReceiptHandle, "bad payload"); err != nil {
		t.Fatal(err)
	}
	// The handle is settled: a second DeadLetter has nothing to re-publish.
	if err := q.DeadLetter(ctx, msgs[0].ReceiptHandle, "again"); !errors.Is(err, core.ErrMessaging) {
		t.Fatalf("second DeadLetter err = %v; want core.ErrMessaging", err)
	}
	// Acked on the source subscription: an idle pull returns empty, not an error.
	if left, err := q.Receive(ctx, 10, 100*time.Millisecond); err != nil || len(left) != 0 {
		t.Fatalf("source Receive = %v, err = %v; want empty", left, err)
	}

	// A full resource name works as well as a bare ID.
	dlq := newTestPubSub(t, Config{Subscription: "projects/p/subscriptions/jobs-dlq-sub"})
	dead, err := dlq.Receive(ctx, 10, 2*time.Second)
	if err != nil || len(dead) != 1 {
		t.Fatalf("DLQ Receive = %v, err = %v; want 1 message", dead, err)
	}
	if string(dead[0].Body) != `{"n":1}` || dead[0].Attributes["DeadLetterReason"] != "bad payload" {
		t.Fatalf("dead-lettered %+v; want the original body with DeadLetterReason", dead[0])
	}
	if err := dlq.Delete(ctx, dead[0].ReceiptHandle); err != nil {
		t.Fatal(err)
	}
}

func TestGCPPubSubSendBatchAndPurge(t *testing.T) {
	startPubSub(t, fakeIAM{}, map[string]string{"jobs": "jobs-sub"})
	ctx := context.Background()
	q := newTestPubSub(t, Config{Topic: "jobs", Subscription: "jobs-sub"})

	ids, err := q.SendBatch(ctx, []OutgoingMessage{{Body: []byte("a")}, {Body: []byte("b")}})
	if err != nil || len(ids) != 2 {
		t.Fatalf("SendBatch = %v, err = %v; want 2 IDs", ids, err)
	}
	if err := q.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	if msgs, err := q.Receive(ctx, 10, 100*time.Millisecond); err != nil || len(msgs) != 0 {
		t.Fatalf("Receive after Purge = %v, err = %v; want empty", msgs, err)
	}
}

func TestGCPPubSubHealthCheck(t *testing.T) {
	startPubSub(t, fakeIAM{granted: map[string]string{
		"projects/p/topics/jobs":            "pubsub.topics.publish",
		"projects/p/subscriptions/jobs-sub": "pubsub.subscriptions.consume",
	}}, map[string]string{"jobs": "jobs-sub", "other": "other-sub"})

	if !newTestPubSub(t, Config{Topic: "jobs", Subscription: "jobs-sub"}).HealthCheck(context.Background()) {
		t.Fatal("HealthCheck = false; want true when publish and consume are both granted")
	}
	if newTestPubSub(t, Config{Subscription: "other-sub"}).HealthCheck(context.Background()) {
		t.Fatal("HealthCheck = true; want false without pubsub.subscriptions.consume")
	}
}

func TestGCPPubSubFailsLoudly(t *testing.T) {
	startPubSub(t, fakeIAM{}, map[string]string{"jobs": "jobs-sub"})
	ctx := context.Background()
	sendOnly := newTestPubSub(t, Config{Topic: "jobs"})
	receiveOnly := newTestPubSub(t, Config{Subscription: "jobs-sub"})

	if _, err := sendOnly.Send(ctx, []byte("x"), nil, time.Second); !errors.Is(err, core.ErrNotImplemented) {
		t.Fatalf("Send with delay err = %v; want core.ErrNotImplemented", err)
	}
	if _, err := sendOnly.GetQueueDepth(ctx); !errors.Is(err, core.ErrNotImplemented) {
		t.Fatalf("GetQueueDepth err = %v; want core.ErrNotImplemented", err)
	}
	if _, err := sendOnly.Receive(ctx, 1, 0); !errors.Is(err, core.ErrMessaging) {
		t.Fatalf("Receive without Subscription err = %v; want core.ErrMessaging", err)
	}
	if _, err := receiveOnly.Send(ctx, []byte("x"), nil, 0); !errors.Is(err, core.ErrMessaging) {
		t.Fatalf("Send without Topic err = %v; want core.ErrMessaging", err)
	}
	missing := newTestPubSub(t, Config{Topic: "no-such-topic"})
	if _, err := missing.Send(ctx, []byte("x"), nil, 0); !errors.Is(err, core.ErrQueueNotFound) {
		t.Fatalf("Send to missing topic err = %v; want core.ErrQueueNotFound", err)
	}
}

func TestNewGCPPubSubRejectsBadConfig(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no project":          {Topic: "t"},
		"no topic or sub":     {Project: "p"},
		"both key sources":    {Project: "p", Topic: "t", ServiceAccountFile: "sa.json", ServiceAccountJSON: "{}"},
		"metadata plus a key": {Project: "p", Topic: "t", ServiceAccountFile: "sa.json", PreferMetadata: true},
	} {
		if _, err := NewGCPPubSub(context.Background(), cfg); !errors.Is(err, core.ErrMessaging) {
			t.Errorf("%s: err = %v; want core.ErrMessaging", name, err)
		}
	}
}

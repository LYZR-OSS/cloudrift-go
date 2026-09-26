// Package integration is the live suite for the GCP backends. Each test reads
// its target from the environment and skips when that is unset, so `go test
// ./...` never touches the network. Variable names match the Python suite
// (cloudrift/tests/integration), so one env file drives both.
//
//	CLOUDRIFT_GCP_PROJECT                 Pub/Sub project
//	CLOUDRIFT_PUBSUB_TOPIC                topic and pull subscription under test, plus a
//	CLOUDRIFT_PUBSUB_SUBSCRIPTION         dead-letter pair; the running identity needs only
//	CLOUDRIFT_PUBSUB_DLQ_TOPIC            roles/pubsub.publisher and roles/pubsub.subscriber
//	CLOUDRIFT_PUBSUB_DLQ_SUBSCRIPTION
//	CLOUDRIFT_FIRESTORE_UID               a MongoDB-compatibility (Enterprise) database
//	CLOUDRIFT_FIRESTORE_LOCATION
//	CLOUDRIFT_FIRESTORE_DATABASE
//	CLOUDRIFT_MEMORYSTORE_HOST            VPC-private, so only reachable from inside the VPC;
//	CLOUDRIFT_MEMORYSTORE_AUTH            _AUTH is the AUTH string, _CA the instance's server
//	CLOUDRIFT_MEMORYSTORE_CA              CA bundle (selects from_server_ca_cert)
//	CLOUDRIFT_GCP_PREFER_METADATA=true    running on GCE/GKE: authenticate from the metadata
//	                                      server (Pub/Sub PreferMetadata, Firestore OIDC)
//
// Off GCP, Firestore authenticates with an access token minted from ADC:
// OIDC needs the metadata server, and the token path reaches the same live
// endpoint through the same URI construction.
package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/oauth2/google"

	"github.com/LYZR-OSS/cloudrift-go/cache"
	"github.com/LYZR-OSS/cloudrift-go/core"
	"github.com/LYZR-OSS/cloudrift-go/document"
	"github.com/LYZR-OSS/cloudrift-go/messaging"
)

// runID keeps concurrent or repeated runs from colliding on shared resources.
var runID = strconv.FormatInt(time.Now().UnixNano(), 36)

var onGCP = os.Getenv("CLOUDRIFT_GCP_PREFER_METADATA") == "true"

func env(t *testing.T, names ...string) []string {
	t.Helper()
	vals := make([]string, len(names))
	for i, n := range names {
		if vals[i] = os.Getenv(n); vals[i] == "" {
			t.Skipf("%s is not set", n)
		}
	}
	return vals
}

func TestLivePubSub(t *testing.T) {
	v := env(t, "CLOUDRIFT_GCP_PROJECT", "CLOUDRIFT_PUBSUB_TOPIC", "CLOUDRIFT_PUBSUB_SUBSCRIPTION",
		"CLOUDRIFT_PUBSUB_DLQ_TOPIC", "CLOUDRIFT_PUBSUB_DLQ_SUBSCRIPTION")
	project, topic, sub, dlqTopic, dlqSub := v[0], v[1], v[2], v[3], v[4]
	ctx := context.Background()
	open := func(cfg messaging.Config) messaging.Backend {
		t.Helper()
		cfg.Project, cfg.PreferMetadata = project, onGCP
		b, err := messaging.New(ctx, "gcp_pubsub", cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close(ctx) })
		return b
	}
	q := open(messaging.Config{Topic: topic, Subscription: sub, DeadLetterTopic: dlqTopic})
	dlq := open(messaging.Config{Subscription: dlqSub})

	t.Run("health check needs only publish and consume", func(t *testing.T) {
		if !q.HealthCheck(ctx) || !dlq.HealthCheck(ctx) {
			t.Fatal("HealthCheck = false for existing resources")
		}
		if open(messaging.Config{Subscription: "cloudrift-go-it-missing-" + runID}).HealthCheck(ctx) {
			t.Fatal("HealthCheck = true for a missing subscription")
		}
	})

	t.Run("send, receive, delete", func(t *testing.T) {
		body := "rt-" + runID
		id, err := q.Send(ctx, []byte(body), map[string]string{"encoding": "text"}, 0)
		if err != nil {
			t.Fatal(err)
		}
		m := pull(t, q, body, 1)[0]
		if m.ID != id || m.Attributes["encoding"] != "text" {
			t.Fatalf("received %+v; want ID %q with its attributes", m, id)
		}
		if err := q.Delete(ctx, m.ReceiptHandle); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("send batch", func(t *testing.T) {
		prefix := "batch-" + runID + "-"
		ids, err := q.SendBatch(ctx, []messaging.OutgoingMessage{
			{Body: []byte(prefix + "0")}, {Body: []byte(prefix + "1")}, {Body: []byte(prefix + "2")},
		})
		if err != nil || len(ids) != 3 {
			t.Fatalf("SendBatch = %v, err = %v; want 3 IDs", ids, err)
		}
		for _, m := range pull(t, q, prefix, 3) {
			if err := q.Delete(ctx, m.ReceiptHandle); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("dead letter", func(t *testing.T) {
		body := "dl-" + runID
		if _, err := q.Send(ctx, []byte(body), nil, 0); err != nil {
			t.Fatal(err)
		}
		m := pull(t, q, body, 1)[0]
		if err := q.DeadLetter(ctx, m.ReceiptHandle, "e2e"); err != nil {
			t.Fatal(err)
		}
		d := pull(t, dlq, body, 1)[0]
		if d.Attributes["DeadLetterReason"] != "e2e" {
			t.Fatalf("dead-lettered attributes = %v; want DeadLetterReason", d.Attributes)
		}
		if err := dlq.Delete(ctx, d.ReceiptHandle); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("purge seeks past the backlog", func(t *testing.T) {
		prefix := "purge-" + runID + "-"
		if _, err := q.SendBatch(ctx, []messaging.OutgoingMessage{
			{Body: []byte(prefix + "0")}, {Body: []byte(prefix + "1")},
		}); err != nil {
			t.Fatal(err)
		}
		if err := q.Purge(ctx); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			msgs, err := q.Receive(ctx, 10, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range msgs {
				if strings.HasPrefix(string(m.Body), prefix) {
					t.Fatalf("purged message %q was redelivered", m.Body)
				}
			}
		}
	})

	t.Run("errors translate and gaps fail loudly", func(t *testing.T) {
		missing := open(messaging.Config{Topic: "cloudrift-go-it-missing-" + runID})
		if _, err := missing.Send(ctx, []byte("x"), nil, 0); !errors.Is(err, core.ErrQueueNotFound) {
			t.Fatalf("Send to a missing topic err = %v; want core.ErrQueueNotFound", err)
		}
		if _, err := q.Send(ctx, []byte("x"), nil, time.Second); !errors.Is(err, core.ErrNotImplemented) {
			t.Fatalf("Send with delay err = %v; want core.ErrNotImplemented", err)
		}
		if _, err := q.GetQueueDepth(ctx); !errors.Is(err, core.ErrNotImplemented) {
			t.Fatalf("GetQueueDepth err = %v; want core.ErrNotImplemented", err)
		}
	})
}

// pull polls until n messages whose body starts with prefix arrive and returns
// them unacked, acking any stray messages left by earlier runs. Pub/Sub
// delivery is not instant and a pull may legitimately return nothing.
func pull(t *testing.T, q messaging.Backend, prefix string, n int) []messaging.Message {
	t.Helper()
	ctx := context.Background()
	var got []messaging.Message
	for range 15 {
		msgs, err := q.Receive(ctx, 10, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if strings.HasPrefix(string(m.Body), prefix) {
				got = append(got, m)
			} else if err := q.Delete(ctx, m.ReceiptHandle); err != nil {
				t.Fatal(err)
			}
		}
		if len(got) >= n {
			return got
		}
	}
	t.Fatalf("received %d of %d messages with prefix %q", len(got), n, prefix)
	return nil
}

func TestLiveFirestore(t *testing.T) {
	v := env(t, "CLOUDRIFT_FIRESTORE_UID", "CLOUDRIFT_FIRESTORE_LOCATION", "CLOUDRIFT_FIRESTORE_DATABASE")
	cfg := document.Config{UID: v[0], Location: v[1], Database: v[2]}
	if !onGCP {
		creds, err := google.FindDefaultCredentials(context.Background(), "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			t.Fatal(err)
		}
		tok, err := creds.TokenSource.Token()
		if err != nil {
			t.Fatal(err)
		}
		cfg.AccessToken = tok.AccessToken
		t.Run("access token", func(t *testing.T) { crud(t, cfg) })
		return
	}
	t.Run("oidc", func(t *testing.T) { crud(t, cfg) })
	t.Run("bare connection string picks up oidc", func(t *testing.T) {
		crud(t, document.Config{
			ConnectionString: fmt.Sprintf("mongodb://%s.%s.firestore.goog:443/%s", cfg.UID, cfg.Location, cfg.Database),
			Database:         cfg.Database,
		})
	})
}

// crud is the real proof that the mandatory URI options and the auth path are
// right: a wrong combination does not fail at construction but here, as a
// server-selection timeout or a silently closed connection.
func crud(t *testing.T, cfg document.Config) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := document.New("firestore", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(ctx)

	// Only cfg.Database is selectable on a Firestore endpoint.
	coll := client.Database(cfg.Database).Collection("cloudrift_go_it_" + runID)
	defer coll.DeleteMany(ctx, bson.M{})
	res, err := coll.InsertOne(ctx, bson.M{"name": "alice", "n": 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coll.UpdateOne(ctx, bson.M{"_id": res.InsertedID}, bson.M{"$inc": bson.M{"n": 1}}); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Name string `bson:"name"`
		N    int    `bson:"n"`
	}
	if err := coll.FindOne(ctx, bson.M{"_id": res.InsertedID}).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "alice" || got.N != 2 {
		t.Fatalf("read back %+v; want alice with n=2", got)
	}
}

func TestLiveMemorystore(t *testing.T) {
	host := env(t, "CLOUDRIFT_MEMORYSTORE_HOST")[0]
	cfg := cache.Config{
		Host:       host,
		AuthString: os.Getenv("CLOUDRIFT_MEMORYSTORE_AUTH"),
		CACerts:    os.Getenv("CLOUDRIFT_MEMORYSTORE_CA"),
	}
	method := "from_auth_string"
	if cfg.CACerts != "" {
		method = "from_server_ca_cert"
	}
	ctx := context.Background()
	c, err := cache.New(ctx, "memorystore", method, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	key := "cloudrift-go-it:" + runID
	defer c.Delete(ctx, key, key+":set", key+":n")
	if err := c.Set(ctx, key, "value", time.Minute); err != nil {
		t.Fatalf("Set over %s: %v", method, err)
	}
	if got, err := c.Get(ctx, key); err != nil || string(got) != "value" {
		t.Fatalf("Get = %q, err = %v", got, err)
	}
	if err := c.Pipeline(ctx, func(p cache.Pipeliner) {
		p.SAdd(key+":set", "a", "b")
		p.Expire(key+":set", time.Minute)
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := c.SCard(ctx, key+":set"); err != nil || n != 2 {
		t.Fatalf("SCard = %d, err = %v", n, err)
	}
	if res, err := c.Eval(ctx, "return redis.call('INCR', KEYS[1])", []string{key + ":n"}); err != nil || res != int64(1) {
		t.Fatalf("Eval = %v, err = %v", res, err)
	}

	if cfg.CACerts != "" {
		// The TLS knobs on from_auth_string must reach the same verified endpoint.
		tlsCfg := cfg
		tlsCfg.TLS, tlsCfg.Port = core.Ptr(true), 6378
		b, err := cache.New(ctx, "memorystore", "from_auth_string", tlsCfg)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close(ctx)
		if ok, err := b.Ping(ctx); err != nil || !ok {
			t.Fatalf("Ping via from_auth_string with TLS = %v, err = %v", ok, err)
		}
	}
}

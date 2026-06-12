# cloudrift-go

Cloud-agnostic abstraction for **storage**, **messaging**, **document databases**, **cache**, **secrets**, and **pub/sub** — built for Lyzr microservices. The Go port of [cloudrift](../README.md) (Python).

- **Context-first.** Every operation takes a `context.Context` and is backed by native Go cloud SDKs (`aws-sdk-go-v2`, `azure-sdk-for-go`, `mongo-driver/v2`, `go-redis/v9`) — connection-pooled, no wrappers.
- **Drop-in providers.** Same interface across AWS, Azure, and self-hosted backends. Swap `s3` ↔ `azure_blob` (or `sqs` ↔ `azure_bus`, `documentdb` ↔ `cosmos`, `redis` ↔ `elasticache` ↔ `azure_redis`) by changing one string.
- **Multiple auth methods per provider.** Static keys, IAM roles, profiles, managed identity, service principals, SAS tokens, mTLS, IAM auth — pick what your microservice already has.

| Category | Factory | AWS | Azure | Self-hosted |
|---|---|---|---|---|
| Storage | `storage.New` | `s3` | `azure_blob` | — |
| Messaging | `messaging.New` | `sqs` | `azure_bus` | — |
| Document DB | `document.New` | `documentdb` | `cosmos` | — |
| Cache | `cache.New` | `elasticache` | `azure_redis` | `redis` |
| Secrets | `secrets.New` | `aws_secrets_manager` | `azure_keyvault` | — |
| Pub/Sub | `pubsub.New` | `sns` | `azure_eventgrid` | — |

---

## Install

```bash
go get github.com/NeuralgoLyzr/cloudrift-go
```

Go 1.23+. Import only the categories you use — Go links only what you import, so a service using just the cache never pulls AWS/Azure SDK code into its binary.

---

## Quick start

Every backend is constructed via a factory function and held for the lifetime of the service. Reuse one instance per resource — the underlying client is connection-pooled.

```go
import "github.com/NeuralgoLyzr/cloudrift-go/storage"

// Construct once at startup
st, err := storage.New(ctx, "s3", storage.Config{
    Bucket:             "my-bucket",
    AWSAccessKeyID:     "AKIA...",
    AWSSecretAccessKey: "...",
    Region:             "us-east-1",
})
if err != nil { ... }

// Use anywhere
_, err = st.Upload(ctx, "docs/hello.txt", []byte("hello world"), "text/plain")
data, err := st.Download(ctx, "docs/hello.txt")
url, err := st.PresignedURL(ctx, "docs/hello.txt", time.Hour)

// Release sockets at shutdown
defer st.Close(ctx)
```

### Configuration via env vars

Pick the provider per environment with a single env var:

```go
st, err := storage.New(ctx, os.Getenv("STORAGE_PROVIDER"), storage.Config{ // "s3" in prod, "azure_blob" in dev
    Bucket:    os.Getenv("STORAGE_BUCKET"),
    Container: os.Getenv("STORAGE_CONTAINER"),
    Region:    os.Getenv("STORAGE_REGION"),
    // ...
})
```

The factory routes to the right auth method based on which credential fields are set (mirroring the Python library's kwargs-presence routing); unset fields are simply ignored.

---

## Storage

```go
// AWS S3
storage.New(ctx, "s3", storage.Config{Bucket: "b", Region: "us-east-1"})       // IAM role
storage.New(ctx, "s3", storage.Config{Bucket: "b", AWSAccessKeyID: "...",      // static keys
    AWSSecretAccessKey: "...", Region: "us-east-1"})
storage.New(ctx, "s3", storage.Config{Bucket: "b", ProfileName: "dev"})        // ~/.aws/credentials

// Azure Blob
storage.New(ctx, "azure_blob", storage.Config{ConnectionString: "...", Container: "c"})
storage.New(ctx, "azure_blob", storage.Config{AccountURL: "https://acct.blob.core.windows.net",
    AccountKey: "...", Container: "c"})
storage.New(ctx, "azure_blob", storage.Config{AccountURL: "...", SASToken: "...", Container: "c"})
storage.New(ctx, "azure_blob", storage.Config{AccountURL: "...", Container: "c"}) // managed identity
storage.New(ctx, "azure_blob", storage.Config{AccountURL: "...", Container: "c",
    TenantID: "...", ClientID: "...", ClientSecret: "..."})                       // service principal
```

**Operations** — same on every backend:

```go
key, err := st.Upload(ctx, key, data, "application/json")
data, err := st.Download(ctx, key)
err = st.Delete(ctx, key)
ok, err := st.Exists(ctx, key)
keys, err := st.List(ctx, "logs/")
for key, err := range st.ListIter(ctx, "logs/") { ... } // lazy, true pagination
url, err := st.PresignedURL(ctx, key, time.Hour)
key, err = st.Copy(ctx, src, dst)
md, err := st.GetMetadata(ctx, key)
key, err = st.UploadStream(ctx, key, reader, "video/mp4")
err = st.Close(ctx)
```

> **Azure note:** `PresignedURL` requires account-key auth (connection string or `AccountKey`) — SAS URLs cannot be minted from managed-identity credentials.

---

## Messaging

```go
import "github.com/NeuralgoLyzr/cloudrift-go/messaging"

// AWS SQS
q, err := messaging.New(ctx, "sqs", messaging.Config{
    QueueURL: "https://sqs.us-east-1.amazonaws.com/.../q", Region: "us-east-1"})

// Azure Service Bus
q, err := messaging.New(ctx, "azure_bus", messaging.Config{
    ConnectionString: "...", QueueName: "my-queue"})
q, err := messaging.New(ctx, "azure_bus", messaging.Config{
    FullyQualifiedNamespace: "ns.servicebus.windows.net", QueueName: "my-queue"}) // managed identity
```

**Operations**:

```go
id, err := q.Send(ctx, map[string]any{"action": "process", "id": 42}, 0)
ids, err := q.SendBatch(ctx, []map[string]any{{"n": 1}, {"n": 2}})

msgs, err := q.Receive(ctx, 10, 20*time.Second) // long-poll
for _, m := range msgs {
    handleJob(m.Body)
    err = q.Delete(ctx, m.ReceiptHandle)              // ack
    // or: q.DeadLetter(ctx, m.ReceiptHandle, "bad payload")
}

depth, err := q.GetQueueDepth(ctx)
err = q.Purge(ctx)
err = q.Close(ctx)
```

> **Dead-lettering:** Azure Service Bus dead-letters natively. SQS has no per-message dead-letter API, so the backend emulates it: it re-sends the message body to the DLQ (from `Config.DLQURL` or the queue's RedrivePolicy) and deletes the original.

---

## Document Database

`document.New` is a connection factory: it returns a configured `*mongo.Client` from the official [MongoDB Go driver](https://pkg.go.dev/go.mongodb.org/mongo-driver/v2/mongo) regardless of provider — both AWS DocumentDB and Azure Cosmos DB (MongoDB API) speak the MongoDB wire protocol. You get the driver's full native API: typed decoding, transactions, bulk writes, change streams, aggregation.

```go
import "github.com/NeuralgoLyzr/cloudrift-go/document"

// AWS DocumentDB (MongoDB-compatible)
client, err := document.New("documentdb", document.Config{
    URI:       "mongodb://user:pass@cluster.docdb.amazonaws.com:27017/?tls=true",
    TLSCAFile: "/etc/ssl/rds-ca-bundle.pem",
    MaxPoolSize: 200,
})
client, err := document.New("documentdb", document.Config{ // or host/port credentials
    Host: "cluster.docdb.amazonaws.com", Port: 27017, Username: "u", Password: "p"})
client, err := document.New("documentdb", document.Config{ // or mTLS
    Host: "cluster.docdb.amazonaws.com", Port: 27017, Username: "u", Password: "p",
    TLSCertKeyFile: "/etc/ssl/client.pem"})

// Azure Cosmos DB (MongoDB API)
client, err := document.New("cosmos", document.Config{ConnectionString: "mongodb://..."})
client, err := document.New("cosmos", document.Config{Account: "myacct", AccountKey: "..."})
```

**Operations** — the native driver API, identical on both providers:

```go
import "go.mongodb.org/mongo-driver/v2/bson"

coll := client.Database("lyzr").Collection("users")

res, err := coll.InsertOne(ctx, bson.M{"name": "Alice", "age": 30})
err = coll.FindOne(ctx, bson.M{"name": "Alice"}).Decode(&user)
cur, err := coll.Find(ctx, bson.M{"age": bson.M{"$gte": 18}})
n, err := coll.UpdateOne(ctx, bson.M{"_id": res.InsertedID},
    bson.M{"$set": bson.M{"age": 31}})

err = client.Disconnect(ctx) // lifecycle is caller-managed; call at shutdown
```

> **Cosmos note:** key-based auth only (connection string or account + key) — Cosmos for MongoDB (RU) does not accept Azure AD tokens at the wire-protocol layer. `Account` + `AccountKey` builds the URI with the Cosmos-required parameters (`ssl`, `replicaSet=globaldb`, `retryWrites=false`).

---

## Cache

```go
import "github.com/NeuralgoLyzr/cloudrift-go/cache"

// Self-hosted Redis
c, err := cache.New(ctx, "redis", "from_url", cache.Config{URL: "redis://localhost:6379/0"})
c, err := cache.New(ctx, "redis", "from_credentials", cache.Config{
    Host: "redis.internal", Port: 6379, Password: "...", DB: 0})

// AWS ElastiCache
c, err := cache.New(ctx, "elasticache", "from_auth_token", cache.Config{
    Host: "my-cluster.cache.amazonaws.com", AuthToken: "..."})
c, err := cache.New(ctx, "elasticache", "from_iam_auth", cache.Config{
    Host: "my-cluster.cache.amazonaws.com",
    Username: "lyzr-app", Region: "us-east-1"}) // SigV4 + auto-refresh

// Azure Cache for Redis
c, err := cache.New(ctx, "azure_redis", "from_access_key", cache.Config{
    Host: "my-cache.redis.cache.windows.net", AccessKey: "..."})
c, err := cache.New(ctx, "azure_redis", "from_managed_identity", cache.Config{
    Host: "my-cache.redis.cache.windows.net", Username: "lyzr-app"})
```

**Operations** — KV, hash, set, list, counters:

```go
err = c.Set(ctx, "session:abc", data, time.Hour)
val, err := c.Get(ctx, "session:abc") // nil, nil if missing
n, err := c.Delete(ctx, "session:abc")

n, err = c.HSet(ctx, "user:1", "name", "Alice")
fields, err := c.HGetAll(ctx, "user:1")

added, err := c.SAdd(ctx, "dau:2026-06-10", userID) // returns newly-added count (dedup signal)
common, err := c.SInter(ctx, "dau:2026-06-09", "dau:2026-06-10")

n, err = c.LPush(ctx, "jobs", "job-1", "job-2")
batch, err := c.LRange(ctx, "jobs", 0, 99)

n, err = c.Incr(ctx, "hits:home")

// Atomic multi-command round trip (MULTI/EXEC)
err = c.Pipeline(ctx, func(p cache.Pipeliner) {
    p.SAdd("dau:2026-06-10", userID)
    p.Expire("dau:2026-06-10", 48*time.Hour)
})

err = c.Close(ctx)
```

---

## Secrets

```go
import "github.com/NeuralgoLyzr/cloudrift-go/secrets"

sm, err := secrets.New(ctx, "aws_secrets_manager", secrets.Config{Region: "us-east-1"}) // IAM
kv, err := secrets.New(ctx, "azure_keyvault", secrets.Config{
    VaultURL: "https://myvault.vault.azure.net"}) // managed identity

val, err := sm.GetSecret(ctx, "db-password")
cfg, err := sm.GetSecretJSON(ctx, "db-config")
err = sm.SetSecret(ctx, "db-password", "s3cret")
err = sm.DeleteSecret(ctx, "db-password")
names, err := sm.ListSecrets(ctx, "db-")
```

---

## Pub/Sub

```go
import "github.com/NeuralgoLyzr/cloudrift-go/pubsub"

ps, err := pubsub.New(ctx, "sns", pubsub.Config{Region: "us-east-1"})
ps, err := pubsub.New(ctx, "azure_eventgrid", pubsub.Config{
    Endpoint: "https://topic.region.eventgrid.azure.net/api/events", AccessKey: "..."})

id, err := ps.Publish(ctx, topicARN, `{"event": "user.created"}`,
    map[string]string{"source": "auth-svc"})
ids, err := ps.PublishBatch(ctx, topicARN, []pubsub.BatchMessage{
    {Message: `{"n": 1}`}, {Message: `{"n": 2}`},
}) // SNS chunks at the 10-entry batch limit automatically
```

---

## Errors

All backends translate provider-native errors into one hierarchy under `core`, built on wrapped sentinel errors — match with `errors.Is` at any level:

```go
import "github.com/NeuralgoLyzr/cloudrift-go/core"

data, err := st.Download(ctx, "missing.txt")
switch {
case errors.Is(err, core.ErrObjectNotFound):  // specific
case errors.Is(err, core.ErrStorage):         // any storage error
case errors.Is(err, core.ErrCloudRift):       // any cloudrift error
}
```

Specific errors per category: `ErrObjectNotFound`/`ErrStoragePermission`, `ErrQueueNotFound`/`ErrMessageSend`, `ErrDocumentConnection`, `ErrCacheConnection`/`ErrCacheKeyNotFound`, `ErrSecretNotFound`/`ErrSecretPermission`, `ErrTopicNotFound`/`ErrPublish`. Operations a provider cannot honor return `core.ErrNotImplemented`. The document package returns a native `*mongo.Client`, so operation errors come from the MongoDB driver directly.

---

## Connection pooling & lifecycle

Every backend holds **one long-lived client** reused across all operations:

- **Don't** call `storage.New(...)` inside a request handler.
- **Do** construct it once at startup and share it (struct field, DI container, or package-level singleton).
- Release sockets at shutdown with `backend.Close(ctx)`. (AWS/Azure HTTP-based backends are no-op closes; Redis, Mongo, and Service Bus actually tear down connections.)

---

## Testing

```bash
go test ./...
```

The cache suite runs against an in-process [miniredis](https://github.com/alicebob/miniredis) (the Go analogue of the Python suite's fakeredis), so no real cloud credentials are needed. AWS backends accept `Config.EndpointURL` for LocalStack/MinIO-style integration testing.

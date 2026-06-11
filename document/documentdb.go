package document

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/NeuralgoLyzr/cloudrift-go/core"
)

// AWSDocumentDBBackend is the AWS DocumentDB (MongoDB-compatible) backend,
// built on the official MongoDB Go driver. It also works against plain
// MongoDB and Cosmos DB's MongoDB API.
type AWSDocumentDBBackend struct {
	client *mongo.Client
	db     *mongo.Database
}

var _ Backend = (*AWSDocumentDBBackend)(nil)

// NewDocumentDB constructs a DocumentDB backend. Routing mirrors the Python
// factory: URI > TLSCertKeyFile (mTLS) > host/port credentials.
func NewDocumentDB(ctx context.Context, cfg Config) (*AWSDocumentDBBackend, error) {
	uri := cfg.URI
	if uri == "" {
		// Build a URI from host/port credentials, mirroring from_credentials /
		// from_tls_cert. TLS defaults to on (required for AWS DocumentDB).
		params := url.Values{}
		if cfg.TLSCertKeyFile != "" {
			params.Set("tls", "true")
			params.Set("tlsCertificateKeyFile", cfg.TLSCertKeyFile)
		} else if cfg.TLS == nil || *cfg.TLS {
			params.Set("tls", "true")
		}
		if cfg.TLSCAFile != "" {
			params.Set("tlsCAFile", cfg.TLSCAFile)
		}
		uri = fmt.Sprintf("mongodb://%s:%s@%s:%d/?%s",
			url.QueryEscape(cfg.Username), url.QueryEscape(cfg.Password),
			cfg.Host, cfg.Port, params.Encode())
	} else if cfg.TLSCAFile != "" && !strings.Contains(uri, "tlsCAFile=") {
		sep := "?"
		if strings.Contains(uri, "?") {
			sep = "&"
		}
		uri += sep + "tlsCAFile=" + url.QueryEscape(cfg.TLSCAFile)
	}

	maxPool := cfg.MaxPoolSize
	if maxPool == 0 {
		maxPool = 100
	}
	opts := options.Client().ApplyURI(uri).SetMaxPoolSize(maxPool).SetMinPoolSize(cfg.MinPoolSize)
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to connect to DocumentDB: %w", core.ErrDocumentConnection, err)
	}
	return &AWSDocumentDBBackend{client: client, db: client.Database(cfg.Database)}, nil
}

func wrapDoc(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", core.ErrDocument, err)
}

func (b *AWSDocumentDBBackend) InsertOne(ctx context.Context, collection string, document map[string]any) (string, error) {
	res, err := b.db.Collection(collection).InsertOne(ctx, document)
	if err != nil {
		return "", wrapDoc(err)
	}
	return idToString(res.InsertedID), nil
}

func (b *AWSDocumentDBBackend) InsertMany(ctx context.Context, collection string, documents []map[string]any) ([]string, error) {
	docs := make([]any, len(documents))
	for i, d := range documents {
		docs[i] = d
	}
	res, err := b.db.Collection(collection).InsertMany(ctx, docs)
	if err != nil {
		return nil, wrapDoc(err)
	}
	ids := make([]string, len(res.InsertedIDs))
	for i, id := range res.InsertedIDs {
		ids[i] = idToString(id)
	}
	return ids, nil
}

func (b *AWSDocumentDBBackend) FindOne(ctx context.Context, collection string, query map[string]any) (map[string]any, error) {
	var doc map[string]any
	err := b.db.Collection(collection).FindOne(ctx, prepareQuery(query)).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, wrapDoc(err)
	}
	return serializeDoc(doc), nil
}

func (b *AWSDocumentDBBackend) Find(ctx context.Context, collection string, query map[string]any, limit, skip int64) ([]map[string]any, error) {
	cur, err := b.db.Collection(collection).Find(ctx, prepareQuery(query),
		options.Find().SetLimit(limit).SetSkip(skip))
	if err != nil {
		return nil, wrapDoc(err)
	}
	defer cur.Close(ctx)
	var docs []map[string]any
	for cur.Next(ctx) {
		var doc map[string]any
		if err := cur.Decode(&doc); err != nil {
			return nil, wrapDoc(err)
		}
		docs = append(docs, serializeDoc(doc))
	}
	return docs, wrapDoc(cur.Err())
}

func (b *AWSDocumentDBBackend) FindIter(ctx context.Context, collection string, query map[string]any) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		cur, err := b.db.Collection(collection).Find(ctx, prepareQuery(query))
		if err != nil {
			yield(nil, wrapDoc(err))
			return
		}
		defer cur.Close(ctx)
		for cur.Next(ctx) {
			var doc map[string]any
			if err := cur.Decode(&doc); err != nil {
				yield(nil, wrapDoc(err))
				return
			}
			if !yield(serializeDoc(doc), nil) {
				return
			}
		}
		if err := cur.Err(); err != nil {
			yield(nil, wrapDoc(err))
		}
	}
}

func (b *AWSDocumentDBBackend) UpdateOne(ctx context.Context, collection string, query, update map[string]any) (int64, error) {
	res, err := b.db.Collection(collection).UpdateOne(ctx, prepareQuery(query), update)
	if err != nil {
		return 0, wrapDoc(err)
	}
	return res.ModifiedCount, nil
}

func (b *AWSDocumentDBBackend) UpdateMany(ctx context.Context, collection string, query, update map[string]any) (int64, error) {
	res, err := b.db.Collection(collection).UpdateMany(ctx, prepareQuery(query), update)
	if err != nil {
		return 0, wrapDoc(err)
	}
	return res.ModifiedCount, nil
}

func (b *AWSDocumentDBBackend) DeleteOne(ctx context.Context, collection string, query map[string]any) (int64, error) {
	res, err := b.db.Collection(collection).DeleteOne(ctx, prepareQuery(query))
	if err != nil {
		return 0, wrapDoc(err)
	}
	return res.DeletedCount, nil
}

func (b *AWSDocumentDBBackend) DeleteMany(ctx context.Context, collection string, query map[string]any) (int64, error) {
	res, err := b.db.Collection(collection).DeleteMany(ctx, prepareQuery(query))
	if err != nil {
		return 0, wrapDoc(err)
	}
	return res.DeletedCount, nil
}

func (b *AWSDocumentDBBackend) Count(ctx context.Context, collection string, query map[string]any) (int64, error) {
	if query == nil {
		query = map[string]any{}
	}
	n, err := b.db.Collection(collection).CountDocuments(ctx, prepareQuery(query))
	return n, wrapDoc(err)
}

func (b *AWSDocumentDBBackend) CreateIndex(ctx context.Context, collection string, keys []IndexKey, unique bool) (string, error) {
	indexKeys := bson.D{}
	for _, k := range keys {
		indexKeys = append(indexKeys, bson.E{Key: k.Field, Value: k.Order})
	}
	name, err := b.db.Collection(collection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    indexKeys,
		Options: options.Index().SetUnique(unique),
	})
	return name, wrapDoc(err)
}

func (b *AWSDocumentDBBackend) Aggregate(ctx context.Context, collection string, pipeline []map[string]any) ([]map[string]any, error) {
	stages := make([]any, len(pipeline))
	for i, s := range pipeline {
		stages[i] = s
	}
	cur, err := b.db.Collection(collection).Aggregate(ctx, stages)
	if err != nil {
		return nil, wrapDoc(err)
	}
	defer cur.Close(ctx)
	var docs []map[string]any
	for cur.Next(ctx) {
		var doc map[string]any
		if err := cur.Decode(&doc); err != nil {
			return nil, wrapDoc(err)
		}
		docs = append(docs, serializeDoc(doc))
	}
	return docs, wrapDoc(cur.Err())
}

func (b *AWSDocumentDBBackend) UpsertOne(ctx context.Context, collection string, query, update map[string]any) (string, error) {
	res, err := b.db.Collection(collection).UpdateOne(ctx, prepareQuery(query), update,
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return "", wrapDoc(err)
	}
	if res.UpsertedID != nil {
		return idToString(res.UpsertedID), nil
	}
	doc, err := b.FindOne(ctx, collection, query)
	if err != nil || doc == nil {
		return "", err
	}
	if id, ok := doc["_id"].(string); ok {
		return id, nil
	}
	return idToString(doc["_id"]), nil
}

func (b *AWSDocumentDBBackend) HealthCheck(ctx context.Context) bool {
	return b.client.Ping(ctx, readpref.Primary()) == nil
}

func (b *AWSDocumentDBBackend) Close(ctx context.Context) error {
	return wrapDoc(b.client.Disconnect(ctx))
}

// prepareQuery converts a string _id to a bson.ObjectID for MongoDB queries.
func prepareQuery(query map[string]any) map[string]any {
	if id, ok := query["_id"].(string); ok {
		if oid, err := bson.ObjectIDFromHex(id); err == nil {
			out := make(map[string]any, len(query))
			for k, v := range query {
				out[k] = v
			}
			out["_id"] = oid
			return out
		}
	}
	return query
}

// serializeDoc converts an ObjectID _id to its hex string for external use.
func serializeDoc(doc map[string]any) map[string]any {
	if doc == nil {
		return nil
	}
	if oid, ok := doc["_id"].(bson.ObjectID); ok {
		doc["_id"] = oid.Hex()
	}
	return doc
}

// idToString renders an inserted/upserted ID (ObjectID or other) as a string.
func idToString(id any) string {
	if oid, ok := id.(bson.ObjectID); ok {
		return oid.Hex()
	}
	return fmt.Sprint(id)
}

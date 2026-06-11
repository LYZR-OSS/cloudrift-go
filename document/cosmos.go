package document

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"sort"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/NeuralgoLyzr/cloudrift-go/core"
)

// AzureCosmosDBBackend is the Azure Cosmos DB backend (Core/SQL API). It
// exposes the same MongoDB-style interface as DocumentDB by translating a
// supported query subset (top-level field equality; $match/$count/$sort/
// $project/$limit/$skip aggregation stages) into Cosmos SQL.
type AzureCosmosDBBackend struct {
	client           *azcosmos.Client
	databaseName     string
	partitionKeyPath string // e.g. "/id"

	mu         sync.Mutex
	containers map[string]*azcosmos.ContainerClient
}

var _ Backend = (*AzureCosmosDBBackend)(nil)

// NewCosmos constructs a Cosmos DB backend. Routing mirrors the Python
// factory: ConnectionString > AccountKey > ClientSecret (service principal) >
// managed identity. cfg.PartitionKey defaults to "/id".
func NewCosmos(cfg Config) (*AzureCosmosDBBackend, error) {
	pk := cfg.PartitionKey
	if pk == "" {
		pk = "/id"
	}
	var client *azcosmos.Client
	var err error
	switch {
	case cfg.ConnectionString != "":
		client, err = azcosmos.NewClientFromConnectionString(cfg.ConnectionString, nil)

	case cfg.AccountKey != "":
		var cred azcosmos.KeyCredential
		cred, err = azcosmos.NewKeyCredential(cfg.AccountKey)
		if err == nil {
			client, err = azcosmos.NewClientWithKey(cfg.URL, cred, nil)
		}

	case cfg.ClientSecret != "":
		var cred *azidentity.ClientSecretCredential
		cred, err = azidentity.NewClientSecretCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret, nil)
		if err == nil {
			client, err = azcosmos.NewClient(cfg.URL, cred, nil)
		}

	default: // managed identity (system or user-assigned via ClientID)
		var miOpts *azidentity.ManagedIdentityCredentialOptions
		if cfg.ClientID != "" {
			miOpts = &azidentity.ManagedIdentityCredentialOptions{ID: azidentity.ClientID(cfg.ClientID)}
		}
		var cred *azidentity.ManagedIdentityCredential
		cred, err = azidentity.NewManagedIdentityCredential(miOpts)
		if err == nil {
			client, err = azcosmos.NewClient(cfg.URL, cred, nil)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%w: failed to initialize Cosmos DB client: %w", core.ErrDocumentConnection, err)
	}
	return &AzureCosmosDBBackend{
		client:           client,
		databaseName:     cfg.Database,
		partitionKeyPath: pk,
		containers:       make(map[string]*azcosmos.ContainerClient),
	}, nil
}

func (b *AzureCosmosDBBackend) container(collection string) (*azcosmos.ContainerClient, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c, ok := b.containers[collection]; ok {
		return c, nil
	}
	c, err := b.client.NewContainer(b.databaseName, collection)
	if err != nil {
		return nil, wrapCosmosErr(err)
	}
	b.containers[collection] = c
	return c, nil
}

// pkValue extracts the document's partition key value, falling back to its id
// (matching the Python backend's behaviour for the default "/id" path).
func (b *AzureCosmosDBBackend) pkValue(doc map[string]any) string {
	field := strings.TrimPrefix(b.partitionKeyPath, "/")
	if v, ok := doc[field]; ok {
		return fmt.Sprint(v)
	}
	return fmt.Sprint(doc["id"])
}

func (b *AzureCosmosDBBackend) InsertOne(ctx context.Context, collection string, document map[string]any) (string, error) {
	c, err := b.container(collection)
	if err != nil {
		return "", err
	}
	// Cosmos requires an "id"; generate one if absent so InsertOne can return it.
	if _, ok := document["id"]; !ok {
		document = withID(document)
	}
	body, err := json.Marshal(document)
	if err != nil {
		return "", wrapCosmosErr(err)
	}
	pk := azcosmos.NewPartitionKeyString(b.pkValue(document))
	if _, err := c.CreateItem(ctx, pk, body, nil); err != nil {
		return "", wrapCosmosErr(err)
	}
	return fmt.Sprint(document["id"]), nil
}

func (b *AzureCosmosDBBackend) InsertMany(ctx context.Context, collection string, documents []map[string]any) ([]string, error) {
	ids := make([]string, 0, len(documents))
	for _, doc := range documents {
		id, err := b.InsertOne(ctx, collection, doc)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (b *AzureCosmosDBBackend) FindOne(ctx context.Context, collection string, query map[string]any) (map[string]any, error) {
	docs, err := b.Find(ctx, collection, query, 1, 0)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil
	}
	return docs[0], nil
}

func (b *AzureCosmosDBBackend) Find(ctx context.Context, collection string, query map[string]any, limit, skip int64) ([]map[string]any, error) {
	where, params := buildWhere(query)
	sql := fmt.Sprintf("SELECT * FROM c%s OFFSET %d LIMIT %d", where, skip, limit)
	return b.queryItems(ctx, collection, sql, params)
}

func (b *AzureCosmosDBBackend) FindIter(ctx context.Context, collection string, query map[string]any) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		c, err := b.container(collection)
		if err != nil {
			yield(nil, err)
			return
		}
		where, params := buildWhere(query)
		pager := c.NewQueryItemsPager("SELECT * FROM c"+where, azcosmos.NewPartitionKey(),
			&azcosmos.QueryOptions{QueryParameters: params})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				yield(nil, wrapCosmosErr(err))
				return
			}
			for _, raw := range page.Items {
				var doc map[string]any
				if err := json.Unmarshal(raw, &doc); err != nil {
					yield(nil, wrapCosmosErr(err))
					return
				}
				if !yield(doc, nil) {
					return
				}
			}
		}
	}
}

func (b *AzureCosmosDBBackend) queryItems(ctx context.Context, collection, sql string, params []azcosmos.QueryParameter) ([]map[string]any, error) {
	c, err := b.container(collection)
	if err != nil {
		return nil, err
	}
	pager := c.NewQueryItemsPager(sql, azcosmos.NewPartitionKey(),
		&azcosmos.QueryOptions{QueryParameters: params})
	var items []map[string]any
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, wrapCosmosErr(err)
		}
		for _, raw := range page.Items {
			var doc map[string]any
			if err := json.Unmarshal(raw, &doc); err != nil {
				return nil, wrapCosmosErr(err)
			}
			items = append(items, doc)
		}
	}
	return items, nil
}

func (b *AzureCosmosDBBackend) UpdateOne(ctx context.Context, collection string, query, update map[string]any) (int64, error) {
	doc, err := b.FindOne(ctx, collection, query)
	if err != nil || doc == nil {
		return 0, err
	}
	c, err := b.container(collection)
	if err != nil {
		return 0, err
	}
	merged := mergeUpdate(doc, update)
	body, err := json.Marshal(merged)
	if err != nil {
		return 0, wrapCosmosErr(err)
	}
	id := fmt.Sprint(doc["id"])
	pk := azcosmos.NewPartitionKeyString(b.pkValue(doc))
	if _, err := c.ReplaceItem(ctx, pk, id, body, nil); err != nil {
		if isCosmosNotFound(err) {
			return 0, nil
		}
		return 0, wrapCosmosErr(err)
	}
	return 1, nil
}

func (b *AzureCosmosDBBackend) UpdateMany(ctx context.Context, collection string, query, update map[string]any) (int64, error) {
	docs, err := b.Find(ctx, collection, query, 1000, 0)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, doc := range docs {
		n, err := b.UpdateOne(ctx, collection, map[string]any{"id": doc["id"]}, update)
		if err != nil {
			return count, err
		}
		count += n
	}
	return count, nil
}

func (b *AzureCosmosDBBackend) DeleteOne(ctx context.Context, collection string, query map[string]any) (int64, error) {
	doc, err := b.FindOne(ctx, collection, query)
	if err != nil || doc == nil {
		return 0, err
	}
	c, err := b.container(collection)
	if err != nil {
		return 0, err
	}
	pk := azcosmos.NewPartitionKeyString(b.pkValue(doc))
	if _, err := c.DeleteItem(ctx, pk, fmt.Sprint(doc["id"]), nil); err != nil {
		if isCosmosNotFound(err) {
			return 0, nil
		}
		return 0, wrapCosmosErr(err)
	}
	return 1, nil
}

func (b *AzureCosmosDBBackend) DeleteMany(ctx context.Context, collection string, query map[string]any) (int64, error) {
	docs, err := b.Find(ctx, collection, query, 1000, 0)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, doc := range docs {
		n, err := b.DeleteOne(ctx, collection, map[string]any{"id": doc["id"]})
		if err != nil {
			return count, err
		}
		count += n
	}
	return count, nil
}

func (b *AzureCosmosDBBackend) Count(ctx context.Context, collection string, query map[string]any) (int64, error) {
	c, err := b.container(collection)
	if err != nil {
		return 0, err
	}
	where, params := buildWhere(query)
	pager := c.NewQueryItemsPager("SELECT VALUE COUNT(1) FROM c"+where, azcosmos.NewPartitionKey(),
		&azcosmos.QueryOptions{QueryParameters: params})
	var total int64
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, wrapCosmosErr(err)
		}
		for _, raw := range page.Items {
			var n int64
			if err := json.Unmarshal(raw, &n); err != nil {
				return 0, wrapCosmosErr(err)
			}
			total += n
		}
	}
	return total, nil
}

// CreateIndex adds the given fields to the container's included index paths.
// Unique indexes are not supported: Cosmos DB unique key policies must be
// defined at container creation time (returns core.ErrNotImplemented).
func (b *AzureCosmosDBBackend) CreateIndex(ctx context.Context, collection string, keys []IndexKey, unique bool) (string, error) {
	if unique {
		return "", fmt.Errorf(
			"%w: Cosmos DB unique key policies must be defined at container creation time and cannot be added dynamically",
			core.ErrNotImplemented)
	}
	c, err := b.container(collection)
	if err != nil {
		return "", err
	}
	resp, err := c.Read(ctx, nil)
	if err != nil {
		return "", wrapCosmosErr(err)
	}
	props := resp.ContainerProperties
	policy := props.IndexingPolicy
	if policy == nil {
		policy = &azcosmos.IndexingPolicy{
			IndexingMode:  azcosmos.IndexingModeConsistent,
			Automatic:     true,
			ExcludedPaths: []azcosmos.ExcludedPath{{Path: `/"_etag"/?`}},
		}
	}
	existing := make(map[string]struct{}, len(policy.IncludedPaths))
	for _, p := range policy.IncludedPaths {
		existing[p.Path] = struct{}{}
	}
	nameParts := make([]string, 0, len(keys))
	for _, k := range keys {
		entryPath := "/" + strings.TrimPrefix(k.Field, "/") + "/?"
		if _, ok := existing[entryPath]; !ok {
			policy.IncludedPaths = append(policy.IncludedPaths, azcosmos.IncludedPath{Path: entryPath})
		}
		nameParts = append(nameParts, fmt.Sprintf("%s_%d", k.Field, k.Order))
	}
	props.IndexingPolicy = policy
	if _, err := c.Replace(ctx, *props, nil); err != nil {
		return "", wrapCosmosErr(err)
	}
	return strings.Join(nameParts, "_"), nil
}

// Aggregate translates the $match, $count, $sort, $project, $limit, and $skip
// stages to Cosmos SQL. Unsupported stages return core.ErrNotImplemented.
func (b *AzureCosmosDBBackend) Aggregate(ctx context.Context, collection string, pipeline []map[string]any) ([]map[string]any, error) {
	selectClause := "SELECT * FROM c"
	whereClause := ""
	var params []azcosmos.QueryParameter
	orderBy := ""
	var limitVal int64 = -1
	var skipVal int64

	for _, stage := range pipeline {
		switch {
		case stage["$match"] != nil:
			match, ok := stage["$match"].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: $match stage must be a map", core.ErrDocument)
			}
			whereClause, params = buildWhere(match)

		case stage["$count"] != nil:
			countKey := fmt.Sprint(stage["$count"])
			c, err := b.container(collection)
			if err != nil {
				return nil, err
			}
			pager := c.NewQueryItemsPager("SELECT VALUE COUNT(1) FROM c"+whereClause,
				azcosmos.NewPartitionKey(), &azcosmos.QueryOptions{QueryParameters: params})
			var results []map[string]any
			for pager.More() {
				page, err := pager.NextPage(ctx)
				if err != nil {
					return nil, wrapCosmosErr(err)
				}
				for _, raw := range page.Items {
					var n int64
					if err := json.Unmarshal(raw, &n); err != nil {
						return nil, wrapCosmosErr(err)
					}
					results = append(results, map[string]any{countKey: n})
				}
			}
			return results, nil

		case stage["$sort"] != nil:
			sortSpec, ok := stage["$sort"].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: $sort stage must be a map", core.ErrDocument)
			}
			fields := make([]string, 0, len(sortSpec))
			for f := range sortSpec {
				fields = append(fields, f)
			}
			sort.Strings(fields) // deterministic order for map iteration
			parts := make([]string, 0, len(fields))
			for _, f := range fields {
				dir := "DESC"
				if n, ok := toInt(sortSpec[f]); ok && n == 1 {
					dir = "ASC"
				}
				parts = append(parts, fmt.Sprintf("c.%s %s", f, dir))
			}
			orderBy = " ORDER BY " + strings.Join(parts, ", ")

		case stage["$project"] != nil:
			projSpec, ok := stage["$project"].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: $project stage must be a map", core.ErrDocument)
			}
			fields := make([]string, 0, len(projSpec))
			for f, inc := range projSpec {
				if truthy(inc) && f != "_id" {
					fields = append(fields, "c."+f)
				}
			}
			sort.Strings(fields)
			if len(fields) > 0 {
				selectClause = "SELECT " + strings.Join(fields, ", ") + " FROM c"
			}

		case stage["$limit"] != nil:
			if n, ok := toInt(stage["$limit"]); ok {
				limitVal = n
			}

		case stage["$skip"] != nil:
			if n, ok := toInt(stage["$skip"]); ok {
				skipVal = n
			}

		default:
			stageNames := make([]string, 0, len(stage))
			for k := range stage {
				stageNames = append(stageNames, k)
			}
			return nil, fmt.Errorf(
				"%w: Cosmos DB aggregate supports $match, $count, $sort, $project, $limit, $skip; unsupported stage: %v",
				core.ErrNotImplemented, stageNames)
		}
	}

	sql := selectClause + whereClause + orderBy
	// Cosmos SQL requires LIMIT when OFFSET is used.
	if limitVal >= 0 || skipVal > 0 {
		if limitVal < 0 {
			limitVal = 999999
		}
		sql += fmt.Sprintf(" OFFSET %d LIMIT %d", skipVal, limitVal)
	}
	return b.queryItems(ctx, collection, sql, params)
}

func (b *AzureCosmosDBBackend) UpsertOne(ctx context.Context, collection string, query, update map[string]any) (string, error) {
	c, err := b.container(collection)
	if err != nil {
		return "", err
	}
	existing, err := b.FindOne(ctx, collection, query)
	if err != nil {
		return "", err
	}
	var doc map[string]any
	if existing != nil {
		doc = mergeUpdate(existing, update)
	} else {
		doc = mergeUpdate(query, update)
		if _, ok := doc["id"]; !ok {
			doc = withID(doc)
		}
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return "", wrapCosmosErr(err)
	}
	pk := azcosmos.NewPartitionKeyString(b.pkValue(doc))
	if _, err := c.UpsertItem(ctx, pk, body, nil); err != nil {
		return "", wrapCosmosErr(err)
	}
	return fmt.Sprint(doc["id"]), nil
}

func (b *AzureCosmosDBBackend) HealthCheck(ctx context.Context) bool {
	pager := b.client.NewQueryDatabasesPager("SELECT * FROM c", nil)
	if !pager.More() {
		return true
	}
	_, err := pager.NextPage(ctx)
	return err == nil
}

// Close is a no-op: the azcosmos client shares an HTTP transport managed by Go.
func (b *AzureCosmosDBBackend) Close(ctx context.Context) error { return nil }

// buildWhere builds a Cosmos DB SQL WHERE clause from a top-level-equality
// query map, with parameterized values. Fields are sorted for determinism.
func buildWhere(query map[string]any) (string, []azcosmos.QueryParameter) {
	if len(query) == 0 {
		return "", nil
	}
	fields := make([]string, 0, len(query))
	for k := range query {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	conditions := make([]string, 0, len(fields))
	params := make([]azcosmos.QueryParameter, 0, len(fields))
	for i, k := range fields {
		name := fmt.Sprintf("@p%d", i)
		conditions = append(conditions, fmt.Sprintf("c.%s = %s", k, name))
		params = append(params, azcosmos.QueryParameter{Name: name, Value: query[k]})
	}
	return " WHERE " + strings.Join(conditions, " AND "), params
}

// mergeUpdate merges a MongoDB-style update into doc: {"$set": {...}} merges
// the $set fields; a plain map merges directly.
func mergeUpdate(doc, update map[string]any) map[string]any {
	fields := update
	if set, ok := update["$set"].(map[string]any); ok {
		fields = set
	}
	merged := make(map[string]any, len(doc)+len(fields))
	for k, v := range doc {
		merged[k] = v
	}
	for k, v := range fields {
		merged[k] = v
	}
	return merged
}

// withID returns a copy of doc with a generated "id".
func withID(doc map[string]any) map[string]any {
	out := make(map[string]any, len(doc)+1)
	for k, v := range doc {
		out[k] = v
	}
	out["id"] = newCosmosID()
	return out
}

func toInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func truthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case int:
		return t != 0
	case float64:
		return t != 0
	}
	return v != nil
}

func isCosmosNotFound(err error) bool {
	var re *azcore.ResponseError
	return errors.As(err, &re) && re.StatusCode == 404
}

func wrapCosmosErr(err error) error {
	if err == nil {
		return nil
	}
	if isCosmosNotFound(err) {
		return fmt.Errorf("%w: %w", core.ErrDocumentNotFound, err)
	}
	return fmt.Errorf("%w: %w", core.ErrDocument, err)
}

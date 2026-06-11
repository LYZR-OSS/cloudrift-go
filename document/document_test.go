package document

import (
	"context"
	"errors"
	"testing"

	"github.com/NeuralgoLyzr/cloudrift-go/core"
)

func TestNewUnknownProvider(t *testing.T) {
	_, err := New(context.Background(), "dynamodb", Config{})
	if !errors.Is(err, core.ErrDocument) {
		t.Fatalf("err = %v; want core.ErrDocument", err)
	}
}

func TestBuildWhere(t *testing.T) {
	where, params := buildWhere(map[string]any{"name": "Alice", "age": 30})
	// Fields are sorted, so "age" binds @p0 and "name" binds @p1.
	want := " WHERE c.age = @p0 AND c.name = @p1"
	if where != want {
		t.Fatalf("where = %q; want %q", where, want)
	}
	if len(params) != 2 || params[0].Name != "@p0" || params[0].Value != 30 {
		t.Fatalf("params = %+v", params)
	}
	if w, p := buildWhere(nil); w != "" || p != nil {
		t.Fatalf("empty query: %q, %v", w, p)
	}
}

func TestMergeUpdate(t *testing.T) {
	doc := map[string]any{"id": "1", "name": "Alice", "age": 30}
	merged := mergeUpdate(doc, map[string]any{"$set": map[string]any{"age": 31}})
	if merged["age"] != 31 || merged["name"] != "Alice" || merged["id"] != "1" {
		t.Fatalf("merged = %v", merged)
	}
	// Plain (non-$set) updates merge directly.
	merged = mergeUpdate(doc, map[string]any{"age": 32})
	if merged["age"] != 32 || merged["name"] != "Alice" {
		t.Fatalf("merged = %v", merged)
	}
	// The original document is not mutated.
	if doc["age"] != 30 {
		t.Fatalf("doc mutated: %v", doc)
	}
}

func TestPrepareQueryConvertsObjectID(t *testing.T) {
	q := prepareQuery(map[string]any{"_id": "507f1f77bcf86cd799439011"})
	if _, ok := q["_id"].(string); ok {
		t.Fatal("hex _id should be converted to bson.ObjectID")
	}
	// Non-hex strings stay as-is.
	q = prepareQuery(map[string]any{"_id": "not-an-objectid"})
	if q["_id"] != "not-an-objectid" {
		t.Fatalf("_id = %v", q["_id"])
	}
}

func TestWithIDGeneratesUniqueIDs(t *testing.T) {
	a := withID(map[string]any{"v": 1})
	b := withID(map[string]any{"v": 1})
	if a["id"] == "" || a["id"] == b["id"] {
		t.Fatalf("ids = %v, %v", a["id"], b["id"])
	}
}

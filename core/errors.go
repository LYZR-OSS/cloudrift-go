// Package core defines the cloudrift error hierarchy.
//
// Every backend translates provider-native errors (AWS smithy errors,
// *azcore.ResponseError, redis errors, mongo errors, ...) into this hierarchy
// at the boundary, so callers only ever match cloudrift errors:
//
//	if errors.Is(err, core.ErrObjectNotFound) { ... } // specific
//	if errors.Is(err, core.ErrStorage)        { ... } // any storage error
//	if errors.Is(err, core.ErrCloudRift)      { ... } // any cloudrift error
//
// Specific errors wrap their category error, and category errors wrap
// ErrCloudRift, mirroring the Python exception class hierarchy.
package core

import (
	"errors"
	"fmt"
)

// ErrCloudRift is the root of all cloudrift errors.
var ErrCloudRift = errors.New("cloudrift")

// ErrNotImplemented is returned when a provider cannot honor an operation
// (the fail-loud analogue of Python's NotImplementedError).
var ErrNotImplemented = fmt.Errorf("%w: not implemented", ErrCloudRift)

// Category base errors.
var (
	ErrStorage   = fmt.Errorf("%w: storage", ErrCloudRift)
	ErrMessaging = fmt.Errorf("%w: messaging", ErrCloudRift)
	ErrDocument  = fmt.Errorf("%w: document", ErrCloudRift)
	ErrCache     = fmt.Errorf("%w: cache", ErrCloudRift)
	ErrSecret    = fmt.Errorf("%w: secret", ErrCloudRift)
	ErrPubSub    = fmt.Errorf("%w: pubsub", ErrCloudRift)
)

// Storage errors.
var (
	ErrObjectNotFound    = fmt.Errorf("%w: object not found", ErrStorage)
	ErrStoragePermission = fmt.Errorf("%w: permission denied", ErrStorage)
)

// Messaging errors.
var (
	ErrQueueNotFound = fmt.Errorf("%w: queue not found", ErrMessaging)
	ErrMessageSend   = fmt.Errorf("%w: message send failed", ErrMessaging)
)

// Document DB errors. The document package returns a native *mongo.Client,
// so operation errors come from the MongoDB driver directly; only connection
// construction is wrapped.
var (
	ErrDocumentConnection = fmt.Errorf("%w: connection failed", ErrDocument)
)

// Cache errors.
var (
	ErrCacheConnection  = fmt.Errorf("%w: connection failed", ErrCache)
	ErrCacheKeyNotFound = fmt.Errorf("%w: key not found", ErrCache)
)

// Secret errors.
var (
	ErrSecretNotFound   = fmt.Errorf("%w: secret not found", ErrSecret)
	ErrSecretPermission = fmt.Errorf("%w: permission denied", ErrSecret)
)

// Pub/Sub errors.
var (
	ErrTopicNotFound = fmt.Errorf("%w: topic not found", ErrPubSub)
	ErrPublish       = fmt.Errorf("%w: publish failed", ErrPubSub)
)

// Ptr returns a pointer to v. Useful for optional config fields like
// cache.Config.TLS (*bool), where nil means "use the provider default".
func Ptr[T any](v T) *T { return &v }

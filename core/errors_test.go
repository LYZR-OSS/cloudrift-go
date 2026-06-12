package core

import (
	"errors"
	"fmt"
	"testing"
)

func TestHierarchy(t *testing.T) {
	cases := []struct {
		name     string
		specific error
		category error
	}{
		{"storage/not-found", ErrObjectNotFound, ErrStorage},
		{"storage/permission", ErrStoragePermission, ErrStorage},
		{"messaging/queue-not-found", ErrQueueNotFound, ErrMessaging},
		{"messaging/send", ErrMessageSend, ErrMessaging},
		{"document/connection", ErrDocumentConnection, ErrDocument},
		{"cache/connection", ErrCacheConnection, ErrCache},
		{"cache/key-not-found", ErrCacheKeyNotFound, ErrCache},
		{"secret/not-found", ErrSecretNotFound, ErrSecret},
		{"secret/permission", ErrSecretPermission, ErrSecret},
		{"pubsub/topic-not-found", ErrTopicNotFound, ErrPubSub},
		{"pubsub/publish", ErrPublish, ErrPubSub},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A wrapped specific error matches itself, its category, and the root.
			err := fmt.Errorf("%w: some detail: %w", tc.specific, errors.New("sdk error"))
			if !errors.Is(err, tc.specific) {
				t.Errorf("errors.Is(err, specific) = false")
			}
			if !errors.Is(err, tc.category) {
				t.Errorf("errors.Is(err, category) = false")
			}
			if !errors.Is(err, ErrCloudRift) {
				t.Errorf("errors.Is(err, ErrCloudRift) = false")
			}
		})
	}
}

func TestCategoriesAreDistinct(t *testing.T) {
	if errors.Is(ErrObjectNotFound, ErrCache) {
		t.Error("storage error should not match cache category")
	}
	if errors.Is(ErrCacheConnection, ErrStorage) {
		t.Error("cache error should not match storage category")
	}
}

func TestNotImplemented(t *testing.T) {
	err := fmt.Errorf("%w: cosmos unique indexes", ErrNotImplemented)
	if !errors.Is(err, ErrNotImplemented) || !errors.Is(err, ErrCloudRift) {
		t.Error("ErrNotImplemented should wrap ErrCloudRift")
	}
}

package core

import "testing"

func TestNewAzureCredential(t *testing.T) {
	for _, clientID := range []string{"", "00000000-0000-0000-0000-000000000000"} {
		cred, err := NewAzureCredential(clientID)
		if err != nil {
			t.Fatalf("NewAzureCredential(%q): %v", clientID, err)
		}
		if cred == nil {
			t.Fatalf("NewAzureCredential(%q): nil credential", clientID)
		}
	}
}

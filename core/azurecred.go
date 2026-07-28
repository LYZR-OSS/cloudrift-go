package core

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

// NewAzureCredential builds the standard Azure AD credential chain shared by
// every Azure backend:
//
//	Workload Identity -> Managed Identity -> Azure CLI
//
// which covers the three environments a Lyzr service actually runs in — AKS
// with workload identity, App Service / Container Apps / VM with a managed
// identity, and a developer machine with `az login` — without any
// per-environment code.
//
// Mirrors cloudrift (Python) core/azure_credentials.py: the environment,
// shared-token-cache, VS Code, PowerShell, and azd credential sources are
// deliberately excluded so ambient env vars or user-scoped caches can never
// silently shadow the workload's real identity.
//
// clientID selects a user-assigned managed identity (also passed to workload
// identity); empty means the system-assigned identity.
func NewAzureCredential(clientID string) (azcore.TokenCredential, error) {
	var sources []azcore.TokenCredential

	// A constructor error means the source is unavailable in this environment
	// (e.g. no workload-identity env vars outside AKS) — skip it, exactly as
	// DefaultAzureCredential does.
	wiOpts := &azidentity.WorkloadIdentityCredentialOptions{ClientID: clientID}
	if wi, err := azidentity.NewWorkloadIdentityCredential(wiOpts); err == nil {
		sources = append(sources, wi)
	}

	var miOpts *azidentity.ManagedIdentityCredentialOptions
	if clientID != "" {
		miOpts = &azidentity.ManagedIdentityCredentialOptions{ID: azidentity.ClientID(clientID)}
	}
	if mi, err := azidentity.NewManagedIdentityCredential(miOpts); err == nil {
		sources = append(sources, mi)
	}

	if cli, err := azidentity.NewAzureCLICredential(nil); err == nil {
		sources = append(sources, cli)
	}

	return azidentity.NewChainedTokenCredential(sources, nil)
}

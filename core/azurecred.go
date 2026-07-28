package core

import (
	"context"
	"errors"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
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
		sources = append(sources, &imdsTimeoutWrapper{cred: mi, timeout: imdsProbeTimeout})
	}

	if cli, err := azidentity.NewAzureCLICredential(nil); err == nil {
		sources = append(sources, cli)
	}

	return azidentity.NewChainedTokenCredential(sources, nil)
}

// imdsProbeTimeout bounds the first managed-identity token attempt. Outside
// Azure the IMDS endpoint (169.254.169.254) blackholes packets, so an unbounded
// attempt hangs instead of falling through to the Azure CLI — Python's
// DefaultAzureCredential probes IMDS with the same short timeout for the same
// reason.
const imdsProbeTimeout = 2 * time.Second

// imdsTimeoutWrapper is the official azidentity pattern for chaining a
// ManagedIdentityCredential: apply a probe timeout to the first GetToken and
// convert a deadline hit into a credential-unavailable error so the chain
// advances. Once any attempt gets a response from IMDS, the timeout is
// dropped — the endpoint exists, so later slowness is real latency, not
// absence.
type imdsTimeoutWrapper struct {
	cred    azcore.TokenCredential
	timeout time.Duration
}

func (w *imdsTimeoutWrapper) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	if w.timeout <= 0 {
		return w.cred.GetToken(ctx, opts)
	}
	c, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	tk, err := w.cred.GetToken(c, opts)
	if errors.Is(c.Err(), context.DeadlineExceeded) {
		return tk, azidentity.NewCredentialUnavailableError("managed identity timed out; IMDS unreachable")
	}
	w.timeout = 0 // IMDS responded — never time out real token refreshes
	return tk, err
}

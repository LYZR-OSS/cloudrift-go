package cache

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/redis/go-redis/v9"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// AWSElastiCacheBackend is the cache backend for AWS ElastiCache (Redis).
//
// Construct with one of:
//   - NewElastiCacheFromAuthToken — Redis AUTH token (ElastiCache auth token / password)
//   - NewElastiCacheFromIAMAuth   — IAM-based authentication (ElastiCache Redis 7+, SigV4)
//   - NewElastiCacheFromTLSCert   — mTLS with client certificate and key files
type AWSElastiCacheBackend struct {
	redisOps
}

var _ Backend = (*AWSElastiCacheBackend)(nil)

// NewElastiCacheFromAuthToken connects using an ElastiCache AUTH token
// (shared secret). For clusters with Transit Encryption enabled, leave
// cfg.TLS nil (TLS defaults to on). cfg.AuthToken maps to the Redis AUTH
// password. Port defaults to 6379.
func NewElastiCacheFromAuthToken(cfg Config) (*AWSElastiCacheBackend, error) {
	port := portOrDefault(cfg.Port, 6379)
	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, port),
		Password: cfg.AuthToken,
		DB:       cfg.DB,
	}
	if tlsOrDefault(cfg.TLS, true) {
		tlsCfg, err := buildTLSConfig(cfg.Host, cfg.CACerts, "", "")
		if err != nil {
			return nil, err
		}
		opts.TLSConfig = tlsCfg
	}
	return &AWSElastiCacheBackend{redisOps{client: redis.NewClient(opts)}}, nil
}

// NewElastiCacheFromIAMAuth connects using IAM-based authentication
// (ElastiCache Redis 7+ with IAM enabled). A short-lived SigV4 token is
// generated per new connection and refreshed automatically, because go-redis
// re-invokes the credentials provider whenever it dials.
//
// cfg.Username is the Redis ACL username that maps to an IAM identity;
// cfg.Region is required. Explicit credentials (AWSAccessKeyID/...) or a
// ProfileName are optional — the default AWS credential chain is used
// otherwise. Port defaults to 6379; TLS defaults to on (required for IAM auth).
func NewElastiCacheFromIAMAuth(ctx context.Context, cfg Config) (*AWSElastiCacheBackend, error) {
	if cfg.Region == "" {
		return nil, fmt.Errorf("%w: from_iam_auth requires Region", core.ErrCacheConnection)
	}
	port := portOrDefault(cfg.Port, 6379)

	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AWSAccessKeyID != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSSessionToken)))
	} else if cfg.ProfileName != "" {
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(cfg.ProfileName))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to connect to ElastiCache (IAM): %w", core.ErrCacheConnection, err)
	}

	provider := &elastiCacheIAMProvider{
		host:     cfg.Host,
		port:     port,
		username: cfg.Username,
		region:   cfg.Region,
		awsCfg:   awsCfg,
	}
	opts := &redis.Options{
		Addr:                       fmt.Sprintf("%s:%d", cfg.Host, port),
		DB:                         cfg.DB,
		CredentialsProviderContext: provider.credentials,
	}
	if tlsOrDefault(cfg.TLS, true) {
		tlsCfg, err := buildTLSConfig(cfg.Host, cfg.CACerts, "", "")
		if err != nil {
			return nil, err
		}
		opts.TLSConfig = tlsCfg
	}
	return &AWSElastiCacheBackend{redisOps{client: redis.NewClient(opts)}}, nil
}

// NewElastiCacheFromTLSCert connects using mutual TLS (mTLS) with a client
// certificate (cfg.CertFile) and key (cfg.KeyFile), plus an optional
// cfg.AuthToken. Port defaults to 6380.
func NewElastiCacheFromTLSCert(cfg Config) (*AWSElastiCacheBackend, error) {
	port := portOrDefault(cfg.Port, 6380)
	tlsCfg, err := buildTLSConfig(cfg.Host, cfg.CACerts, cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	opts := &redis.Options{
		Addr:      fmt.Sprintf("%s:%d", cfg.Host, port),
		Password:  cfg.AuthToken,
		DB:        cfg.DB,
		TLSConfig: tlsCfg,
	}
	return &AWSElastiCacheBackend{redisOps{client: redis.NewClient(opts)}}, nil
}

// elastiCacheIAMProvider generates a SigV4-signed IAM auth token for
// ElastiCache Redis 7+. Tokens are valid for 15 minutes; go-redis re-calls
// credentials() on each new connection, keeping tokens fresh.
type elastiCacheIAMProvider struct {
	host     string
	port     int
	username string
	region   string
	awsCfg   aws.Config
}

// sha256 of an empty payload, per the SigV4 spec.
const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func (p *elastiCacheIAMProvider) credentials(ctx context.Context) (string, string, error) {
	creds, err := p.awsCfg.Credentials.Retrieve(ctx)
	if err != nil {
		return "", "", fmt.Errorf("%w: retrieving AWS credentials: %w", core.ErrCacheConnection, err)
	}
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://%s:%d/", p.host, p.port), nil)
	if err != nil {
		return "", "", fmt.Errorf("%w: %w", core.ErrCacheConnection, err)
	}
	q := req.URL.Query()
	q.Set("Action", "connect")
	q.Set("User", p.username)
	q.Set("X-Amz-Expires", "900")
	req.URL.RawQuery = q.Encode()

	signer := v4.NewSigner()
	signedURL, _, err := signer.PresignHTTP(
		ctx, creds, req, emptyPayloadHash, "elasticache", p.region, time.Now(),
	)
	if err != nil {
		return "", "", fmt.Errorf("%w: signing IAM auth token: %w", core.ErrCacheConnection, err)
	}
	// Redis expects the signed URL as the password, without the scheme.
	return p.username, strings.TrimPrefix(signedURL, "https://"), nil
}

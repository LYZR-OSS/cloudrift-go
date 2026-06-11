package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/NeuralgoLyzr/cloudrift-go/core"
)

// AWSS3Backend is the AWS S3 storage backend.
//
// A single S3 client is held for the lifetime of the backend and reused for
// all operations. Authentication is chosen by NewS3 based on which Config
// fields are set: static access key, named profile, or the default AWS
// credential chain (IAM role / instance profile / environment).
type AWSS3Backend struct {
	bucket  string
	client  *s3.Client
	presign *s3.PresignClient
}

var _ Backend = (*AWSS3Backend)(nil)

// NewS3 constructs an S3 backend. Region defaults to "us-east-1".
// cfg.EndpointURL points the client at a custom endpoint (LocalStack, MinIO)
// and enables path-style addressing.
func NewS3(ctx context.Context, cfg Config) (*AWSS3Backend, error) {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if cfg.AWSAccessKeyID != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSSessionToken)))
	} else if cfg.ProfileName != "" {
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(cfg.ProfileName))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.EndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
			o.UsePathStyle = true
		}
	})
	return &AWSS3Backend{
		bucket:  cfg.Bucket,
		client:  client,
		presign: s3.NewPresignClient(client),
	}, nil
}

func (b *AWSS3Backend) Upload(ctx context.Context, key string, data []byte, contentType string) (string, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}
	if _, err := b.client.PutObject(ctx, input); err != nil {
		return "", mapS3Err(err, key)
	}
	return key, nil
}

func (b *AWSS3Backend) Download(ctx context.Context, key string) ([]byte, error) {
	resp, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, mapS3Err(err, key)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
	}
	return data, nil
}

func (b *AWSS3Backend) Delete(ctx context.Context, key string) error {
	if _, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return mapS3Err(err, key)
	}
	return nil
}

func (b *AWSS3Backend) Exists(ctx context.Context, key string) (bool, error) {
	_, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return false, nil
		}
		return false, mapS3Err(err, key)
	}
	return true, nil
}

func (b *AWSS3Backend) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for key, err := range b.ListIter(ctx, prefix) {
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (b *AWSS3Backend) ListIter(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
			Bucket: aws.String(b.bucket),
			Prefix: aws.String(prefix),
		})
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				yield("", mapS3Err(err, prefix))
				return
			}
			for _, obj := range page.Contents {
				if !yield(aws.ToString(obj.Key), nil) {
					return
				}
			}
		}
	}
}

func (b *AWSS3Backend) PresignedURL(ctx context.Context, key string, expiresIn time.Duration) (string, error) {
	req, err := b.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expiresIn))
	if err != nil {
		return "", mapS3Err(err, key)
	}
	return req.URL, nil
}

func (b *AWSS3Backend) Copy(ctx context.Context, srcKey, dstKey string) (string, error) {
	if _, err := b.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(b.bucket),
		CopySource: aws.String(b.bucket + "/" + url.PathEscape(srcKey)),
		Key:        aws.String(dstKey),
	}); err != nil {
		return "", mapS3Err(err, srcKey)
	}
	return dstKey, nil
}

func (b *AWSS3Backend) Move(ctx context.Context, srcKey, dstKey string) (string, error) {
	if _, err := b.Copy(ctx, srcKey, dstKey); err != nil {
		return "", err
	}
	if err := b.Delete(ctx, srcKey); err != nil {
		return "", err
	}
	return dstKey, nil
}

func (b *AWSS3Backend) GetMetadata(ctx context.Context, key string) (Metadata, error) {
	resp, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return Metadata{}, mapS3Err(err, key)
	}
	md := Metadata{
		ContentType: aws.ToString(resp.ContentType),
		Size:        aws.ToInt64(resp.ContentLength),
		ETag:        aws.ToString(resp.ETag),
		Custom:      resp.Metadata,
	}
	if resp.LastModified != nil {
		md.LastModified = *resp.LastModified
	}
	return md, nil
}

// UploadStream uploads from a byte stream by buffering it in memory (S3
// requires a content length for signed payloads, mirroring the Python
// backend's accumulate-then-upload behaviour).
func (b *AWSS3Backend) UploadStream(ctx context.Context, key string, r io.Reader, contentType string) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("%w: %w", core.ErrStorage, err)
	}
	return b.Upload(ctx, key, data, contentType)
}

func (b *AWSS3Backend) HealthCheck(ctx context.Context) bool {
	_, err := b.Exists(ctx, "__cloudrift_health__")
	return err == nil
}

// Close is a no-op: the aws-sdk-go-v2 client shares the default HTTP
// transport, which Go manages.
func (b *AWSS3Backend) Close(ctx context.Context) error { return nil }

func isS3NotFound(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		code := ae.ErrorCode()
		if code == "NotFound" || code == "NoSuchKey" || code == "404" {
			return true
		}
	}
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == 404
}

func mapS3Err(err error, key string) error {
	if isS3NotFound(err) {
		return fmt.Errorf("%w: %s: %w", core.ErrObjectNotFound, key, err)
	}
	var ae smithy.APIError
	if errors.As(err, &ae) && (ae.ErrorCode() == "AccessDenied" || ae.ErrorCode() == "403") {
		return fmt.Errorf("%w: %s: %w", core.ErrStoragePermission, key, err)
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == 403 {
		return fmt.Errorf("%w: %s: %w", core.ErrStoragePermission, key, err)
	}
	return fmt.Errorf("%w: %w", core.ErrStorage, err)
}

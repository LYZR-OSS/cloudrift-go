package cache

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// memorystoreConfig points a Config at mr.
func memorystoreConfig(t *testing.T, mr *miniredis.Miniredis) Config {
	t.Helper()
	port, err := strconv.Atoi(mr.Port())
	if err != nil {
		t.Fatal(err)
	}
	return Config{Host: mr.Host(), Port: port}
}

func pingMemorystore(t *testing.T, authMethod string, cfg Config) error {
	t.Helper()
	b, err := New(context.Background(), "memorystore", authMethod, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	_, err = b.Ping(context.Background())
	return err
}

func TestMemorystoreFromAuthString(t *testing.T) {
	mr := miniredis.RunT(t) // plaintext: Memorystore's TLS is opt-in
	mr.RequireAuth("auth-string")
	cfg := memorystoreConfig(t, mr)

	cfg.AuthString = "auth-string"
	if err := pingMemorystore(t, "from_auth_string", cfg); err != nil {
		t.Fatalf("Ping with the AUTH string: %v", err)
	}
	cfg.AuthString = "wrong"
	if err := pingMemorystore(t, "from_auth_string", cfg); err == nil {
		t.Fatal("Ping with a wrong AUTH string succeeded")
	}
}

func TestMemorystoreFromServerCACert(t *testing.T) {
	caFile, serverCert := memorystoreCerts(t)
	mr, err := miniredis.RunTLS(&tls.Config{Certificates: []tls.Certificate{serverCert}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	mr.RequireAuth("auth-string")
	cfg := memorystoreConfig(t, mr)
	cfg.AuthString = "auth-string"

	if _, err := New(context.Background(), "memorystore", "from_server_ca_cert", cfg); !errors.Is(err, core.ErrCacheConnection) {
		t.Fatalf("without CACerts err = %v; want core.ErrCacheConnection", err)
	}
	cfg.CACerts = caFile
	if err := pingMemorystore(t, "from_server_ca_cert", cfg); err != nil {
		t.Fatalf("Ping over TLS pinned to the instance CA: %v", err)
	}
}

// memorystoreCerts mints a per-instance-style CA (written to a PEM file) and a
// server certificate it signs for 127.0.0.1, standing in for Memorystore's
// private CA.
func memorystoreCerts(t *testing.T) (string, tls.Certificate) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "memorystore-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caTmpl, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return caFile, tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvKey}
}

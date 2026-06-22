package email

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/smtp"
	"net/textproto"
	"strings"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// SMTP transport modes.
const (
	modeStartTLS  = "starttls"
	modeTLS       = "tls"
	modePlaintext = "plaintext"
)

// SMTPBackend is a raw SMTP backend (SendGrid, Mailgun, Postmark, Office365,
// MailHog, ...).
//
// A fresh connection is opened per Send. SMTP servers commonly drop idle
// connections and transactional volumes don't benefit from pooling.
type SMTPBackend struct {
	host        string
	port        int
	mode        string
	username    string
	password    string
	defaultFrom string
}

var _ Backend = (*SMTPBackend)(nil)

// NewSMTP constructs an SMTP backend. Mode defaults to "starttls". Port
// defaults by mode: 587 (starttls), 465 (tls), 25 (plaintext).
func NewSMTP(cfg Config) (*SMTPBackend, error) {
	mode := cfg.Mode
	if mode == "" {
		mode = modeStartTLS
	}
	if mode != modeStartTLS && mode != modeTLS && mode != modePlaintext {
		return nil, fmt.Errorf("%w: unknown SMTP mode %q (choose 'starttls', 'tls', or 'plaintext')",
			core.ErrEmail, mode)
	}
	if cfg.Host == "" {
		return nil, fmt.Errorf("%w: SMTP host is required", core.ErrEmail)
	}
	port := cfg.Port
	if port == 0 {
		switch mode {
		case modeTLS:
			port = 465
		case modePlaintext:
			port = 25
		default:
			port = 587
		}
	}
	return &SMTPBackend{
		host:        cfg.Host,
		port:        port,
		mode:        mode,
		username:    cfg.Username,
		password:    cfg.Password,
		defaultFrom: cfg.DefaultFrom,
	}, nil
}

func (b *SMTPBackend) Send(ctx context.Context, msg EmailMessage) (string, error) {
	sender, err := resolveSender(msg, b.defaultFrom)
	if err != nil {
		return "", err
	}

	messageID := newMessageID(sender)
	raw, err := buildMIME(sender, msg, messageID)
	if err != nil {
		return "", fmt.Errorf("%w: building MIME message: %w", core.ErrEmailSend, err)
	}

	// Bcc recipients go in the envelope, never in the headers.
	recipients := make([]string, 0, len(msg.To)+len(msg.Cc)+len(msg.Bcc))
	recipients = append(recipients, msg.To...)
	recipients = append(recipients, msg.Cc...)
	recipients = append(recipients, msg.Bcc...)

	if err := b.deliver(ctx, sender, recipients, raw); err != nil {
		return "", err
	}
	return messageID, nil
}

func (b *SMTPBackend) SendBatch(ctx context.Context, msgs []EmailMessage) ([]string, error) {
	return sendBatchLoop(ctx, b, msgs)
}

// deliver opens a connection per the mode, authenticates if credentials are
// set, and writes the message.
func (b *SMTPBackend) deliver(ctx context.Context, sender string, recipients []string, raw []byte) error {
	client, err := b.dial(ctx)
	if err != nil {
		return mapSMTPErr(err)
	}
	defer client.Close()

	if b.username != "" {
		auth := smtp.PlainAuth("", b.username, b.password, b.host)
		if err := client.Auth(auth); err != nil {
			return mapSMTPErr(err)
		}
	}
	if err := client.Mail(sender); err != nil {
		return mapSMTPErr(err)
	}
	for _, rcpt := range recipients {
		if err := client.Rcpt(rcpt); err != nil {
			return mapSMTPErr(err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return mapSMTPErr(err)
	}
	if _, err := w.Write(raw); err != nil {
		return mapSMTPErr(err)
	}
	if err := w.Close(); err != nil {
		return mapSMTPErr(err)
	}
	return client.Quit()
}

// dial establishes the SMTP connection and performs the TLS handshake or
// STARTTLS upgrade per the configured mode.
func (b *SMTPBackend) dial(ctx context.Context) (*smtp.Client, error) {
	addr := net.JoinHostPort(b.host, fmt.Sprint(b.port))
	var dialer net.Dialer

	if b.mode == modeTLS {
		conn, err := tls.DialWithDialer(&dialer, "tcp", addr, &tls.Config{ServerName: b.host})
		if err != nil {
			return nil, err
		}
		return smtp.NewClient(conn, b.host)
	}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	client, err := smtp.NewClient(conn, b.host)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if b.mode == modeStartTLS {
		if err := client.StartTLS(&tls.Config{ServerName: b.host}); err != nil {
			client.Close()
			return nil, err
		}
	}
	return client, nil
}

func (b *SMTPBackend) HealthCheck(ctx context.Context) bool {
	client, err := b.dial(ctx)
	if err != nil {
		return false
	}
	defer client.Close()
	if err := client.Noop(); err != nil {
		return false
	}
	return client.Quit() == nil
}

// Close is a no-op: SMTPBackend opens a fresh connection per Send and holds no
// long-lived sockets.
func (b *SMTPBackend) Close(ctx context.Context) error { return nil }

// newMessageID generates an RFC 5322 Message-ID using the sender's domain (or
// "localhost" when it cannot be derived).
func newMessageID(sender string) string {
	domain := "localhost"
	if i := strings.LastIndex(sender, "@"); i >= 0 && i < len(sender)-1 {
		domain = sender[i+1:]
		domain = strings.Trim(domain, "<> ")
	}
	var rnd [16]byte
	_, _ = rand.Read(rnd[:])
	return fmt.Sprintf("<%s@%s>", hex.EncodeToString(rnd[:]), domain)
}

func mapSMTPErr(err error) error {
	// net/smtp surfaces SMTP protocol replies as *textproto.Error.
	var protoErr *textproto.Error
	if errors.As(err, &protoErr) {
		switch protoErr.Code {
		case 421, 450, 451, 452:
			return fmt.Errorf("%w: %w", core.ErrEmailThrottled, err)
		case 550, 551, 553:
			return fmt.Errorf("%w: %w", core.ErrRecipientRejected, err)
		}
	}
	return fmt.Errorf("%w: %w", core.ErrEmailSend, err)
}

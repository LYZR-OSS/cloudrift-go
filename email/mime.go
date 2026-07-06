package email

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/textproto"
	"strings"
)

// buildMIME builds a raw RFC 5322 MIME message.
//
// Structure:
//   - text-only / html-only  → a single text/plain or text/html body part
//   - text + html            → multipart/alternative
//   - any attachments        → multipart/mixed wrapping the body part(s)
//
// Bcc is intentionally NOT written to the headers — Bcc recipients belong only
// in the SMTP envelope. The caller is responsible for the envelope.
//
// messageID, when non-empty, is written as the Message-ID header.
func buildMIME(sender string, msg EmailMessage, messageID string) ([]byte, error) {
	var buf bytes.Buffer

	writeHeader(&buf, "From", sender)
	if len(msg.To) > 0 {
		writeHeader(&buf, "To", strings.Join(msg.To, ", "))
	}
	if len(msg.Cc) > 0 {
		writeHeader(&buf, "Cc", strings.Join(msg.Cc, ", "))
	}
	if len(msg.ReplyTo) > 0 {
		writeHeader(&buf, "Reply-To", strings.Join(msg.ReplyTo, ", "))
	}
	writeHeader(&buf, "Subject", msg.Subject)
	if messageID != "" {
		writeHeader(&buf, "Message-ID", messageID)
	}
	for k, v := range msg.Headers {
		// Skip headers we manage ourselves to avoid duplicates.
		switch textproto.CanonicalMIMEHeaderKey(k) {
		case "From", "To", "Cc", "Reply-To", "Subject", "Message-Id",
			"Mime-Version", "Content-Type", "Content-Transfer-Encoding":
			continue
		}
		writeHeader(&buf, k, v)
	}
	buf.WriteString("MIME-Version: 1.0\r\n")

	if len(msg.Attachments) > 0 {
		return buildMixed(&buf, msg)
	}
	return buildBody(&buf, msg)
}

// buildBody writes the body directly into the message headers (no attachments).
func buildBody(buf *bytes.Buffer, msg EmailMessage) ([]byte, error) {
	if msg.BodyText != "" && msg.BodyHTML != "" {
		mw := multipart.NewWriter(buf)
		writeHeader(buf, "Content-Type", "multipart/alternative; boundary="+mw.Boundary())
		buf.WriteString("\r\n")
		if err := writeAlternative(mw, msg); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	ctype := "text/plain; charset=UTF-8"
	body := msg.BodyText
	if msg.BodyHTML != "" {
		ctype = "text/html; charset=UTF-8"
		body = msg.BodyHTML
	}
	writeHeader(buf, "Content-Type", ctype)
	buf.WriteString("Content-Transfer-Encoding: quoted-printable\r\n\r\n")
	if err := writeQP(buf, body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// buildMixed wraps the body part(s) and attachments in multipart/mixed.
func buildMixed(buf *bytes.Buffer, msg EmailMessage) ([]byte, error) {
	mw := multipart.NewWriter(buf)
	writeHeader(buf, "Content-Type", "multipart/mixed; boundary="+mw.Boundary())
	buf.WriteString("\r\n")

	if err := writeBodyPart(mw, msg); err != nil {
		return nil, err
	}

	for _, att := range msg.Attachments {
		h := make(textproto.MIMEHeader)
		h.Set("Content-Type", att.contentType())
		h.Set("Content-Transfer-Encoding", "base64")
		h.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", att.Filename))
		pw, err := mw.CreatePart(h)
		if err != nil {
			return nil, err
		}
		if err := writeBase64(pw, att.Content); err != nil {
			return nil, err
		}
	}

	if err := mw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeBodyPart writes the body (single part or nested multipart/alternative)
// as one part of an enclosing multipart writer.
func writeBodyPart(mw *multipart.Writer, msg EmailMessage) error {
	if msg.BodyText != "" && msg.BodyHTML != "" {
		var inner bytes.Buffer
		altW := multipart.NewWriter(&inner)
		h := make(textproto.MIMEHeader)
		h.Set("Content-Type", "multipart/alternative; boundary="+altW.Boundary())
		pw, err := mw.CreatePart(h)
		if err != nil {
			return err
		}
		if err := writeAlternative(altW, msg); err != nil {
			return err
		}
		_, err = pw.Write(inner.Bytes())
		return err
	}

	ctype := "text/plain; charset=UTF-8"
	body := msg.BodyText
	if msg.BodyHTML != "" {
		ctype = "text/html; charset=UTF-8"
		body = msg.BodyHTML
	}
	h := make(textproto.MIMEHeader)
	h.Set("Content-Type", ctype)
	h.Set("Content-Transfer-Encoding", "quoted-printable")
	pw, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	return writeQP(pw, body)
}

// writeAlternative writes a text/plain then text/html part into a
// multipart/alternative writer and closes it.
func writeAlternative(mw *multipart.Writer, msg EmailMessage) error {
	for _, part := range []struct{ ctype, body string }{
		{"text/plain; charset=UTF-8", msg.BodyText},
		{"text/html; charset=UTF-8", msg.BodyHTML},
	} {
		h := make(textproto.MIMEHeader)
		h.Set("Content-Type", part.ctype)
		h.Set("Content-Transfer-Encoding", "quoted-printable")
		pw, err := mw.CreatePart(h)
		if err != nil {
			return err
		}
		if err := writeQP(pw, part.body); err != nil {
			return err
		}
	}
	return mw.Close()
}

func writeHeader(buf *bytes.Buffer, key, value string) {
	buf.WriteString(textproto.CanonicalMIMEHeaderKey(key))
	buf.WriteString(": ")
	// RFC 2047-encode values with non-ASCII so headers stay 7-bit clean.
	buf.WriteString(mime.QEncoding.Encode("UTF-8", value))
	buf.WriteString("\r\n")
}

func writeQP(w interface{ Write([]byte) (int, error) }, body string) error {
	qp := quotedprintable.NewWriter(w)
	if _, err := qp.Write([]byte(body)); err != nil {
		return err
	}
	return qp.Close()
}

func writeBase64(w interface{ Write([]byte) (int, error) }, content []byte) error {
	const lineLen = 76
	enc := base64.StdEncoding.EncodeToString(content)
	for i := 0; i < len(enc); i += lineLen {
		end := i + lineLen
		if end > len(enc) {
			end = len(enc)
		}
		if _, err := w.Write([]byte(enc[i:end])); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\r\n")); err != nil {
			return err
		}
	}
	return nil
}

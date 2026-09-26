package document

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

// parseFirestore parses uri with the driver's own parser — the component that
// would reject it at connect time — and checks Firestore's mandatory options.
func parseFirestore(t *testing.T, uri string) *connstring.ConnString {
	t.Helper()
	cs, err := connstring.ParseAndValidate(uri)
	if err != nil {
		t.Fatalf("driver rejected %q: %v", uri, err)
	}
	if !cs.LoadBalanced || !cs.SSL || !cs.RetryWritesSet || cs.RetryWrites {
		t.Fatalf("%q: loadBalanced=%v tls=%v retryWrites(set=%v)=%v; want true, true, false",
			uri, cs.LoadBalanced, cs.SSL, cs.RetryWritesSet, cs.RetryWrites)
	}
	return cs
}

func TestFirestoreURIAuthPaths(t *testing.T) {
	triple := Config{UID: "f116f93a", Location: "nam5", Database: "mydb"}

	// OIDC is the default: no credentials, workload identity via the driver.
	got := firestoreURI(triple)
	want := "mongodb://f116f93a.nam5.firestore.goog:443/mydb?loadBalanced=true&tls=true&retryWrites=false" +
		"&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE"
	if got != want {
		t.Fatalf("uri = %q; want %q", got, want)
	}
	cs := parseFirestore(t, got)
	if cs.AuthMechanism != "MONGODB-OIDC" || cs.AuthMechanismProperties["ENVIRONMENT"] != "gcp" ||
		cs.AuthMechanismProperties["TOKEN_RESOURCE"] != "FIRESTORE" || cs.Database != "mydb" {
		t.Fatalf("OIDC parsed as %s %v db=%q", cs.AuthMechanism, cs.AuthMechanismProperties, cs.Database)
	}

	// SCRAM: credentials full of URI-special characters survive the round trip.
	scram := triple
	scram.Username, scram.Password = "svc user", "p@ss:w/rd+ %#?"
	cs = parseFirestore(t, firestoreURI(scram))
	if cs.AuthMechanism != "SCRAM-SHA-256" || cs.Username != scram.Username || cs.Password != scram.Password {
		t.Fatalf("SCRAM parsed as %s %q/%q; want %q/%q",
			cs.AuthMechanism, cs.Username, cs.Password, scram.Username, scram.Password)
	}

	// Access token rides PLAIN against $external, and wins over a password.
	token := scram
	token.AccessToken = "ya29.a0-token_value"
	cs = parseFirestore(t, firestoreURI(token))
	if cs.AuthMechanism != "PLAIN" || cs.AuthSource != "$external" || cs.Password != token.AccessToken {
		t.Fatalf("token parsed as %s source=%q password=%q", cs.AuthMechanism, cs.AuthSource, cs.Password)
	}

	triple.Port = 10443
	if cs := parseFirestore(t, firestoreURI(triple)); cs.Hosts[0] != "f116f93a.nam5.firestore.goog:10443" {
		t.Fatalf("hosts = %v", cs.Hosts)
	}
}

func TestEnsureFirestoreParams(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{
			name: "complete connection string passes through untouched",
			in: "mongodb://u:p@uid.nam5.firestore.goog:443/mydb?loadBalanced=true&tls=true" +
				"&authMechanism=SCRAM-SHA-256&retryWrites=false",
			want: "mongodb://u:p@uid.nam5.firestore.goog:443/mydb?loadBalanced=true&tls=true" +
				"&authMechanism=SCRAM-SHA-256&retryWrites=false",
		},
		{
			name: "bare endpoint gets the mandatory options and OIDC",
			in:   "mongodb://uid.nam5.firestore.goog:443/mydb",
			want: "mongodb://uid.nam5.firestore.goog:443/mydb?loadBalanced=true&tls=true&retryWrites=false" +
				"&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE",
		},
		{
			name: "names match case-insensitively, ssl counts as tls, credentials suppress OIDC",
			in:   "mongodb://u:p@uid.nam5.firestore.goog:443/mydb?LOADBALANCED=true&ssl=true",
			want: "mongodb://u:p@uid.nam5.firestore.goog:443/mydb?LOADBALANCED=true&ssl=true&retryWrites=false",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ensureFirestoreParams(tc.in)
			if got != tc.want {
				t.Fatalf("got  %q\nwant %q", got, tc.want)
			}
			parseFirestore(t, got)
		})
	}
}

func TestNewFirestoreAcceptsEveryAuthPath(t *testing.T) {
	// mongo.Connect validates the URI and auth mechanism properties without
	// dialing, so each path must construct a client.
	for name, cfg := range map[string]Config{
		"oidc":  {UID: "uid", Location: "nam5", Database: "mydb"},
		"scram": {UID: "uid", Location: "nam5", Database: "mydb", Username: "u", Password: "p"},
		"token": {UID: "uid", Location: "nam5", Database: "mydb", AccessToken: "ya29.token"},
		"uri":   {ConnectionString: "mongodb://uid.nam5.firestore.goog:443/mydb"},
	} {
		client, err := New("firestore", cfg)
		if err != nil {
			t.Fatalf("%s: New: %v", name, err)
		}
		if err := client.Disconnect(context.Background()); err != nil {
			t.Fatalf("%s: Disconnect: %v", name, err)
		}
	}
}

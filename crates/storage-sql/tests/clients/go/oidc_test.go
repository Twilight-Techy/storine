package sqlclients

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
)

// mintToken returns a JWT signed with the test issuer's key (tests/clients/oidc/test-key.pem),
// as an OpenID Connect provider would issue it.
func mintToken(t *testing.T, keyFile, extraClaims string) string {
	raw, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(raw)
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	b64 := base64.RawURLEncoding.EncodeToString
	head := b64([]byte(`{"alg":"RS256","kid":"storine-test","typ":"JWT"}`))
	body := b64([]byte(fmt.Sprintf(`{"iss":"https://id.storine.test/realms/test","aud":"storine","exp":%d,"sub":"u-1"%s}`,
		time.Now().Unix()+300, extraClaims)))
	digest := sha256.Sum256([]byte(head + "." + body))
	sig, err := rsa.SignPKCS1v15(nil, parsed.(*rsa.PrivateKey), crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return head + "." + body + "." + b64(sig)
}

func tokenDB(t *testing.T, addr, token string) *sql.DB {
	host, port, _ := strings.Cut(addr, ":")
	conn, err := mssql.NewAccessTokenConnector(
		"sqlserver://"+host+":"+port+"?database=shop&encrypt=true&TrustServerCertificate=true&dial+timeout=10",
		func() (string, error) { return token, nil })
	if err != nil {
		t.Fatal(err)
	}
	return sql.OpenDB(conn)
}

// OpenID Connect access tokens through go-mssqldb's access-token connector. The main server has
// no issuer configured and says so; STORINE_MSSQL_AUTH names one started with --oidc-*.
func TestMSSQLAccessToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	off := tokenDB(t, msAddr, "a.b.c")
	if err := off.PingContext(ctx); err == nil || !strings.Contains(err.Error(), "not enabled") {
		t.Fatalf("token login on a server without an issuer: %v", err)
	}
	off.Close()
	auth, key := os.Getenv("STORINE_MSSQL_AUTH"), os.Getenv("STORINE_OIDC_KEY")
	if auth == "" || key == "" {
		t.Skip("STORINE_MSSQL_AUTH / STORINE_OIDC_KEY not set")
	}
	db := tokenDB(t, auth, mintToken(t, key, `,"preferred_username":"ann"`))
	var who string
	if err := db.QueryRowContext(ctx, "SELECT SUSER_NAME()").Scan(&who); err != nil || who != "ann" {
		t.Fatalf("SUSER_NAME() = %q, %v", who, err)
	}
	db.Close()
	other := tokenDB(t, auth, mintToken(t, key, `,"iss":"https://elsewhere.test"`))
	if err := other.PingContext(ctx); err == nil || !strings.Contains(err.Error(), "issuer") {
		t.Fatalf("foreign issuer: %v", err)
	}
	other.Close()
}

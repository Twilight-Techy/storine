package sqlclients

import (
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	_ "github.com/microsoft/go-mssqldb/integratedauth/krb5"
)

// Kerberos through go-mssqldb's krb5 authenticator (pure Go, gokrb5) with ann's ticket cache.
// STORINE_MSSQL_KRB5 names a server started with --kerberos-keytab; STORINE_KRB5_DIR the test
// realm from kerberos/realm.sh.
func TestMSSQLKerberos(t *testing.T) {
	addr, dir := os.Getenv("STORINE_MSSQL_KRB5"), os.Getenv("STORINE_KRB5_DIR")
	if addr == "" || dir == "" {
		t.Skip("STORINE_MSSQL_KRB5 / STORINE_KRB5_DIR not set")
	}
	_, port, _ := strings.Cut(addr, ":")
	q := url.Values{
		"database":               {"shop"},
		"encrypt":                {"true"},
		"TrustServerCertificate": {"true"},
		"authenticator":          {"krb5"},
		"krb5-configfile":        {dir + "/krb5.conf"},
		"krb5-credcachefile":     {dir + "/ann.ccache"},
		// go-mssqldb canonicalizes the SPN host with a CNAME lookup and drops the last character of
		// an answer without a trailing dot (as for localhost); this name does not resolve.
		"ServerSPN": {"MSSQLSvc/sql.storine.test:" + port},
	}
	db, err := sql.Open("sqlserver", "sqlserver://localhost:"+port+"?"+q.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var who string
	if err := db.QueryRow("SELECT SUSER_NAME()").Scan(&who); err != nil || who != "ann" {
		t.Fatalf("SUSER_NAME() = %q, %v", who, err)
	}
}

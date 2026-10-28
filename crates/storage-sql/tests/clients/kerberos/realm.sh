#!/usr/bin/env bash
# Creates a throwaway MIT Kerberos realm STORINE.TEST in DIR, with a KDC on 127.0.0.1:PORT, the
# user ann@STORINE.TEST (password "ann-secret", tickets in DIR/ann.ccache) and the SQL Server
# service MSSQLSvc/localhost:SQLPORT (also as MSSQLSvc/sql.storine.test:SQLPORT, a name that
# does not resolve) whose keys are exported to DIR/storine-sql.keytab.
#
#   kerberos/realm.sh DIR KDC_PORT SQL_PORT     (needs krb5kdc, kdb5_util, kadmin.local, kinit)
#   . DIR/env                                    (KRB5_CONFIG, KRB5CCNAME, ...)
#   kill "$(cat DIR/kdc.pid)"
set -eu
dir=$1 kdc_port=$2 sql_port=$3
mkdir -p "$dir"
cat >"$dir/krb5.conf" <<CONF
[libdefaults]
  default_realm = STORINE.TEST
  dns_lookup_kdc = false
  dns_lookup_realm = false
  rdns = false
  dns_canonicalize_hostname = false
  udp_preference_limit = 1
[realms]
  STORINE.TEST = {
    kdc = 127.0.0.1:$kdc_port
  }
[domain_realm]
  localhost = STORINE.TEST
  .localhost = STORINE.TEST
CONF
cat >"$dir/kdc.conf" <<CONF
[kdcdefaults]
  kdc_ports = $kdc_port
  kdc_tcp_ports = $kdc_port
[realms]
  STORINE.TEST = {
    database_name = $dir/principal
    key_stash_file = $dir/stash
    acl_file = $dir/kadm5.acl
    supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal aes256-cts-hmac-sha384-192:normal aes128-cts-hmac-sha256-128:normal
  }
CONF
: >"$dir/kadm5.acl"
export KRB5_CONFIG="$dir/krb5.conf" KRB5_KDC_PROFILE="$dir/kdc.conf"
kdb5_util create -s -r STORINE.TEST -P master-secret >/dev/null
kadmin.local -q "addprinc -pw ann-secret ann" >/dev/null
for host in localhost sql.storine.test; do
  kadmin.local -q "addprinc -randkey MSSQLSvc/$host:$sql_port" >/dev/null
  kadmin.local -q "ktadd -k $dir/storine-sql.keytab MSSQLSvc/$host:$sql_port" >/dev/null
done
# A service the KDC knows but storine-sql has no key for (tests the refusal).
kadmin.local -q "addprinc -randkey MSSQLSvc/other.storine.test:$sql_port" >/dev/null
krb5kdc -P "$dir/kdc.pid"
for _ in $(seq 50); do (echo >/dev/tcp/127.0.0.1/"$kdc_port") 2>/dev/null && break; sleep 0.1; done
echo ann-secret | KRB5CCNAME="$dir/ann.ccache" kinit ann >/dev/null
cat >"$dir/env" <<ENV
export KRB5_CONFIG=$dir/krb5.conf KRB5CCNAME=FILE:$dir/ann.ccache
export STORINE_KRB5_DIR=$dir STORINE_KRB5_KEYTAB=$dir/storine-sql.keytab
ENV

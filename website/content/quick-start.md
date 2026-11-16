---
title: Quick start
description: Start a server, open the console, and read and write documents from your language, in about five minutes.
---

This guide assumes you have [installed](installation.md) the Storine programs.

## 1. Start a server

```bash
storage-server ./data 127.0.0.1:7878
```

The first argument is the data directory, created if it does not exist. The second is the address to listen on. The server prints one line once it is listening, and keeps running until you stop it with `Ctrl C`.

Without tokens the server only accepts a loopback address such as `127.0.0.1`. To listen on the network, give it tokens first: see [security](security.md).

## 2. Open the console

In another terminal:

```bash
storine-console storine://127.0.0.1:7878
```

It prints a link with a key made for this run. Open it in your browser to see the server's health, browse collections, and edit documents. More in [the web console](console.md).

## 3. Get a client

Pick your language. The clients are installed from the repository, and need nothing but their language's standard library.

```bash tab=Python
pip install "git+https://github.com/Twilight-Techy/storine#subdirectory=sdks/python"
```

```bash tab=TypeScript
git clone https://github.com/Twilight-Techy/storine
(cd storine/sdks/typescript && npm install && npm run build)
npm install ./storine/sdks/typescript
```

```bash tab=Go
go get github.com/twilight-techy/storine/sdks/go
```

```bash tab=Java
git clone https://github.com/Twilight-Techy/storine
mvn -f storine/sdks/java/pom.xml install -DskipTests
# then depend on io.storine:storine:0.1.0
```

```bash tab=C#
git clone https://github.com/Twilight-Techy/storine
dotnet add reference storine/sdks/csharp/src/Storine/Storine.csproj
```

```toml tab=Rust
[dependencies]
storine = { git = "https://github.com/twilight-techy/storine", package = "storine" }
```

```bash tab=PHP
git clone https://github.com/Twilight-Techy/storine
# then: require 'storine/sdks/php/src/autoload.php';
```

```bash tab=Ruby
git clone https://github.com/Twilight-Techy/storine
cd storine/sdks/ruby && gem build storine.gemspec && gem install storine-*.gem
```

## 4. Write and read documents

This program creates two accounts, moves money between them in one atomic transaction, and queries them. The balance is an exact decimal, and `min=0` makes the server refuse any change that would take it below zero.

```python tab=Python
from decimal import Decimal
import storine
from storine import add, eq, gt

db = storine.connect("storine://127.0.0.1:7878")
accounts = db.collection("accounts")

accounts.insert("alice", {"owner": "Alice", "balance": Decimal("100.00")})
accounts.insert("bob", {"owner": "Bob", "balance": Decimal("0.00")})

# Atomic, exact, server-side: no lost updates, no overdraft.
accounts.update("alice", add("balance", Decimal("-25.00"), min=0))

# Several writes, all or nothing.
with db.transaction() as tx:
    tx.update("accounts", "alice", add("balance", Decimal("-10.00"), min=0))
    tx.update("accounts", "bob", add("balance", Decimal("10.00")))

doc = accounts.get("alice")
print(doc["balance"], doc.version)      # 65.00 and the document's version

accounts.create_index("owner")
rich = accounts.find(gt("balance", Decimal("50")), order_by="-balance", limit=10)
print(accounts.count(eq("owner", "Bob")))
```

```ts tab=TypeScript
import { connect, add, gt, eq, Decimal } from "storine";

const db = connect("storine://127.0.0.1:7878");
const accounts = db.collection("accounts");

await accounts.insert("alice", { owner: "Alice", balance: new Decimal("100.00") });
await accounts.insert("bob", { owner: "Bob", balance: new Decimal("0.00") });

// Atomic, exact, server-side: no lost updates, no overdraft.
await accounts.update("alice", add("balance", new Decimal("-25.00"), { min: 0 }));

// Several writes, all or nothing.
await db.transaction((tx) => {
  tx.update("accounts", "alice", add("balance", new Decimal("-10.00"), { min: 0 }));
  tx.update("accounts", "bob", add("balance", new Decimal("10.00")));
});

const doc = await accounts.get("alice");
console.log(doc?.fields.balance.toString(), doc?.version);   // "65.00" and the version

await accounts.createIndex("owner");
const rich = await accounts.find(gt("balance", new Decimal("50")), { orderBy: "-balance", limit: 10 });
console.log(await accounts.count(eq("owner", "Bob")));
db.close();
```

```go tab=Go
package main

import (
	"context"
	"fmt"

	storine "github.com/twilight-techy/storine/sdks/go"
)

func main() {
	ctx := context.Background()
	db, err := storine.Connect("storine://127.0.0.1:7878")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	accounts := db.Collection("accounts")

	accounts.Insert(ctx, "alice", storine.Doc{"owner": "Alice", "balance": storine.MustDecimal("100.00")})
	accounts.Insert(ctx, "bob", storine.Doc{"owner": "Bob", "balance": storine.MustDecimal("0.00")})

	// Atomic, exact, server-side: no lost updates, no overdraft.
	accounts.Update(ctx, "alice", storine.Add("balance", storine.MustDecimal("-25.00")).Min(0))

	// Several writes, all or nothing.
	db.Transaction(ctx, func(tx *storine.Tx) error {
		tx.Update("accounts", "alice", storine.Add("balance", storine.MustDecimal("-10.00")).Min(0))
		tx.Update("accounts", "bob", storine.Add("balance", storine.MustDecimal("10.00")))
		return nil
	})

	doc, _ := accounts.Get(ctx, "alice")
	fmt.Println(doc.Fields["balance"], doc.Version) // 65.00 and the version

	accounts.CreateIndex(ctx, "owner")
	rich, _ := accounts.Find(ctx, storine.Query{Where: storine.Gt("balance", storine.MustDecimal("50")), OrderBy: []string{"-balance"}, Limit: 10})
	n, _ := accounts.Count(ctx, storine.Eq("owner", "Bob"))
	fmt.Println(len(rich), n)
}
```

```java tab=Java
import static io.storine.FieldOp.add;
import static io.storine.Filter.*;

import io.storine.*;
import java.math.BigDecimal;
import java.util.Map;

try (Storine db = Storine.connect("storine://127.0.0.1:7878")) {
    Collection accounts = db.collection("accounts");

    accounts.insert("alice", Map.of("owner", "Alice", "balance", new BigDecimal("100.00")));
    accounts.insert("bob", Map.of("owner", "Bob", "balance", new BigDecimal("0.00")));

    // Atomic, exact, server-side: no lost updates, no overdraft.
    accounts.update("alice", add("balance", new BigDecimal("-25.00")).min(0));

    // Several writes, all or nothing.
    db.transaction(tx -> {
        tx.update("accounts", "alice", add("balance", new BigDecimal("-10.00")).min(0));
        tx.update("accounts", "bob", add("balance", new BigDecimal("10.00")));
    });

    Document doc = accounts.get("alice").orElseThrow();
    System.out.println(doc.get("balance") + " " + doc.version());   // 65.00 and the version

    accounts.createIndex("owner");
    var rich = accounts.find(new Query().where(gt("balance", new BigDecimal("50"))).orderBy("-balance").limit(10));
    long bobs = accounts.count(eq("owner", "Bob"));
}
```

```csharp tab=C#
using Storine;
using static Storine.FieldOp;
using static Storine.Filter;

await using var db = StorineClient.Connect("storine://127.0.0.1:7878");
var accounts = db.Collection("accounts");

await accounts.InsertAsync("alice", new Doc { ["owner"] = "Alice", ["balance"] = 100.00m });
await accounts.InsertAsync("bob", new Doc { ["owner"] = "Bob", ["balance"] = 0.00m });

// Atomic, exact, server-side: no lost updates, no overdraft.
await accounts.UpdateAsync("alice", Add("balance", -25.00m).Min(0));

// Several writes, all or nothing.
await db.TransactionAsync(tx =>
{
    tx.Update("accounts", "alice", Add("balance", -10.00m).Min(0));
    tx.Update("accounts", "bob", Add("balance", 10.00m));
});

var doc = await accounts.GetAsync("alice");
Console.WriteLine($"{doc!["balance"]} {doc.Version}");   // 65.00 and the version

await accounts.CreateIndexAsync("owner");
var rich = await accounts.FindAsync(Gt("balance", 50m), orderBy: "-balance", limit: 10);
long bobs = await accounts.CountAsync(Eq("owner", "Bob"));
```

```rust tab=Rust
use storine::{add, dec, doc, eq, gt, Query};

fn main() -> Result<(), storine::Error> {
    let db = storine::connect("storine://127.0.0.1:7878")?;
    let accounts = db.collection("accounts");

    accounts.insert("alice", doc! { "owner" => "Alice", "balance" => dec("100.00") })?;
    accounts.insert("bob", doc! { "owner" => "Bob", "balance" => dec("0.00") })?;

    // Atomic, exact, server-side: no lost updates, no overdraft.
    accounts.update("alice", [add("balance", dec("-25.00")).min(0)])?;

    // Several writes, all or nothing.
    db.transaction(|tx| {
        tx.update("accounts", "alice", [add("balance", dec("-10.00")).min(0)]);
        tx.update("accounts", "bob", [add("balance", dec("10.00"))]);
        Ok(())
    })?;

    let doc = accounts.get("alice")?.expect("exists");
    println!("{:?} {}", doc.get("balance"), doc.version);

    accounts.create_index("owner")?;
    let rich = accounts.find(Query::new().filter(gt("balance", dec("50"))).order_by("-balance").limit(10))?;
    let bobs = accounts.count(eq("owner", "Bob"))?;
    println!("{} {}", rich.len(), bobs);
    Ok(())
}
```

```php tab=PHP
<?php
use Storine\{Decimal, FieldOp, Filter, Storine, Transaction};

require 'storine/sdks/php/src/autoload.php';

$db = Storine::connect('storine://127.0.0.1:7878');
$accounts = $db->collection('accounts');

$accounts->insert('alice', ['owner' => 'Alice', 'balance' => Decimal::of('100.00')]);
$accounts->insert('bob', ['owner' => 'Bob', 'balance' => Decimal::of('0.00')]);

// Atomic, exact, server-side: no lost updates, no overdraft.
$accounts->update('alice', FieldOp::add('balance', Decimal::of('-25.00'))->min(0));

// Several writes, all or nothing.
$db->transaction(function (Transaction $tx) {
    $tx->update('accounts', 'alice', FieldOp::add('balance', Decimal::of('-10.00'))->min(0));
    $tx->update('accounts', 'bob', FieldOp::add('balance', Decimal::of('10.00')));
});

$doc = $accounts->get('alice');
echo $doc['balance'], ' ', $doc->version;   // 65.00 and the version

$accounts->createIndex('owner');
$rich = $accounts->find(Filter::gt('balance', Decimal::of('50')), orderBy: '-balance', limit: 10);
echo $accounts->count(Filter::eq('owner', 'Bob'));
```

```ruby tab=Ruby
require "storine"

D = Storine.method(:Decimal)

db = Storine.connect("storine://127.0.0.1:7878")
accounts = db.collection("accounts")

accounts.insert("alice", { "owner" => "Alice", "balance" => D.("100.00") })
accounts.insert("bob", { "owner" => "Bob", "balance" => D.("0.00") })

# Atomic, exact, server-side: no lost updates, no overdraft.
accounts.update("alice", Storine.add("balance", D.("-25.00"), min: 0))

# Several writes, all or nothing.
db.transaction do |tx|
  tx.update("accounts", "alice", Storine.add("balance", D.("-10.00"), min: 0))
  tx.update("accounts", "bob", Storine.add("balance", D.("10.00")))
end

doc = accounts.get("alice")
puts doc["balance"], doc.version         # 65.00 and the version

accounts.create_index("owner")
rich = accounts.find(Storine.gt("balance", D.("50")), order_by: "-balance", limit: 10)
puts accounts.count(Storine.eq("owner", "Bob"))
```

Run it twice and the second run fails on the first `insert`, because `insert` only creates. Use `put` to create or replace.

Now try an overdraft: change `-25.00` to `-500.00`. The update fails with an invalid-request error, and Alice's balance is unchanged.

## 5. See it in the console

Reload the console. The `accounts` collection is there with both documents, the `owner` index, and each document's version. Open `alice` to edit it: saving checks the version you opened, so you never overwrite someone else's change.

## Next steps

- [Core concepts](concepts.md): what a document, a version and a request id are.
- [Documents and writes](documents.md): every kind of write, preconditions, transactions and TTL.
- [Queries](queries.md), [indexes](indexes.md) and [analytics](analytics.md).
- Your client's own page under [Clients](clients/index.md) lists its whole API.
- Going to production? [Security](security.md), [clusters](clusters.md) and [backups](backups.md).

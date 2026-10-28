// Package storine is the Go client of the Storine database. Standard library only.
//
//	db, _ := storine.Connect("storine://127.0.0.1:7878") // or "storine://token@h1:7878,h2:7878,h3:7878"
//	accounts := db.Collection("accounts")
//	accounts.Insert(ctx, "alice", storine.Doc{"balance": storine.MustDecimal("100.00")})
//	// Atomic, exact, server-side: no lost updates, no overdraft.
//	accounts.Update(ctx, "alice", storine.Add("balance", storine.MustDecimal("-25.00")).Min(0))
//
// Every write carries a request id that the client reuses on its own retries, so timeouts,
// reconnects and cluster leader changes never apply a write twice. Errors are *Error values;
// test them with errors.Is(err, storine.ErrConflict) and friends.
package storine

"""Storine Python client.

    import storine
    from decimal import Decimal

    db = storine.connect("storine://127.0.0.1:7878")
    accounts = db.collection("accounts")
    accounts.insert("alice", {"balance": Decimal("100.00")})
    accounts.update("alice", storine.add("balance", Decimal("-25.00"), min=0))   # no overdraft, no lost update
    print(accounts.get("alice")["balance"])                                     # Decimal('75.00')
"""

from ._analytics import (Aggregate, AggregateResult, ColumnRow, Expr, GroupKey, approx_count_distinct, avg, bucket,
                         count, count_distinct, field, lit, max, min, sum)
from ._sve import Id, Timestamp, UInt64
from ._wire import (Filter, FieldOp, add, and_, append, between, contains, eq, exists, ge, gt, in_, key_prefix, le, lt,
                    missing, ne, not_, or_, prefix, set_, unset)
from .client import ByteRange, Client, Collection, ColumnScan, Document, Transaction, connect
from .errors import (ConflictError, InvalidError, NotFoundError, OverloadedError, RateLimitedError, ServerError,
                     StorineError, UnauthorizedError, UnavailableError)

__version__ = "0.1.0"

__all__ = [
    "connect", "Client", "Collection", "Document", "ByteRange", "Transaction",
    "set_", "unset", "add", "append", "FieldOp",
    "eq", "ne", "lt", "le", "gt", "ge", "between", "in_", "prefix", "contains", "exists", "missing", "key_prefix",
    "and_", "or_", "not_", "Filter",
    "Id", "Timestamp", "UInt64",
    # Analytics. `sum`, `min` and `max` are left out of __all__ so `from storine import *` does
    # not shadow the builtins: use storine.sum / storine.min / storine.max.
    "count", "avg", "count_distinct", "approx_count_distinct", "field", "lit", "bucket",
    "Aggregate", "AggregateResult", "ColumnRow", "ColumnScan", "Expr", "GroupKey",
    "StorineError", "NotFoundError", "ConflictError", "InvalidError", "UnauthorizedError", "OverloadedError",
    "RateLimitedError", "UnavailableError", "ServerError",
]

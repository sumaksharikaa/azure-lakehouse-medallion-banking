"""
Synthetic retail-banking data generator.

Produces daily landing-zone batches in three formats, the way upstream
systems typically deliver them:

    landing/customers/batch_date=YYYY-MM-DD/customers.csv       (CDC feed: I/U/D)
    landing/accounts/batch_date=YYYY-MM-DD/accounts.json         (JSON lines, full-row upserts)
    landing/transactions/batch_date=YYYY-MM-DD/transactions.parquet

Every batch after the first contains realistic problems so the pipeline has
something to do: duplicate transactions, late-arriving records, orphan account
ids, invalid currencies, non-positive amounts, malformed emails and
customer attribute changes that drive SCD Type 2 history.

All data is fake. No real customer data is used anywhere in this project.

Usage:
    python data_generator/generate_banking_data.py --out data/landing \
        --start-date 2026-11-01 --days 3 --seed 42
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

FIRST_NAMES = [
    "James",
    "Mary",
    "Robert",
    "Patricia",
    "John",
    "Jennifer",
    "Michael",
    "Linda",
    "David",
    "Elizabeth",
    "William",
    "Barbara",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Priya",
    "Arjun",
    "Wei",
    "Mei",
    "Carlos",
    "Sofia",
    "Ahmed",
    "Fatima",
]
LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Patel",
    "Reddy",
    "Chen",
    "Wang",
    "Nguyen",
    "Kim",
    "Khan",
]
CITIES = [
    ("Atlanta", "GA"),
    ("Charlotte", "NC"),
    ("Austin", "TX"),
    ("Denver", "CO"),
    ("Seattle", "WA"),
    ("Chicago", "IL"),
    ("Boston", "MA"),
    ("Phoenix", "AZ"),
    ("Miami", "FL"),
    ("Columbus", "OH"),
]
SEGMENTS = ["MASS", "MASS", "MASS", "AFFLUENT", "AFFLUENT", "PRIVATE"]
ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "CREDIT_CARD", "LOAN"]
CHANNELS = ["POS", "ONLINE", "ATM", "MOBILE", "BRANCH", "ACH", "WIRE"]
MERCHANT_CATEGORIES = [
    "GROCERY",
    "FUEL",
    "RESTAURANT",
    "TRAVEL",
    "UTILITIES",
    "ENTERTAINMENT",
    "HEALTHCARE",
    "RETAIL",
    "TRANSFER",
    "PAYROLL",
]
CURRENCIES = ["USD", "USD", "USD", "USD", "EUR", "GBP", "CAD"]


@dataclass
class Customer:
    customer_id: str
    first_name: str
    last_name: str
    email: str
    phone: str
    city: str
    state: str
    segment: str
    kyc_status: str
    updated_at: str


@dataclass
class Account:
    account_id: str
    customer_id: str
    account_type: str
    currency: str
    status: str
    opened_date: str
    opening_balance: float
    credit_limit: float | None
    updated_at: str


def _ts(day: date, rng: random.Random) -> str:
    moment = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86_399))
    return moment.strftime("%Y-%m-%d %H:%M:%S")


class BankSimulator:
    def __init__(self, seed: int, n_customers: int, txns_per_day: int) -> None:
        self.rng = random.Random(seed)
        self.n_customers = n_customers
        self.txns_per_day = txns_per_day
        self.customers: dict[str, Customer] = {}
        self.accounts: dict[str, Account] = {}
        self._cust_seq = 100_000
        self._acct_seq = 5_000_000
        self.previous_day_txns: list[dict] = []

    # ------------------------------------------------------------------ customers
    def _new_customer(self, updated_at: str) -> Customer:
        self._cust_seq += 1
        first, last = self.rng.choice(FIRST_NAMES), self.rng.choice(LAST_NAMES)
        city, state = self.rng.choice(CITIES)
        return Customer(
            customer_id=f"C{self._cust_seq}",
            first_name=first,
            last_name=last,
            email=f"{first}.{last}{self.rng.randint(1, 999)}@example.com".lower(),
            phone=f"+1-555-{self.rng.randint(100, 999)}-{self.rng.randint(1000, 9999)}",
            city=city,
            state=state,
            segment=self.rng.choice(SEGMENTS),
            kyc_status="VERIFIED",
            updated_at=updated_at,
        )

    def customer_batch(self, day: date, is_first: bool) -> list[dict]:
        rows: list[dict] = []
        if is_first:
            # Initial load: history starts well before the first transaction day.
            for _ in range(self.n_customers):
                c = self._new_customer("2026-01-01 00:00:00")
                self.customers[c.customer_id] = c
                rows.append({"op": "I", **asdict(c)})
            return rows

        # New customers (onboarded at start of day, before their accounts transact)
        for _ in range(max(1, self.n_customers // 20)):
            c = self._new_customer(f"{day.isoformat()} 00:00:00")
            self.customers[c.customer_id] = c
            rows.append({"op": "I", **asdict(c)})

        active = [c for c in self.customers.values() if c.kyc_status != "CLOSED"]
        # Attribute changes -> SCD2 versions (relocation, segment upgrade)
        for c in self.rng.sample(active, k=max(1, len(active) // 12)):
            if self.rng.random() < 0.5:
                c.city, c.state = self.rng.choice(CITIES)
            else:
                c.segment = self.rng.choice(SEGMENTS)
            c.updated_at = _ts(day, self.rng)
            rows.append({"op": "U", **asdict(c)})

        # Account closures / customer off-boarding (CDC delete)
        for c in self.rng.sample(active, k=2):
            c.kyc_status = "CLOSED"
            c.updated_at = _ts(day, self.rng)
            rows.append({"op": "D", **asdict(c)})

        # Data-quality noise
        bad = self._new_customer(_ts(day, self.rng))
        bad.email = "not-an-email"
        rows.append({"op": "I", **asdict(bad)})
        rows.append({"op": "U", **{**asdict(bad), "customer_id": ""}})  # missing key
        return rows

    # ------------------------------------------------------------------ accounts
    def account_batch(self, day: date, new_customer_ids: list[str]) -> list[dict]:
        rows: list[dict] = []
        for cid in new_customer_ids:
            for _ in range(self.rng.choice([1, 1, 2, 3])):
                self._acct_seq += 1
                acct_type = self.rng.choice(ACCOUNT_TYPES)
                a = Account(
                    account_id=f"A{self._acct_seq}",
                    customer_id=cid,
                    account_type=acct_type,
                    currency="USD",
                    status="ACTIVE",
                    opened_date=(day - timedelta(days=self.rng.randint(0, 2000))).isoformat(),
                    opening_balance=round(self.rng.uniform(100, 25_000), 2),
                    credit_limit=(
                        float(self.rng.choice([2_000, 5_000, 10_000, 20_000]))
                        if acct_type == "CREDIT_CARD"
                        else None
                    ),
                    updated_at=_ts(day, self.rng),
                )
                self.accounts[a.account_id] = a
                rows.append(asdict(a))

        # Status / limit changes on existing accounts (SCD1 upserts)
        existing = [a for a in self.accounts.values() if a.customer_id not in new_customer_ids]
        for a in self.rng.sample(existing, k=min(len(existing), 10)):
            if a.account_type == "CREDIT_CARD" and self.rng.random() < 0.6:
                a.credit_limit = float((a.credit_limit or 0) + 1_000)
            else:
                a.status = self.rng.choice(["ACTIVE", "DORMANT", "FROZEN"])
            a.updated_at = _ts(day, self.rng)
            rows.append(asdict(a))
        return rows

    # -------------------------------------------------------------- transactions
    def transaction_batch(self, day: date) -> list[dict]:
        accounts = [a for a in self.accounts.values() if a.status == "ACTIVE"]
        rows: list[dict] = []
        for _ in range(self.txns_per_day):
            a = self.rng.choice(accounts)
            category = self.rng.choice(MERCHANT_CATEGORIES)
            amount = round(self.rng.lognormvariate(3.6, 1.1), 2)
            if category == "PAYROLL":
                txn_type, amount = "CREDIT", round(self.rng.uniform(1_500, 6_000), 2)
            elif category == "TRANSFER":
                txn_type = self.rng.choice(["CREDIT", "DEBIT"])
            else:
                txn_type = "DEBIT"
            rows.append(
                {
                    "txn_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
                    "account_id": a.account_id,
                    "txn_ts": _ts(day, self.rng),
                    "amount": amount,
                    "currency": self.rng.choice(CURRENCIES),
                    "txn_type": txn_type,
                    "channel": self.rng.choice(CHANNELS),
                    "merchant_category": category,
                    "description": f"{category.title()} purchase"
                    if txn_type == "DEBIT"
                    else "Incoming funds",
                }
            )

        if self.previous_day_txns:
            n = max(1, self.txns_per_day // 100)
            # Exact duplicates re-sent by the source (should be de-duplicated)
            rows.extend(self.rng.sample(rows, k=n))
            # Late-arriving: yesterday's records re-delivered today (idempotent MERGE)
            rows.extend(self.rng.sample(self.previous_day_txns, k=n))
            # Orphans, bad currency, non-positive amounts, missing key
            template = dict(self.rng.choice(rows))
            rows.append({**template, "txn_id": str(uuid.uuid4()), "account_id": "A0000000"})
            rows.append({**template, "txn_id": str(uuid.uuid4()), "currency": "XXX"})
            rows.append({**template, "txn_id": str(uuid.uuid4()), "amount": -45.10})
            rows.append({**template, "txn_id": None})

        self.previous_day_txns = [r for r in rows if r.get("txn_id")]
        return rows


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
        fh.write('{"account_id": "A_BROKEN", "customer_id": \n')  # one corrupt record


def _write_parquet(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def generate(out: Path, start: date, days: int, seed: int, customers: int, txns: int) -> None:
    sim = BankSimulator(seed=seed, n_customers=customers, txns_per_day=txns)
    for i in range(days):
        day = start + timedelta(days=i)
        part = f"batch_date={day.isoformat()}"
        cust_rows = sim.customer_batch(day, is_first=(i == 0))
        new_ids = [
            r["customer_id"] for r in cust_rows if r["op"] == "I" and r["customer_id"] in sim.customers
        ]
        acct_rows = sim.account_batch(day, new_ids)
        txn_rows = sim.transaction_batch(day)

        _write_csv(out / "customers" / part / "customers.csv", cust_rows)
        _write_jsonl(out / "accounts" / part / "accounts.json", acct_rows)
        _write_parquet(out / "transactions" / part / "transactions.parquet", txn_rows)
        print(
            f"{day}: customers={len(cust_rows):>5}  accounts={len(acct_rows):>5}  "
            f"transactions={len(txn_rows):>6}"
        )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=Path("data/landing"))
    p.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 11, 1))
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--customers", type=int, default=500)
    p.add_argument("--txns-per-day", type=int, default=5_000)
    a = p.parse_args()
    generate(a.out, a.start_date, a.days, a.seed, a.customers, a.txns_per_day)


if __name__ == "__main__":
    main()

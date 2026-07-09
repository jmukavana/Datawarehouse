"""Simulation engine.

Holds all mutable simulation state in a JSON-serializable dict so a run can be
resumed across invocations (each Dagster run generates the next day's batch).
All dates are ISO strings inside the state; `simulate_day` returns, per source,
the rows that "arrive" in that day's extract files.
"""

import random
from collections import defaultdict
from datetime import date, timedelta

from faker import Faker

from . import config


def _iso(d: date) -> str:
    return d.isoformat()


def _d(s: str) -> date:
    return date.fromisoformat(s)


def new_state(seed: int, start_date: date) -> dict:
    return {
        "seed": seed,
        "start_date": _iso(start_date),
        "last_date": None,
        "drift_cutoff": _iso(start_date + timedelta(days=config.SCHEMA_DRIFT_AFTER_DAYS)),
        "seq": {"customer": 0, "policy": 0, "claim": 0},
        "customers": {},
        "agents": {},
        "policies": {},
        "pending_payments": [],      # payment records waiting for their file_date
        "pending_claim_events": [],  # claim events waiting for their arrival_date
        "bootstrapped": False,
    }


class Engine:
    def __init__(self, state: dict):
        self.state = state
        self.fake = Faker()

    def _rng_for(self, d: date) -> random.Random:
        rng = random.Random(f"{self.state['seed']}:{d.isoformat()}")
        self.fake.seed_instance(rng.getrandbits(32))
        return rng

    # ------------------------------------------------------------------ #
    # Entity factories
    # ------------------------------------------------------------------ #

    def _next(self, kind: str) -> int:
        self.state["seq"][kind] += 1
        return self.state["seq"][kind]

    def _make_customer(self, rng: random.Random, created: date) -> dict:
        number = f"CUST{self._next('customer'):06d}"
        customer = {
            "customer_number": number,
            "national_id": None if rng.random() < 0.03 else f"{rng.randint(10_000_000, 39_999_999)}",
            "full_name": self.fake.name(),
            "phone_number": f"2547{rng.randint(10_000_000, 99_999_999)}",
            "date_of_birth": _iso(date(rng.randint(1958, 2002), rng.randint(1, 12), rng.randint(1, 28))),
            "county": rng.choice(config.CUSTOMER_COUNTIES),
            "segment": rng.choices(config.SEGMENTS, weights=[85, 15])[0],
            "risk_tier": rng.choices(config.RISK_TIERS, weights=[10, 80, 10])[0],
            "kyc_status": rng.choices(["VERIFIED", "PENDING"], weights=[92, 8])[0],
            "created_at": _iso(created),
            "updated_at": _iso(created),
        }
        self.state["customers"][number] = customer
        return customer

    def _make_agent(self, rng: random.Random, idx: int, hired: date) -> dict:
        code = f"AGT{idx:04d}"
        agent = {
            "agent_code": code,
            "full_name": self.fake.name(),
            "license_number": f"IRA/{rng.randint(2010, 2025)}/{rng.randint(10_000, 99_999)}",
            "branch_code": rng.choice(config.BRANCHES)[0],
            "hire_date": _iso(hired),
            "updated_at": _iso(hired),
        }
        self.state["agents"][code] = agent
        return agent

    def _make_policy(self, rng: random.Random, customer_number: str, start: date) -> tuple[dict, list[dict]]:
        code, name, lob, underwriter, commission, (lo, hi) = rng.choice(config.PRODUCTS)
        frequency = rng.choices(list(config.PAYMENT_FREQUENCIES), weights=[60, 20, 20])[0]
        n_installments = config.PAYMENT_FREQUENCIES[frequency]
        annual_premium = rng.randint(lo, hi) // 100 * 100
        agent = self.state["agents"][rng.choice(list(self.state["agents"]))]
        number = f"POL{self._next('policy'):08d}"

        installments = []
        step = 12 // n_installments
        for i in range(n_installments):
            due = _add_months(start, i * step)
            installments.append({
                "installment_no": i + 1,
                "due_date": _iso(due),
                "amount_due": annual_premium // n_installments,
                "status": "DUE",
            })

        policy = {
            "policy_number": number,
            "customer_number": customer_number,
            "product_code": code,
            "agent_code": agent["agent_code"],
            "branch_code": agent["branch_code"],  # branch at time of sale
            "status": "ACTIVE",
            "start_date": _iso(start),
            "end_date": _iso(_add_months(start, 12)),
            "sum_insured": annual_premium * rng.randint(8, 25),
            "annual_premium": annual_premium,
            "payment_frequency": frequency,
            "created_at": _iso(start),
            "updated_at": _iso(start),
            "_installments": installments,
            "_consecutive_missed": 0,
        }
        self.state["policies"][number] = policy
        return policy, installments

    @staticmethod
    def _public_policy(policy: dict) -> dict:
        return {k: v for k, v in policy.items() if not k.startswith("_")}

    @staticmethod
    def _schedule_rows(policy: dict) -> list[dict]:
        return [
            {
                "policy_number": policy["policy_number"],
                "installment_no": inst["installment_no"],
                "due_date": inst["due_date"],
                "amount_due": inst["amount_due"],
                "created_at": policy["created_at"],
            }
            for inst in policy["_installments"]
        ]

    # ------------------------------------------------------------------ #
    # Bootstrap: the day-1 "initial full extract"
    # ------------------------------------------------------------------ #

    def bootstrap(self, out: dict) -> None:
        start = _d(self.state["start_date"])
        rng = self._rng_for(start)

        out["core_insurance"]["branches"] = [
            {"branch_code": c, "branch_name": n, "county": county, "region": r, "tier": t}
            for c, n, county, r, t in config.BRANCHES
        ]
        out["core_insurance"]["products"] = [
            {
                "product_code": c, "product_name": n, "line_of_business": lob,
                "underwriter": u, "commission_rate": rate,
            }
            for c, n, lob, u, rate, _ in config.PRODUCTS
        ]

        for i in range(1, config.N_AGENTS + 1):
            hired = start - timedelta(days=rng.randint(90, 2500))
            self._make_agent(rng, i, hired)
        out["core_insurance"]["agents"] = list(self.state["agents"].values())

        for _ in range(config.N_INITIAL_CUSTOMERS):
            created = start - timedelta(days=rng.randint(30, 365))
            customer = self._make_customer(rng, created)
            for _ in range(rng.choices([1, 2], weights=[70, 30])[0]):
                policy_start = _d(customer["created_at"]) + timedelta(days=rng.randint(0, 20))
                policy, _insts = self._make_policy(rng, customer["customer_number"], policy_start)
                # Installments due before the simulation window were settled
                # off-camera; only in-window installments produce payment records.
                for inst in policy["_installments"]:
                    if _d(inst["due_date"]) < start:
                        inst["status"] = "HISTORIC"

        out["core_insurance"]["customers"] = list(self.state["customers"].values())
        out["core_insurance"]["policies"] = [
            self._public_policy(p) for p in self.state["policies"].values()
        ]
        out["core_insurance"]["premium_schedules"] = [
            row for p in self.state["policies"].values() for row in self._schedule_rows(p)
        ]
        self.state["bootstrapped"] = True

    # ------------------------------------------------------------------ #
    # Daily simulation
    # ------------------------------------------------------------------ #

    def simulate_day(self, day: date) -> dict:
        out = {
            "core_insurance": defaultdict(list),
            "mpesa_gateway": defaultdict(list),
            "bank_payments": defaultdict(list),
        }
        rng = self._rng_for(day)

        if not self.state["bootstrapped"]:
            self.bootstrap(out)

        self._release_pending(day, out)
        self._new_customers_and_policies(day, rng, out)
        self._rerate_customers(day, rng, out)
        self._transfer_agents(day, rng, out)
        self._cancel_policies(day, rng, out)
        self._process_due_installments(day, rng, out)
        self._raise_claims(day, rng, out)

        self.state["last_date"] = _iso(day)
        return {source: dict(tables) for source, tables in out.items()}

    def _release_pending(self, day: date, out: dict) -> None:
        iso = _iso(day)
        keep = []
        for item in self.state["pending_payments"]:
            if item["file_date"] == iso:
                record = {k: v for k, v in item["record"].items()}
                out[item["source"]]["transactions" if item["source"] == "mpesa_gateway" else "payments"].append(record)
            else:
                keep.append(item)
        self.state["pending_payments"] = keep

        keep = []
        for item in self.state["pending_claim_events"]:
            if item["arrival_date"] == iso:
                out["core_insurance"]["claims"].append(item["record"])
            else:
                keep.append(item)
        self.state["pending_claim_events"] = keep

    def _new_customers_and_policies(self, day: date, rng: random.Random, out: dict) -> None:
        n_new = _poisson(rng, config.NEW_CUSTOMERS_PER_DAY)
        buyers: list[str] = []
        for _ in range(n_new):
            customer = self._make_customer(rng, day)
            out["core_insurance"]["customers"].append(dict(customer))
            buyers.append(customer["customer_number"])
        for number, customer in self.state["customers"].items():
            if number not in buyers and rng.random() < config.P_EXTRA_POLICY_EXISTING:
                buyers.append(number)
        for number in buyers:
            for _ in range(rng.choices([1, 2], weights=[85, 15])[0]):
                policy, _insts = self._make_policy(rng, number, day)
                out["core_insurance"]["policies"].append(self._public_policy(policy))
                out["core_insurance"]["premium_schedules"].extend(self._schedule_rows(policy))

    def _rerate_customers(self, day: date, rng: random.Random, out: dict) -> None:
        for customer in self.state["customers"].values():
            if _d(customer["created_at"]) < day and rng.random() < config.P_RISK_RERATE:
                other_tiers = [t for t in config.RISK_TIERS if t != customer["risk_tier"]]
                customer["risk_tier"] = rng.choice(other_tiers)
                customer["updated_at"] = _iso(day)
                out["core_insurance"]["customers"].append(dict(customer))

    def _transfer_agents(self, day: date, rng: random.Random, out: dict) -> None:
        for agent in self.state["agents"].values():
            if rng.random() < config.P_AGENT_TRANSFER:
                other = [b[0] for b in config.BRANCHES if b[0] != agent["branch_code"]]
                agent["branch_code"] = rng.choice(other)
                agent["updated_at"] = _iso(day)
                out["core_insurance"]["agents"].append(dict(agent))

    def _cancel_policies(self, day: date, rng: random.Random, out: dict) -> None:
        for policy in self.state["policies"].values():
            if policy["status"] == "ACTIVE" and rng.random() < config.P_POLICY_CANCEL:
                policy["status"] = "CANCELLED"
                policy["updated_at"] = _iso(day)
                out["core_insurance"]["policies"].append(self._public_policy(policy))

    def _process_due_installments(self, day: date, rng: random.Random, out: dict) -> None:
        iso = _iso(day)
        for policy in self.state["policies"].values():
            if policy["status"] != "ACTIVE":
                continue
            for inst in policy["_installments"]:
                if inst["due_date"] != iso or inst["status"] != "DUE":
                    continue
                if rng.random() < config.P_MISSED_PAYMENT:
                    inst["status"] = "MISSED"
                    policy["_consecutive_missed"] += 1
                    if policy["_consecutive_missed"] >= config.MISSED_BEFORE_LAPSE:
                        policy["status"] = "LAPSED"
                        policy["updated_at"] = iso
                        out["core_insurance"]["policies"].append(self._public_policy(policy))
                    continue
                policy["_consecutive_missed"] = 0
                inst["status"] = "PAID"
                self._schedule_payment(day, rng, policy, inst)

    def _schedule_payment(self, due: date, rng: random.Random, policy: dict, inst: dict) -> None:
        channel, source, _ = rng.choices(config.CHANNELS, weights=[c[2] for c in config.CHANNELS])[0]
        lag_lo, lag_hi = config.CHANNEL_LAG[channel]
        tx_date = due + timedelta(days=int(rng.triangular(lag_lo, lag_hi, lag_lo)))
        amount = inst["amount_due"]
        if rng.random() < config.P_PARTIAL_PAYMENT:
            amount = int(amount * rng.uniform(0.3, 0.9))
        customer = self.state["customers"][policy["customer_number"]]
        billing_ref = f"{policy['policy_number']}/{inst['installment_no']}"

        if source == "mpesa_gateway":
            record = {
                "receipt_number": _mpesa_code(rng),
                "msisdn": customer["phone_number"],
                "paybill_number": "522100",
                "account_reference": billing_ref,
                "amount": float(amount),
                # Deliberate mess: naive Africa/Nairobi local time, no offset.
                "transaction_time": f"{_iso(tx_date)} {rng.randint(6, 21):02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}",
            }
            if _iso(tx_date) >= self.state["drift_cutoff"]:
                record["paybill_ref"] = f"PB{rng.randint(100_000, 999_999)}"
            self.state["pending_payments"].append(
                {"source": source, "file_date": _iso(tx_date), "record": record}
            )
            if rng.random() < config.P_MPESA_DUPLICATE:
                dup_date = tx_date if rng.random() < 0.6 else tx_date + timedelta(days=1)
                self.state["pending_payments"].append(
                    {"source": source, "file_date": _iso(dup_date), "record": dict(record)}
                )
        else:
            # Deliberate mess: amounts as formatted strings; cheques (and some
            # transfers) use D/M/Y value dates while the rest are ISO.
            use_dmy = channel == "CHEQUE" or rng.random() < 0.3
            record = {
                "bank_reference": f"FT{rng.randint(10_000_000, 99_999_999)}",
                "payer_name": customer["full_name"],
                "billing_reference": billing_ref,
                "amount": f"{amount:,}.00",
                "value_date": tx_date.strftime("%d/%m/%Y") if use_dmy else _iso(tx_date),
                "channel": channel,
            }
            self.state["pending_payments"].append(
                {"source": source, "file_date": _iso(tx_date), "record": record}
            )

    def _raise_claims(self, day: date, rng: random.Random, out: dict) -> None:
        for policy in list(self.state["policies"].values()):
            if policy["status"] != "ACTIVE" or rng.random() >= config.P_CLAIM:
                continue
            claim_number = f"CLM{self._next('claim'):07d}"
            incident = max(_d(policy["start_date"]), day - timedelta(days=rng.randint(0, 10)))
            claimed = rng.randint(policy["annual_premium"], max(policy["annual_premium"] + 1, policy["sum_insured"] // 2))
            assessed = int(claimed * rng.uniform(0.5, 1.0))
            approved = rng.random() < 0.75

            events = [("REGISTERED", day, {"claimed_amount": claimed})]
            cursor = day + timedelta(days=rng.randint(3, 10))
            events.append(("ASSESSED", cursor, {"assessed_amount": assessed}))
            cursor += timedelta(days=rng.randint(2, 7))
            if approved:
                events.append(("APPROVED", cursor, {}))
                cursor += timedelta(days=rng.randint(3, 14))
                events.append(("PAID", cursor, {"paid_amount": assessed}))
            else:
                events.append(("REJECTED", cursor, {}))
            cursor += timedelta(days=rng.randint(1, 3))
            events.append(("CLOSED", cursor, {}))

            for seq, (status, event_date, measures) in enumerate(events, start=1):
                record = {
                    "claim_number": claim_number,
                    "policy_number": policy["policy_number"],
                    "customer_number": policy["customer_number"],
                    "branch_code": policy["branch_code"],
                    "event_sequence": seq,
                    "event_status": status,
                    "event_date": _iso(event_date),
                    "incident_date": _iso(incident),
                    "claimed_amount": measures.get("claimed_amount"),
                    "assessed_amount": measures.get("assessed_amount"),
                    "paid_amount": measures.get("paid_amount"),
                }
                # Deliberate mess: events land in extracts up to 14 days late.
                lateness = int(rng.triangular(0, config.MAX_CLAIM_EVENT_LATENESS, 0))
                self.state["pending_claim_events"].append(
                    {"arrival_date": _iso(event_date + timedelta(days=lateness)), "record": record}
                )


def _add_months(d: date, months: int) -> date:
    month_index = d.month - 1 + months
    year = d.year + month_index // 12
    month = month_index % 12 + 1
    day = min(d.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
                      31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
    return date(year, month, day)


def _poisson(rng: random.Random, mean: float) -> int:
    # Knuth's algorithm; fine for small means.
    import math
    limit = math.exp(-mean)
    k, p = 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def _mpesa_code(rng: random.Random) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ123456789"
    return "Q" + "".join(rng.choice(alphabet) for _ in range(9))

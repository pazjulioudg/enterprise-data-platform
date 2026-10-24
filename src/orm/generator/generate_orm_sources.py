"""
Synthetic Operational Risk Management (ORM) source generator.

Simulates the three source systems a bank's ORM data platform typically ingests:

  1. GRC platform (OpenPages / ServiceNow IRM / Archer style) - daily JSON extracts of changed
     records: org units, risks, controls, risk-control links, RCSAs, control tests, KRIs,
     KRI observations, issues, loss events, loss financial movements, event attributes.
     The GRC uses its OWN codes (e.g. event type 'ET2.2'), mapped to canonical codes by the
     crosswalk in Silver.
  2. Legacy loss log of an acquired building society (LE04) - CSV, UK date format, its own
     categories and department names, single 'Loss GBP' / 'Recovered GBP' columns.
  3. General ledger - CSV postings to operational-loss accounts, used for reconciliation.

Reference data (taxonomies, crosswalk, legal entities, daily FX) is written to
<catalog>.silver.ref_* on batch 0.

Batch 0 = full history as at 2026-09-30. Batch 1 = incremental extract as at 2026-10-31 with
real-world changes (new events, reclassifications, late-booked provisions, issue closures)
and injected data problems (date-sequence violations, unmapped codes, duplicate extracts,
unmapped legacy category, GL breaks).

Output: <root>/{grc,legacy,gl}/<entity>/batch_<NNNN>/  (root defaults to the landing volume)
"""
import argparse
import csv
import datetime as dt
import json
import math
import os
import random
import sys

D = dt.date
AS_AT = {0: D(2026, 9, 30), 1: D(2026, 10, 31)}

# ----------------------------------------------------------------------------- reference data
EVENT_TYPES = [  # canonical L2 code, L1 code, L1 name, L2 name, GRC code, weight
    ("IF_UA", "IF", "Internal fraud", "Unauthorised activity", "ET1.1", 1),
    ("IF_TF", "IF", "Internal fraud", "Theft and fraud", "ET1.2", 3),
    ("EF_TF", "EF", "External fraud", "Theft and fraud", "ET2.1", 17),
    ("EF_SS", "EF", "External fraud", "Systems security", "ET2.2", 8),
    ("EPWS_ER", "EPWS", "Employment practices and workplace safety", "Employee relations", "ET3.1", 4),
    ("EPWS_SE", "EPWS", "Employment practices and workplace safety", "Safe environment", "ET3.2", 2),
    ("EPWS_DD", "EPWS", "Employment practices and workplace safety", "Diversity and discrimination", "ET3.3", 1),
    ("CPBP_SDF", "CPBP", "Clients, products and business practices", "Suitability, disclosure and fiduciary", "ET4.1", 6),
    ("CPBP_IBMP", "CPBP", "Clients, products and business practices", "Improper business or market practices", "ET4.2", 4),
    ("CPBP_PF", "CPBP", "Clients, products and business practices", "Product flaws", "ET4.3", 2),
    ("CPBP_SSE", "CPBP", "Clients, products and business practices", "Selection, sponsorship and exposure", "ET4.4", 1),
    ("CPBP_AA", "CPBP", "Clients, products and business practices", "Advisory activities", "ET4.5", 2),
    ("DPA_DOE", "DPA", "Damage to physical assets", "Disasters and other events", "ET5.1", 6),
    ("BDSF_SYS", "BDSF", "Business disruption and system failures", "Systems", "ET6.1", 8),
    ("EDPM_TCEM", "EDPM", "Execution, delivery and process management", "Transaction capture, execution and maintenance", "ET7.1", 15),
    ("EDPM_MR", "EDPM", "Execution, delivery and process management", "Monitoring and reporting", "ET7.2", 4),
    ("EDPM_CID", "EDPM", "Execution, delivery and process management", "Customer intake and documentation", "ET7.3", 4),
    ("EDPM_CCAM", "EDPM", "Execution, delivery and process management", "Customer or client account management", "ET7.4", 5),
    ("EDPM_TC", "EDPM", "Execution, delivery and process management", "Trade counterparties", "ET7.5", 3),
    ("EDPM_VS", "EDPM", "Execution, delivery and process management", "Vendors and suppliers", "ET7.6", 4),
]
BUSINESS_LINES = [  # canonical L2 code, L1 code, L1 name, L2 name, GRC code
    ("CF_CF", "CF", "Corporate finance", "Corporate finance", "BL01"),
    ("TS_SALES", "TS", "Trading and sales", "Sales", "BL05"),
    ("TS_MM", "TS", "Trading and sales", "Market-making", "BL06"),
    ("TS_TREAS", "TS", "Trading and sales", "Treasury", "BL08"),
    ("RB_RB", "RB", "Retail banking", "Retail banking", "BL09"),
    ("RB_CARD", "RB", "Retail banking", "Card services", "BL11"),
    ("CB_CB", "CB", "Commercial banking", "Commercial banking", "BL12"),
    ("PS_EXT", "PS", "Payment and settlement", "External clients", "BL13"),
    ("AS_CUST", "AS", "Agency services", "Custody", "BL14"),
    ("AM_DISC", "AM", "Asset management", "Discretionary fund management", "BL17"),
    ("RBR_RBR", "RBR", "Retail brokerage", "Retail brokerage", "BL19"),
]
CAUSES = [  # canonical code, L1, L2 name, GRC code
    ("PEOPLE_ERR", "People", "Human error", "C-P1"), ("PEOPLE_MAL", "People", "Malicious act", "C-P2"),
    ("PROC_DESIGN", "Process", "Inadequate process design", "C-R1"), ("PROC_EXEC", "Process", "Process execution failure", "C-R2"),
    ("SYS_FAIL", "Systems", "System failure", "C-S1"), ("SYS_SEC", "Systems", "Security weakness", "C-S2"),
    ("EXT_EVENT", "External", "External event", "C-E1"), ("EXT_3RD", "External", "Third-party failure", "C-E2"),
]
EFFECTS = [("LEGAL", "Legal liability"), ("PENALTY", "Regulatory or compliance penalty"),
           ("RESTITUTION", "Restitution"), ("LOSS_OF_RECOURSE", "Loss of recourse"),
           ("WRITE_DOWN", "Write-down"), ("ASSET_LOSS", "Loss of or damage to assets")]
ATTRIBUTES = [("CYBER", "Cyber or ICT related"), ("THIRD_PARTY", "Third-party or outsourcing related"),
              ("CONDUCT", "Conduct related"), ("ESG", "ESG or greenwashing related"),
              ("LARGE_LOSS", "Large loss event")]
LEGAL_ENTITIES = [  # id, name, LEI (synthetic), country, currency
    ("LE01", "EDP Bank UK plc", "213800EDPBANKUK00001", "GB", "GBP"),
    ("LE02", "EDP Bank Europe DAC", "635400EDPBANKEU00002", "IE", "EUR"),
    ("LE03", "EDP Securities Inc", "549300EDPSECUS000003", "US", "USD"),
    ("LE04", "Northgate Building Society (acquired 2023)", "213800NORTHGATE00004", "GB", "GBP"),
]
ORG_UNITS = [  # id, legal entity, division, business unit, cost centre, business line
    ("OU101", "LE01", "Retail", "Retail Banking UK", "CC1010", "RB_RB"),
    ("OU102", "LE01", "Retail", "Cards UK", "CC1020", "RB_CARD"),
    ("OU103", "LE01", "Transaction Banking", "Payments UK", "CC1030", "PS_EXT"),
    ("OU104", "LE01", "Commercial", "Commercial Banking UK", "CC1040", "CB_CB"),
    ("OU201", "LE02", "Markets", "Markets Europe", "CC2010", "TS_MM"),
    ("OU202", "LE02", "Markets", "Treasury", "CC2020", "TS_TREAS"),
    ("OU203", "LE02", "Securities Services", "Custody", "CC2030", "AS_CUST"),
    ("OU204", "LE02", "Investment Banking", "Corporate Finance", "CC2040", "CF_CF"),
    ("OU301", "LE03", "Markets", "Equities Trading", "CC3010", "TS_SALES"),
    ("OU302", "LE03", "Wealth", "Asset Management", "CC3020", "AM_DISC"),
    ("OU303", "LE03", "Wealth", "Retail Brokerage", "CC3030", "RBR_RBR"),
    ("OU401", "LE04", "Retail", "Branch Network", "CC4010", "RB_RB"),
    ("OU402", "LE04", "Retail", "Mortgages", "CC4020", "RB_RB"),
    ("OU403", "LE04", "Retail", "Cards", "CC4030", "RB_CARD"),
    ("OU404", "LE04", "Finance", "Treasury", "CC4040", "TS_TREAS"),
]
LEGACY_CATEGORIES = {  # legacy category -> canonical event type
    "Fraud - Internal": "IF_TF", "Fraud - External": "EF_TF", "Card Fraud": "EF_TF",
    "Cyber": "EF_SS", "HR Dispute": "EPWS_ER", "Health & Safety": "EPWS_SE",
    "Complaint - Misselling": "CPBP_SDF", "Fine/Penalty": "CPBP_IBMP", "Premises Damage": "DPA_DOE",
    "IT Outage": "BDSF_SYS", "Processing Error": "EDPM_TCEM", "Payment Error": "EDPM_TCEM",
    "Supplier Failure": "EDPM_VS", "Account Error": "EDPM_CCAM",
}
LEGACY_DEPTS = {"Branches": ("OU401", "RB_RB"), "Mortgages": ("OU402", "RB_RB"),
                "Cards": ("OU403", "RB_CARD"), "Treasury": ("OU404", "TS_TREAS")}
GL_ACCOUNTS = {"GROSS_LOSS": "640100", "PROVISION": "640200", "PROVISION_RELEASE": "640200",
               "RECOVERY_INSURANCE": "640900", "RECOVERY_OTHER": "640910"}
FIRST = ["James", "Olivia", "Arjun", "Priya", "Liam", "Amelia", "Rahul", "Sophie", "Noah", "Grace"]
LAST = ["Smith", "Patel", "Jones", "Sharma", "Brown", "Taylor", "Reddy", "Evans", "Khan", "Wright"]
DESCRIPTIONS = {
    "IF": "Investigation into employee {name} for suspected misappropriation of customer funds (case {ref}).",
    "EF": "Customer {name} reported unauthorised transactions after account takeover (ref {ref}).",
    "EPWS": "Employment tribunal claim raised by former employee {name} (ref {ref}).",
    "CPBP": "Complaint upheld for customer {name}: product sold without suitability assessment (ref {ref}).",
    "DPA": "Flood damage at site reported by facilities manager {name} (ref {ref}).",
    "BDSF": "Payments platform outage; compensation paid to customers including {name} (incident {ref}).",
    "EDPM": "Booking error identified by operations analyst {name}; trade amended (ref {ref}).",
}


def crosswalk_rows():
    rows = []
    for e in EVENT_TYPES:
        rows.append(("GRC", "event_type", e[4], e[0]))
    for c, _l1, _n1, _n2, grc in BUSINESS_LINES:
        rows.append(("GRC", "business_line", grc, c))
    for c, _l1, _n2, grc in CAUSES:
        rows.append(("GRC", "cause", grc, c))
    for k, v in LEGACY_CATEGORIES.items():
        rows.append(("LEGACY_LOSSLOG", "event_type", k, v))
    for k, (ou, bl) in LEGACY_DEPTS.items():
        rows.append(("LEGACY_LOSSLOG", "org_unit", k, ou))
        rows.append(("LEGACY_LOSSLOG", "business_line", k, bl))
    return [(s, d, sc, cc, "2016-01-01", "9999-12-31") for s, d, sc, cc in rows]


def fx_rows():
    """Daily EUR rates 2016-2026 (random walk). rate_to_eur = EUR per 1 unit of currency."""
    r = random.Random(7)
    gbp, usd, out = 1.17, 0.90, []
    day = D(2016, 1, 1)
    while day <= D(2026, 12, 31):
        gbp = min(1.25, max(1.08, gbp * math.exp(r.gauss(0, 0.004))))
        usd = min(1.05, max(0.80, usd * math.exp(r.gauss(0, 0.004))))
        for ccy, rate in (("EUR", 1.0), ("GBP", gbp), ("USD", usd)):
            out.append((ccy, day.isoformat(), round(rate, 6), round(rate / gbp, 6)))
        day += dt.timedelta(days=1)
    return out


# ----------------------------------------------------------------------------- helpers
def ts(d: D, hour: int = 9) -> str:
    return dt.datetime(d.year, d.month, d.day, hour, 0, 0).isoformat() + "Z"


def write_jsonl(root, system, entity, batch, rows):
    path = os.path.join(root, system, entity, f"batch_{batch:04d}")
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "part-00000.json"), "w") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")
    print(f"wrote {len(rows):>6} {system}/{entity} batch {batch}")


def write_csv(root, system, entity, batch, header, rows):
    path = os.path.join(root, system, entity, f"batch_{batch:04d}")
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, f"{entity}.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"wrote {len(rows):>6} {system}/{entity} batch {batch}")


def weighted(r, items, weight_idx):
    total = sum(i[weight_idx] for i in items)
    x = r.uniform(0, total)
    for i in items:
        x -= i[weight_idx]
        if x <= 0:
            return i
    return items[-1]


def amount(r):
    """Heavy-tailed loss size in EUR-equivalent (lognormal: median ~8k, rare multi-million)."""
    return round(min(25_000_000, r.lognormvariate(math.log(8000), 1.9)), 2)


# ----------------------------------------------------------------------------- GRC master data
def grc_master(r, batch_date):
    lm = ts(batch_date)
    org = [dict(org_unit_id=o, legal_entity_id=le, division=dv, business_unit=bu, cost_centre=cc,
                business_line_code=next(b[4] for b in BUSINESS_LINES if b[0] == bl), last_modified_ts=lm)
           for o, le, dv, bu, cc, bl in ORG_UNITS]
    risks, controls, links, k = [], [], [], 0
    et_by_l1 = {}
    for e in EVENT_TYPES:
        et_by_l1.setdefault(e[1], []).append(e)
    for o, le, *_ in ORG_UNITS:
        for j in range(4):
            k += 1
            et = weighted(r, EVENT_TYPES, 5)
            rid = f"R{k:04d}"
            risks.append(dict(risk_id=rid, title=f"{et[3]} risk in {o}", risk_category_code=et[4],
                              org_unit_id=o, owner_emplid=f"E{r.randint(1, 20000):07d}",
                              status="ACTIVE", last_modified_ts=lm))
            for c in range(2):
                cid = f"C{k:04d}{c}"
                controls.append(dict(control_id=cid, description=f"Control {c + 1} mitigating {rid}",
                                     control_type=r.choice(["PREVENTIVE", "DETECTIVE", "CORRECTIVE"]),
                                     nature=r.choice(["MANUAL", "AUTOMATED"]),
                                     frequency=r.choice(["DAILY", "WEEKLY", "MONTHLY", "PER_TRANSACTION"]),
                                     is_key_control=(c == 0), owner_emplid=f"E{r.randint(1, 20000):07d}",
                                     status="ACTIVE", last_modified_ts=lm))
                links.append(dict(risk_id=rid, control_id=cid, last_modified_ts=lm))
    for rk in risks:  # shared controls: ~30% of risks also rely on another control
        if r.random() < 0.3:
            links.append(dict(risk_id=rk["risk_id"], control_id=r.choice(controls)["control_id"],
                              last_modified_ts=lm))
    links = list({(x["risk_id"], x["control_id"]): x for x in links}.values())
    return org, risks, controls, links


def rcsa_and_tests(r, risks, controls, batch):
    quarters = ["2025Q1", "2025Q2", "2025Q3", "2025Q4", "2026Q1", "2026Q2", "2026Q3"]
    rcsa, tests = [], []
    for rk in risks:
        il, ii = r.randint(2, 5), r.randint(2, 5)
        for qi, q in enumerate(quarters):
            eff = r.choices(["EFFECTIVE", "PARTIALLY_EFFECTIVE", "INEFFECTIVE"], [6, 3, 1])[0]
            red = {"EFFECTIVE": 2, "PARTIALLY_EFFECTIVE": 1, "INEFFECTIVE": 0}[eff]
            status = "APPROVED"
            if q == "2026Q3" and batch == 0:
                status = r.choice(["SUBMITTED", "CHALLENGED", "APPROVED"])
            rcsa.append(dict(assessment_id=f"A-{rk['risk_id']}-{q}", risk_id=rk["risk_id"],
                             assessment_period=q, inherent_likelihood=il, inherent_impact=ii,
                             control_effectiveness=eff, residual_likelihood=max(1, il - red),
                             residual_impact=max(1, ii - (1 if red == 2 else 0)),
                             assessor_emplid=rk["owner_emplid"], approval_status=status,
                             last_modified_ts=ts(AS_AT[batch])))
    for c in controls:
        if not c["is_key_control"]:
            continue
        for half, d in (("2025H2", D(2025, 12, 15)), ("2026H1", D(2026, 6, 15))):
            de = "PASS" if r.random() < 0.95 else "FAIL"
            oe = "PASS" if (de == "PASS" and r.random() < 0.88) else "FAIL"
            ss = r.choice([10, 25, 40])
            tests.append(dict(test_id=f"T-{c['control_id']}-{half}", control_id=c["control_id"],
                              test_date=d.isoformat(), tester_emplid=f"E{r.randint(1, 20000):07d}",
                              design_effectiveness=de, operating_effectiveness=oe,
                              sample_size=ss, exceptions=0 if oe == "PASS" else r.randint(1, 5),
                              last_modified_ts=ts(d)))
    return rcsa, tests


KRIS = [  # id, name, unit, amber, red, direction, org unit
    ("K01", "Failed trades per day", "count", 20, 40, "HIGHER_IS_WORSE", "OU301"),
    ("K02", "Payment exceptions per day", "count", 50, 100, "HIGHER_IS_WORSE", "OU103"),
    ("K03", "Critical system downtime", "minutes", 60, 180, "HIGHER_IS_WORSE", "OU103"),
    ("K04", "Phishing simulation click rate", "percent", 5, 10, "HIGHER_IS_WORSE", "OU101"),
    ("K05", "Unreconciled nostro breaks > 5 days", "count", 10, 25, "HIGHER_IS_WORSE", "OU202"),
    ("K06", "Staff turnover (annualised)", "percent", 15, 25, "HIGHER_IS_WORSE", "OU101"),
    ("K07", "Upheld complaints per month", "count", 30, 60, "HIGHER_IS_WORSE", "OU102"),
    ("K08", "Overdue audit actions", "count", 5, 10, "HIGHER_IS_WORSE", "OU201"),
    ("K09", "Key control testing completion", "percent", 90, 80, "LOWER_IS_WORSE", "OU104"),
    ("K10", "Vendor SLA breaches per month", "count", 3, 6, "HIGHER_IS_WORSE", "OU203"),
    ("K11", "Card fraud losses as % of spend", "bps", 6, 10, "HIGHER_IS_WORSE", "OU102"),
    ("K12", "Mortgage file defects", "percent", 4, 8, "HIGHER_IS_WORSE", "OU402"),
]


def kris(r, risks, batch):
    defs, obs = [], []
    for kid, name, unit, amber, red, direction, ou in KRIS:
        risk = next((x for x in risks if x["org_unit_id"] == ou), risks[0])
        defs.append(dict(kri_id=kid, name=name, risk_id=risk["risk_id"], unit=unit, frequency="MONTHLY",
                         amber_threshold=amber, red_threshold=red, direction=direction,
                         last_modified_ts=ts(AS_AT[0])))
        months = []
        y, m = 2025, 1
        last = (2026, 9) if batch == 0 else (2026, 10)
        while (y, m) <= last:
            months.append((y, m))
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        if batch == 1:
            months = [(2026, 10)]
        base = amber * 0.7 if direction == "HIGHER_IS_WORSE" else amber * 1.05
        for (y, m) in months:
            trend = 1 + (0.02 * ((y - 2025) * 12 + m)) if kid in ("K02", "K07") else 1
            value = round(max(0, base * trend * r.uniform(0.6, 1.5)), 2)
            pe = (D(y, m, 28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
            obs.append(dict(kri_id=kid, period_end=pe.isoformat(), value=value,
                            last_modified_ts=ts(pe + dt.timedelta(days=3))))
    return defs, obs


# ----------------------------------------------------------------------------- loss events
def gen_event(r, n, le, ou, bl_grc, occurred, batch_date, ccy):
    et = weighted(r, EVENT_TYPES, 5)
    l1 = et[1]
    disc = min(batch_date, occurred + dt.timedelta(days=int(r.expovariate(1 / 30))))
    acct = min(batch_date, disc + dt.timedelta(days=int(r.expovariate(1 / 15))))
    near_miss = r.random() < 0.08
    gain = (not near_miss) and r.random() < 0.01
    boundary = "NONE"
    if bl_grc in ("BL05", "BL06") and l1 == "EDPM" and r.random() < 0.3:
        boundary = "MARKET"
    if bl_grc == "BL12" and l1 == "EF" and r.random() < 0.2:
        boundary = "CREDIT"
    excluded = r.random() < 0.01
    eid = f"GRC-{occurred.year}-{n:06d}"
    name = f"{r.choice(FIRST)} {r.choice(LAST)}"
    ev = dict(event_id=eid, event_title=f"{et[3]} - {ou}",
              event_description=DESCRIPTIONS[l1].format(name=name, ref=f"{r.randint(100000, 999999)}"),
              legal_entity_id=le, org_unit_id=ou, business_line_code=bl_grc, event_type_code=et[4],
              cause_code=r.choice(CAUSES)[3], occurrence_date=occurred.isoformat(),
              discovery_date=disc.isoformat(), first_accounting_date=None if near_miss else acct.isoformat(),
              event_status="CLOSED" if (batch_date - acct).days > 180 else r.choice(["OPEN", "UNDER_REVIEW", "CLOSED"]),
              boundary_type=boundary, is_near_miss=near_miss, is_operational_gain=gain,
              is_rapidly_recovered=False, parent_event_id=None,
              inclusion_status="EXCLUDED" if excluded else "INCLUDED",
              exclusion_reason=("Divested activity" if excluded else None),
              confidentiality="RESTRICTED" if l1 == "IF" else "STANDARD",
              reported_by_emplid=f"E{r.randint(1, 20000):07d}", owner_emplid=f"E{r.randint(1, 20000):07d}",
              currency=ccy, last_modified_ts=ts(min(batch_date, acct + dt.timedelta(days=1))))
    moves, k = [], 0

    def mv(mtype, amt, d, effect):
        nonlocal k
        k += 1
        if d <= batch_date:
            moves.append(dict(movement_id=f"{eid}-M{k:02d}", event_id=eid, movement_type=mtype,
                              effect_type_code=effect, amount=round(amt, 2), currency=ccy,
                              accounting_date=d.isoformat(), gl_account=GL_ACCOUNTS[mtype],
                              cost_centre=next(o[4] for o in ORG_UNITS if o[0] == ou)))
    if not near_miss and not gain:
        gross = amount(r)
        if l1 in ("CPBP", "EPWS") and r.random() < 0.5:  # legal: provision, then settle
            prov = gross * r.uniform(1.0, 1.4)
            mv("PROVISION", prov, acct, "LEGAL")
            settle = acct + dt.timedelta(days=r.randint(90, 500))
            mv("PROVISION_RELEASE", prov, settle, "LEGAL")
            mv("GROSS_LOSS", gross, settle, "LEGAL" if l1 == "EPWS" else "RESTITUTION")
        else:
            effect = {"EF": "WRITE_DOWN", "IF": "WRITE_DOWN", "DPA": "ASSET_LOSS", "BDSF": "RESTITUTION"}.get(l1, "RESTITUTION")
            if l1 == "CPBP" and et[0] == "CPBP_IBMP":
                effect = "PENALTY"
            mv("GROSS_LOSS", gross, acct, effect)
        if l1 in ("EF", "DPA") and r.random() < 0.35:
            rapid = r.random() < 0.06
            rec_d = acct + dt.timedelta(days=r.randint(1, 4) if rapid else r.randint(30, 400))
            rec = gross if rapid else gross * r.uniform(0.2, 0.9)
            mv("RECOVERY_INSURANCE" if l1 == "DPA" else "RECOVERY_OTHER", rec, rec_d, "WRITE_DOWN")
            ev["is_rapidly_recovered"] = rapid and rec_d <= batch_date
        ev["_gross"] = gross
    attrs = []
    if et[0] == "EF_SS":
        attrs.append("CYBER")
    if et[0] == "EDPM_VS":
        attrs.append("THIRD_PARTY")
    if l1 == "CPBP":
        attrs.append("CONDUCT")
        if r.random() < 0.05:
            attrs.append("ESG")
    if ev.get("_gross", 0) > 1_000_000:
        attrs.append("LARGE_LOSS")
    ev.pop("_gross", None)
    return ev, moves, [dict(event_id=eid, attribute_code=a, last_modified_ts=ev["last_modified_ts"]) for a in attrs]


def grc_losses(r, batch):
    batch_date = AS_AT[batch]
    events, moves, attrs, n = [], [], [], 0
    ccy_of = {le[0]: le[4] for le in LEGAL_ENTITIES}
    for o, le, _dv, _bu, _cc, bl in ORG_UNITS:
        if le == "LE04":
            start = D(2023, 1, 1)  # acquired: in the GRC from 2023, legacy log before
        else:
            start = D(2016, 1, 1)
        bl_grc = next(b[4] for b in BUSINESS_LINES if b[0] == bl)
        per_year = 22 if le != "LE04" else 15
        day = start
        while day <= batch_date:
            if r.random() < per_year / 365:
                n += 1
                ccy = ccy_of[le] if r.random() > 0.08 else "USD"
                ev, mv, at = gen_event(r, n, le, o, bl_grc, day, batch_date, ccy)
                events.append(ev)
                moves += mv
                attrs += at
            day += dt.timedelta(days=1)
    # linked events: some outages produce follow-on events grouped under a parent
    outages = [e for e in events if e["event_type_code"] == "ET6.1"]
    for e in outages[: max(1, len(outages) // 10)]:
        child = next((x for x in events if x["legal_entity_id"] == e["legal_entity_id"]
                      and x["event_type_code"] == "ET7.4" and x["parent_event_id"] is None
                      and x["occurrence_date"] >= e["occurrence_date"]), None)
        if child:
            child["parent_event_id"] = e["event_id"]
    return events, moves, attrs


def legacy_loss_log(r, rows_from=2016, rows_to=2022, start_n=1, unmapped=False):
    out = []
    n = start_n
    for year in range(rows_from, rows_to + 1):
        for _ in range(50):
            occurred = D(year, 1, 1) + dt.timedelta(days=r.randint(0, 364))
            found = occurred + dt.timedelta(days=int(r.expovariate(1 / 25)))
            booked = found + dt.timedelta(days=int(r.expovariate(1 / 10)))
            cat = r.choice(list(LEGACY_CATEGORIES))
            if unmapped:
                cat = "Data Breach"
            dept = r.choice(list(LEGACY_DEPTS))
            loss = round(amount(r) * 0.85, 2)
            rec = round(loss * r.uniform(0.1, 0.8), 2) if cat in ("Card Fraud", "Fraud - External", "Premises Damage") and r.random() < 0.4 else 0
            out.append([f"NG-{year}-{n:04d}", occurred.strftime("%d/%m/%Y"), found.strftime("%d/%m/%Y"),
                        booked.strftime("%d/%m/%Y"), cat, dept, f"{loss:.2f}", f"{rec:.2f}",
                        f"{cat} reported at {dept} by {r.choice(FIRST)} {r.choice(LAST)}",
                        r.choice(["Closed", "Closed", "Open"])])
            n += 1
    return out


def gl_postings(r, moves, breaks=True):
    out = []
    for m in moves:
        sign = -1 if m["movement_type"] in ("PROVISION_RELEASE", "RECOVERY_INSURANCE", "RECOVERY_OTHER") else 1
        amt = sign * m["amount"]
        posting_date = dt.date.fromisoformat(m["accounting_date"])
        x = r.random()
        if breaks and x < 0.01:
            continue  # missing in GL
        if breaks and x < 0.015:
            amt = round(amt * r.uniform(0.9, 1.1), 2)  # amount difference
        out.append([f"JE-{len(out) + 1:08d}", posting_date.isoformat(), m["gl_account"], m["cost_centre"],
                    f"{amt:.2f}", m["currency"], f"OPL {m['event_id']}"])
    return out


GL_HEADER = ["journal_id", "posting_date", "gl_account", "cost_centre", "amount", "currency", "narrative"]
LEGACY_HEADER = ["Incident Ref", "Date Occurred", "Date Found", "Date Booked", "Category", "Dept",
                 "Loss GBP", "Recovered GBP", "Description", "Status"]


# ----------------------------------------------------------------------------- batches
def generate(root, batch, seed=11):
    r = random.Random(seed)
    org, risks, controls, links = grc_master(r, AS_AT[0])
    rcsa, tests = rcsa_and_tests(random.Random(seed + 1), risks, controls, 0)
    kri_defs, kri_obs = kris(random.Random(seed + 2), risks, 0)
    issues = make_issues(random.Random(seed + 3), risks, controls)
    events, moves, attrs = grc_losses(random.Random(seed + 4), 0)
    legacy = legacy_loss_log(random.Random(seed + 5))

    if batch == 0:
        for name, rows in [("org_units", org), ("risks", risks), ("controls", controls),
                           ("risk_controls", links), ("rcsa_assessments", rcsa), ("control_tests", tests),
                           ("kri_definitions", kri_defs), ("kri_observations", kri_obs), ("issues", issues),
                           ("loss_events", events), ("loss_movements", moves), ("event_attributes", attrs)]:
            write_jsonl(root, "grc", name, 0, rows)
        write_csv(root, "legacy", "loss_log", 0, LEGACY_HEADER, legacy)
        write_csv(root, "gl", "postings", 0, GL_HEADER, gl_postings(random.Random(seed + 6), moves))
        return

    # ---- batch 1: incremental extract as at 2026-10-31
    rb = random.Random(seed + 100)
    lm = ts(AS_AT[1])
    new_events, new_moves, new_attrs = [], [], []
    for i in range(40):
        o, le, _dv, _bu, _cc, bl = rb.choice(ORG_UNITS)
        bl_grc = next(b[4] for b in BUSINESS_LINES if b[0] == bl)
        ccy = {x[0]: x[4] for x in LEGAL_ENTITIES}[le]
        ev, mv, at = gen_event(rb, 900000 + i, le, o, bl_grc, D(2026, 10, rb.randint(1, 20)), AS_AT[1], ccy)
        new_events.append(ev)
        new_moves += mv
        new_attrs += at
    # reclassifications after 2nd-line review (SCD2 history)
    reclass = []
    for ev in rb.sample([e for e in events if e["event_type_code"].startswith("ET7")], 15):
        e2 = dict(ev, event_type_code="ET4.1", last_modified_ts=lm)
        reclass.append(e2)
    # late-booked provisions/settlements on older events
    late = []
    for ev in rb.sample([e for e in events if e["first_accounting_date"] and e["occurrence_date"] < "2025-06-30"], 20):
        late.append(dict(movement_id=f"{ev['event_id']}-L01", event_id=ev["event_id"], movement_type="PROVISION",
                         effect_type_code="LEGAL", amount=round(amount(rb) * 3, 2), currency=ev["currency"],
                         accounting_date=D(2026, 10, rb.randint(1, 28)).isoformat(), gl_account="640200",
                         cost_centre=next(o[4] for o in ORG_UNITS if o[0] == ev["org_unit_id"])))
    # injected data problems
    bad = []
    for i, ev in enumerate(rb.sample(new_events, 5)):  # discovery before occurrence
        bad.append(dict(ev, event_id=f"GRC-2026-BAD{i:03d}", discovery_date="2026-09-01",
                        occurrence_date="2026-10-15"))
    for i in range(3):  # unknown event type code after a GRC configuration change
        bad.append(dict(new_events[i], event_id=f"GRC-2026-UNM{i:03d}", event_type_code="ET9.9"))
    duplicates = rb.sample(events, 10)  # same records re-sent in the extract
    # issue lifecycle changes
    closed = [dict(x, status="CLOSED", closed_date="2026-10-20", last_modified_ts=lm)
              for x in rb.sample([i for i in issues if i["status"] != "CLOSED"], 10)]
    # RCSA 2026Q3 approvals after challenge
    approvals = [dict(a, approval_status="APPROVED", last_modified_ts=lm)
                 for a in rcsa if a["assessment_period"] == "2026Q3" and a["approval_status"] != "APPROVED"]
    _defs, obs_oct = kris(random.Random(seed + 2), risks, 1)

    write_jsonl(root, "grc", "loss_events", 1, new_events + reclass + bad + duplicates)
    write_jsonl(root, "grc", "loss_movements", 1, new_moves + late)
    write_jsonl(root, "grc", "event_attributes", 1, new_attrs)
    write_jsonl(root, "grc", "issues", 1, closed)
    write_jsonl(root, "grc", "rcsa_assessments", 1, approvals)
    write_jsonl(root, "grc", "kri_observations", 1, obs_oct)
    write_csv(root, "legacy", "loss_log", 1, LEGACY_HEADER,
              legacy_loss_log(rb, 2022, 2022, start_n=5001, unmapped=True)[:5])
    write_csv(root, "gl", "postings", 1, GL_HEADER, gl_postings(rb, new_moves + late))


def make_issues(r, risks, controls):
    out = []
    for i in range(1, 81):
        src = r.choice(["AUDIT", "RCSA", "LOSS_EVENT", "REGULATOR", "SELF_IDENTIFIED"])
        raised = D(2025, 1, 1) + dt.timedelta(days=r.randint(0, 600))
        due = raised + dt.timedelta(days=r.choice([30, 60, 90, 180]))
        status = r.choice(["OPEN", "IN_PROGRESS", "CLOSED", "CLOSED"])
        rk = r.choice(risks)
        out.append(dict(issue_id=f"I{i:04d}", title=f"{src.title()} finding on {rk['risk_id']}", source=src,
                        severity=r.choices(["LOW", "MEDIUM", "HIGH", "CRITICAL"], [4, 4, 2, 1])[0],
                        event_id=None, risk_id=rk["risk_id"], control_id=r.choice(controls)["control_id"],
                        owner_emplid=rk["owner_emplid"], raised_date=raised.isoformat(), due_date=due.isoformat(),
                        status=status, closed_date=(due - dt.timedelta(days=5)).isoformat() if status == "CLOSED" else None,
                        last_modified_ts=ts(raised)))
    return out


def write_reference_tables(spark, catalog):
    tables = {
        "ref_event_type": ([(e[0], e[1], e[2], e[3]) for e in EVENT_TYPES],
                           "event_type_l2_code string, event_type_l1_code string, event_type_l1_name string, event_type_l2_name string"),
        "ref_business_line": ([(b[0], b[1], b[2], b[3]) for b in BUSINESS_LINES],
                              "business_line_l2_code string, business_line_l1_code string, business_line_l1_name string, business_line_l2_name string"),
        "ref_cause": ([(c[0], c[1], c[2]) for c in CAUSES], "cause_code string, cause_l1 string, cause_l2_name string"),
        "ref_effect_type": (EFFECTS, "effect_type_code string, effect_type_name string"),
        "ref_loss_attribute": (ATTRIBUTES, "attribute_code string, attribute_name string"),
        "ref_legal_entity": (LEGAL_ENTITIES, "legal_entity_id string, legal_entity_name string, lei string, country_code string, functional_currency string"),
        "ref_crosswalk": (crosswalk_rows(), "source_system string, source_domain string, source_code string, canonical_code string, valid_from string, valid_to string"),
        "ref_fx_daily": (fx_rows(), "currency string, rate_date string, rate_to_eur double, rate_to_gbp double"),
    }
    for name, (rows, schema) in tables.items():
        df = spark.createDataFrame(rows, schema)
        for c in ("valid_from", "valid_to", "rate_date"):
            if c in df.columns:
                df = df.withColumn(c, df[c].cast("date"))
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.{name}")
        print(f"wrote reference table {catalog}.silver.{name} ({len(rows)} rows)")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Synthetic ORM source generator")
    p.add_argument("--catalog", required=True, help="e.g. risk_dev")
    p.add_argument("--batch-id", type=int, default=0, choices=[0, 1])
    p.add_argument("--output-root", default=None, help="Defaults to /Volumes/<catalog>/bronze/landing/orm")
    p.add_argument("--write-reference-tables", default="true")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    root = args.output_root or f"/Volumes/{args.catalog}/bronze/landing/orm"
    generate(root, args.batch_id)
    if args.batch_id == 0 and args.write_reference_tables.lower() == "true":
        from pyspark.sql import SparkSession
        write_reference_tables(SparkSession.builder.getOrCreate(), args.catalog)


if __name__ == "__main__":
    main(sys.argv[1:])

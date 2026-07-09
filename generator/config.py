"""Static catalogs and tunable rates for the bancassurance simulation."""

# (code, name, line_of_business, underwriter, commission_rate, (min, max) annual premium KES)
PRODUCTS = [
    ("MOTOR_COMP", "Motor Comprehensive", "MOTOR", "Jubilee General", 0.10, (28_000, 150_000)),
    ("MOTOR_TPO", "Motor Third Party Only", "MOTOR", "Jubilee General", 0.08, (7_500, 15_000)),
    ("LIFE_TERM", "Term Life Assurance", "LIFE", "ICEA Lion Life", 0.25, (12_000, 60_000)),
    ("LIFE_ENDW", "Endowment Plan", "LIFE", "ICEA Lion Life", 0.30, (24_000, 120_000)),
    ("HEALTH_IND", "Individual Medical Cover", "HEALTH", "AAR Insurance", 0.12, (18_000, 90_000)),
    ("HEALTH_FAM", "Family Medical Cover", "HEALTH", "AAR Insurance", 0.12, (45_000, 220_000)),
    ("PA_BASIC", "Personal Accident Basic", "PERSONAL_ACCIDENT", "Britam General", 0.15, (3_000, 9_000)),
    ("LAST_EXP", "Last Expense Cover", "LIFE", "CIC Life", 0.20, (4_800, 14_400)),
    ("TRAVEL", "Travel Insurance", "TRAVEL", "Britam General", 0.15, (5_000, 20_000)),
    ("HOME", "Domestic Package", "PROPERTY", "CIC General", 0.12, (10_000, 45_000)),
]

# (code, name, county, region, tier)
BRANCHES = [
    ("BR001", "Nairobi CBD", "Nairobi", "Nairobi", 1),
    ("BR002", "Westlands", "Nairobi", "Nairobi", 1),
    ("BR003", "Mombasa Nkrumah Rd", "Mombasa", "Coast", 1),
    ("BR004", "Kisumu Oginga Odinga St", "Kisumu", "Nyanza", 2),
    ("BR005", "Nakuru Kenyatta Ave", "Nakuru", "Rift Valley", 2),
    ("BR006", "Eldoret Uganda Rd", "Uasin Gishu", "Rift Valley", 2),
    ("BR007", "Thika Arcade", "Kiambu", "Central", 3),
    ("BR008", "Nyeri Kimathi Way", "Nyeri", "Central", 3),
]

CUSTOMER_COUNTIES = [
    "Nairobi", "Mombasa", "Kisumu", "Nakuru", "Uasin Gishu",
    "Kiambu", "Nyeri", "Machakos", "Kakamega", "Meru",
]

RISK_TIERS = ["LOW", "STANDARD", "HIGH_RISK"]
SEGMENTS = ["RETAIL", "SME"]
PAYMENT_FREQUENCIES = {"MONTHLY": 12, "QUARTERLY": 4, "ANNUAL": 1}

N_AGENTS = 60
N_INITIAL_CUSTOMERS = 250

# Daily rates / probabilities
NEW_CUSTOMERS_PER_DAY = 4          # Poisson mean
P_EXTRA_POLICY_EXISTING = 0.003    # per customer per day
P_RISK_RERATE = 0.002              # per customer per day (feeds SCD2)
P_AGENT_TRANSFER = 0.002           # per agent per day (feeds SCD2)
P_POLICY_CANCEL = 0.0004           # per active policy per day
P_CLAIM = 0.0012                   # per active policy per day
P_MISSED_PAYMENT = 0.08
P_PARTIAL_PAYMENT = 0.05
P_MPESA_DUPLICATE = 0.02           # gateway re-pushes the same transaction
MISSED_BEFORE_LAPSE = 2

# Channel mix: (channel, source, weight)
CHANNELS = [
    ("MPESA", "mpesa_gateway", 65),
    ("BANK_TRANSFER", "bank_payments", 25),
    ("CHEQUE", "bank_payments", 7),
    ("CASH", "bank_payments", 3),
]

# Payment lag in days after due date, by channel: (min, max)
CHANNEL_LAG = {
    "MPESA": (0, 5),
    "BANK_TRANSFER": (1, 12),
    "CHEQUE": (3, 15),
    "CASH": (0, 8),
}

# The mpesa_gateway feed gains a `paybill_ref` column this many days into the
# simulation — deliberate schema drift for dlt/dbt to absorb.
SCHEMA_DRIFT_AFTER_DAYS = 45

# Claim events arrive in extracts up to this many days after they occurred.
MAX_CLAIM_EVENT_LATENESS = 14

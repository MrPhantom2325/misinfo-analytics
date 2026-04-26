"""
Sample data pools for realistic misinformation event generation.
Based on patterns from LIAR dataset and FakeNewsNet schema.
"""

# ─── Headline templates per category ───────────────────────────────────────────

HEADLINE_TEMPLATES = {
    "health": [
        "BREAKING: {vaccine} linked to {condition} in new study suppressed by {org}",
        "Doctors REFUSE to tell you: {cure} cures {disease} in 3 days",
        "Government admits {chemical} in water supply causing {condition}",
        "Big Pharma EXPOSED: {drug} causes {condition}, {count}k deaths covered up",
        "Natural {substance} DESTROYS {disease} — pharmaceutical industry panicking",
        "WHO silences researcher who proved {vaccine} causes {condition}",
        "LEAKED: CDC knew {vaccine} caused {condition} since {year}",
        "Hospital whistleblower: {count}0% of {disease} deaths actually caused by {drug}",
    ],
    "politics": [
        "EXPOSED: {politician} caught accepting bribes from {org} in secret meeting",
        "Leaked emails prove {politician} rigged {year} election with help from {country}",
        "BOMBSHELL: {politician} secretly a member of {conspiracy_group}",
        "{country} election machines pre-programmed to favor {politician}, source claims",
        "Deep state plot to remove {politician} from power revealed by insider",
        "SHOCKING: {politician} met secretly with {country} agents before election",
        "Voter fraud: {count} million illegal ballots discovered in {state}",
        "{politician} signs secret deal to hand over US sovereignty to {org}",
    ],
    "climate": [
        "Scientists SILENCED: Global warming is a {org} hoax to raise taxes",
        "LEAKED NASA data proves CO2 actually COOLS the planet",
        "Climate change agenda exposed: {org} pays scientists to fake data",
        "Arctic ice at RECORD HIGH — media refuses to cover it",
        "Green energy scam: {substance} turbines cause more pollution than coal",
        "Climate scientist admits models are wrong, gets fired by {org}",
        "{year} was actually COLDER than average, data shows governments lied",
        "Electric vehicles produce MORE carbon than gas cars, study finds",
    ],
    "finance": [
        "URGENT: {bank} about to collapse — move your money NOW",
        "Insider warns: {currency} crash incoming, {country} dumping dollar",
        "{crypto} about to hit ${amount}k — banks trying to suppress it",
        "Federal Reserve secretly buying {asset} to crash economy before election",
        "EXPOSED: {bank} freezing accounts of customers who criticize government",
        "New {law} gives government power to seize your bank account without notice",
        "{billionaire} quietly moves wealth to gold before market crash",
        "CBDC rollout means government can block your spending — experts warn",
    ],
}

# ─── Fill-in values ─────────────────────────────────────────────────────────────

TEMPLATE_VARS = {
    "vaccine": ["COVID vaccine", "mRNA vaccine", "flu shot", "HPV vaccine", "MMR vaccine"],
    "condition": ["autism", "cancer", "infertility", "heart disease", "paralysis", "dementia"],
    "org": ["WHO", "CDC", "FDA", "Big Pharma", "UN", "WEF", "Gates Foundation", "Soros Foundation"],
    "cure": ["ivermectin", "hydroxychloroquine", "vitamin D", "bleach solution", "colloidal silver"],
    "disease": ["COVID", "cancer", "diabetes", "HIV", "Alzheimer's"],
    "chemical": ["fluoride", "lithium", "5G radiation", "microplastics", "chemtrails"],
    "drug": ["Remdesivir", "Pfizer vaccine", "statins", "antidepressants", "Tamiflu"],
    "substance": ["turmeric", "hemp oil", "baking soda", "lemon juice", "oregano oil"],
    "politician": ["Biden", "Trump", "Obama", "Pelosi", "Fauci", "Gates", "Soros"],
    "conspiracy_group": ["Illuminati", "deep state", "Bilderberg Group", "New World Order", "WEF elites"],
    "country": ["China", "Russia", "Iran", "North Korea", "Soros network"],
    "state": ["Arizona", "Georgia", "Pennsylvania", "Nevada", "Wisconsin"],
    "bank": ["JPMorgan", "Bank of America", "Wells Fargo", "Goldman Sachs", "the Fed"],
    "currency": ["dollar", "euro", "yuan", "yen"],
    "crypto": ["Bitcoin", "Ethereum", "XRP", "Dogecoin"],
    "asset": ["gold", "Bitcoin", "real estate", "foreign bonds"],
    "law": ["executive order", "Senate bill", "UN resolution", "WEF mandate"],
    "billionaire": ["Buffett", "Musk", "Gates", "Soros", "Bezos", "Rothschild"],
    "count": ["3", "5", "10", "50", "100", "500"],
    "amount": ["100", "250", "500", "1000"],
    "year": ["2020", "2021", "2022", "2019"],
}

# ─── Source domains (fake/low-credibility) ──────────────────────────────────────

SOURCE_DOMAINS = {
    "health": [
        "naturalcures247.net", "healthtruthexposed.com", "vaccinedangers.org",
        "realmedicinenews.net", "pharmaexposed.info", "antivaxxertruth.com",
        "holistichealthsecrets.net", "censoredhealthnews.org",
    ],
    "politics": [
        "patriotpulse247.com", "deepstateexposed.net", "electionfraudproof.org",
        "conservativealert.net", "truthaboutpolitics.info", "liberalagenda.exposed",
        "realamericannews.net", "governmentlies.org",
    ],
    "climate": [
        "climatescam.net", "globalwarminghoax.org", "energytruth247.com",
        "co2facts.net", "climatefraud.info", "greenenergylies.org",
        "realclimatedata.net", "ecohoax.com",
    ],
    "finance": [
        "marketcrashwarning.com", "financialcollapse.net", "cryptoinsider247.org",
        "bankingfraud.info", "dollarcrash.net", "economictruth.org",
        "wallstreetexposed.com", "cbdcwarning.net",
    ],
}

# ─── Platforms ───────────────────────────────────────────────────────────────────

PLATFORMS = ["twitter", "facebook", "telegram", "tiktok", "youtube", "rumble", "gab", "truth_social"]

# ─── Country codes with weighted distribution ────────────────────────────────────

GEO_ORIGINS = {
    "US": 0.28,
    "RU": 0.14,
    "IN": 0.10,
    "BR": 0.08,
    "GB": 0.06,
    "DE": 0.05,
    "CN": 0.05,
    "PH": 0.05,
    "MX": 0.04,
    "TR": 0.03,
    "PK": 0.03,
    "NG": 0.03,
    "ID": 0.02,
    "FR": 0.02,
    "IT": 0.02,
}

# ─── Keyword pools per category ─────────────────────────────────────────────────

KEYWORD_POOLS = {
    "health": [
        "vaccine", "autism", "cover-up", "big pharma", "natural cure",
        "suppressed", "CDC lies", "FDA fraud", "miracle cure", "detox",
        "5G", "microchip", "population control", "depopulation",
    ],
    "politics": [
        "election fraud", "deep state", "rigged", "stolen election", "cabal",
        "globalist", "NWO", "false flag", "crisis actor", "shadow government",
        "martial law", "FEMA camps", "censorship",
    ],
    "climate": [
        "climate hoax", "global warming lie", "CO2 myth", "green scam",
        "agenda 21", "WEF plot", "carbon tax fraud", "fake science",
        "chemtrails", "geoengineering", "weather control",
    ],
    "finance": [
        "market crash", "dollar collapse", "bank run", "crypto moon",
        "CBDC control", "economic collapse", "hyperinflation", "gold rush",
        "Fed fraud", "rothschild", "banking cartel", "fiat scam",
    ],
}

# ─── Sentiment ranges per category ──────────────────────────────────────────────

SENTIMENT_RANGES = {
    "health": (-0.95, -0.50),
    "politics": (-0.90, -0.30),
    "climate": (-0.85, -0.40),
    "finance": (-0.80, -0.20),
}

# ─── Credibility score ranges (intentionally low — these are misinfo) ────────────

CREDIBILITY_RANGES = {
    "health": (0.02, 0.35),
    "politics": (0.05, 0.38),
    "climate": (0.05, 0.40),
    "finance": (0.08, 0.42),
}

# ─── Share velocity ranges (shares per minute) ──────────────────────────────────

VELOCITY_RANGES = {
    "normal": (50, 2000),
    "trending": (2000, 8000),
    "viral": (8000, 50000),
}

# ─── Viral burst campaign templates ─────────────────────────────────────────────

BURST_CAMPAIGNS = [
    {
        "name": "Health Disinformation Wave",
        "category": "health",
        "geo_focus": ["US", "GB", "AU"],
        "platform_focus": ["twitter", "facebook"],
        "duration_secs": 30,
        "rate_multiplier": 15,
    },
    {
        "name": "Election Fraud Narrative Push",
        "category": "politics",
        "geo_focus": ["US"],
        "platform_focus": ["twitter", "truth_social", "gab"],
        "duration_secs": 45,
        "rate_multiplier": 20,
    },
    {
        "name": "Russian Geo-Targeted Campaign",
        "category": "politics",
        "geo_focus": ["RU", "UA", "BY"],
        "platform_focus": ["telegram"],
        "duration_secs": 60,
        "rate_multiplier": 12,
    },
    {
        "name": "Financial Panic Campaign",
        "category": "finance",
        "geo_focus": ["US", "EU", "GB"],
        "platform_focus": ["youtube", "twitter", "rumble"],
        "duration_secs": 30,
        "rate_multiplier": 10,
    },
]
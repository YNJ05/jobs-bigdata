"""Level 1 - simple keyword based classifier (pure Python, no ML).

Each domain has a list of keywords. The text of an offer (title + skills +
description) is scanned; every keyword found adds points to its domain. The
title counts triple because it is the strongest signal. The domain with the
highest score wins; when nothing matches we answer "Inconnu".
"""
import re

DOMAINS = ["Data", "Développement", "Sécurité", "Cloud"]

KEYWORDS = {
    "Data": [
        "data", "big data", "machine learning", "deep learning", "ai", "nlp",
        "spark", "pyspark", "hadoop", "kafka", "flink", "airflow", "dbt",
        "etl", "snowflake", "tableau", "power bi", "pandas", "scikit-learn",
        "pytorch", "tensorflow", "sql", "nosql", "cassandra", "analytics",
        "analyst", "scientist", "warehouse", "bi", "mlops", "computer vision",
    ],
    "Développement": [
        "developer", "software", "backend", "frontend", "full stack", "fullstack",
        "java", "spring", "spring boot", "hibernate", "react", "angular", "vue",
        "javascript", "typescript", "node.js", "django", "fastapi", "flask",
        "golang", "go", "rust", "c++", "c#", ".net", "php", "html5", "css3",
        "rest api", "graphql", "grpc", "microservices", "postgresql", "redis",
        "mobile", "android", "ios", "web",
    ],
    "Sécurité": [
        "security", "cybersecurity", "cyber", "penetration", "pentest", "hacker",
        "vulnerability", "owasp", "burp suite", "metasploit", "kali", "soc",
        "siem", "splunk", "incident response", "threat", "wireshark", "iam",
        "identity", "cyberark", "okta", "zero trust", "oauth2", "iso 27001",
        "devsecops", "sonarqube", "snyk", "forensic", "malware", "encryption",
        "compliance", "firewall", "active directory",
    ],
    "Cloud": [
        "cloud", "aws", "azure", "gcp", "google cloud", "terraform", "kubernetes",
        "k8s", "helm", "ansible", "devops", "sre", "infrastructure", "observability",
        "prometheus", "grafana", "linux", "powershell", "arm templates",
        "cloud build", "cloudformation", "serverless", "iaas", "paas", "bash",
        "container", "containers", "docker", "migration", "sysadmin", "systems",
    ],
}

# Pre-compile one regex per keyword with word boundaries, so "go" does not
# match "good" and "ai" does not match "email".
_PATTERNS = {
    dom: [re.compile(r"(?<![a-z0-9])" + re.escape(k) + r"(?![a-z0-9])") for k in kws]
    for dom, kws in KEYWORDS.items()
}


def _score(text, patterns):
    return sum(1 for p in patterns if p.search(text))


def classify(title, skills, description):
    """Return the predicted domain for one offer."""
    title = (title or "").lower()
    body = " ".join([" ".join(skills or []), description or ""]).lower()
    scores = {}
    for dom, pats in _PATTERNS.items():
        scores[dom] = 3 * _score(title, pats) + _score(body, pats)
    best = max(scores.values())
    if best == 0:
        return "Inconnu"
    # deterministic tie-break: first domain in DOMAINS order
    return next(d for d in DOMAINS if scores[d] == best)

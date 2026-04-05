"""
Data extraction module for College Data Enrichment CRM.
Extracts phone numbers, emails, and principal names from scraped text.
"""

import re
from collections import Counter

# ─── Phone Number Extraction ─────────────────────────────────────

PHONE_PATTERNS = [
    # Indian mobile: +91 followed by 10 digits (6-9 start)
    r'(?:\+91[\s.-]?)?[6-9]\d{4}[\s.-]?\d{5}',
    # With +91 prefix: +91-XXXX-XXXXXX
    r'\+91[\s.-]?\d{4,5}[\s.-]?\d{5,6}',
    # Landline with STD code: 0XX-XXXXXXX or 0XXX-XXXXXXX
    r'0\d{2,4}[\s.-]?\d{6,8}',
    # Parenthesized STD: (044) XXXXXXXX
    r'\(\d{2,5}\)[\s.-]?\d{6,8}',
    # Plain 10-digit starting with 6-9
    r'\b[6-9]\d{9}\b',
    # Toll-free: 1800-XXX-XXXX
    r'1800[\s.-]?\d{3}[\s.-]?\d{4}',
]

# Numbers to exclude (common false positives)
PHONE_BLACKLIST = [
    '1234567890', '0000000000', '9999999999', '1111111111',
    '9876543210', '0123456789',
]


def extract_phones(text):
    """
    Extract Indian phone numbers from text.
    Returns list of cleaned phone numbers, sorted by likelihood of being official.
    """
    if not text:
        return []

    all_phones = []

    for pattern in PHONE_PATTERNS:
        matches = re.findall(pattern, text)
        for match in matches:
            # Clean the number
            cleaned = re.sub(r'[\s.\-\(\)]', '', match)
            # Remove leading +91 or 0 for normalization
            normalized = cleaned.lstrip('+').lstrip('91').lstrip('0')

            # Validate length
            if len(normalized) < 7 or len(normalized) > 12:
                continue

            # Skip blacklisted
            if normalized in PHONE_BLACKLIST or cleaned in PHONE_BLACKLIST:
                continue

            # Skip if it looks like a year (19XX, 20XX)
            if re.match(r'^(19|20)\d{2}$', normalized):
                continue

            all_phones.append(cleaned)

    # Deduplicate while preserving order
    seen = set()
    unique_phones = []
    for phone in all_phones:
        norm = re.sub(r'[\s.\-\(\)\+]', '', phone)[-10:]
        if norm not in seen:
            seen.add(norm)
            unique_phones.append(phone)

    # Prioritize landlines (more likely official) over mobile
    def phone_score(p):
        cleaned = re.sub(r'[\s.\-\(\)\+]', '', p)
        if cleaned.startswith('0') or cleaned.startswith('91') and not cleaned[2:3] in '6789':
            return 0  # Landline — likely official
        if cleaned.startswith('+91'):
            return 1
        return 2  # Mobile

    unique_phones.sort(key=phone_score)
    return unique_phones[:5]  # Return top 5


# ─── Email Extraction ─────────────────────────────────────────────

EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# Domains to deprioritize
GENERIC_EMAIL_DOMAINS = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'rediffmail.com']
PREFERRED_EMAIL_DOMAINS = ['.edu.in', '.ac.in', '.edu', '.gov.in', '.nic.in', '.org.in']

EMAIL_BLACKLIST_PATTERNS = [
    r'example\.com', r'test\.com', r'sentry\.io', r'jquery', r'bootstrap',
    r'wordpress', r'w3\.org', r'schema\.org', r'googleapis', r'fontawesome',
    r'noreply', r'no-reply', r'donotreply',
]


def extract_emails(text):
    """
    Extract email addresses from text.
    Returns list sorted by relevance (official domains first).
    """
    if not text:
        return []

    matches = EMAIL_PATTERN.findall(text)
    valid_emails = []

    for email in matches:
        email = email.lower().strip('.')

        # Skip blacklisted patterns
        if any(re.search(pat, email) for pat in EMAIL_BLACKLIST_PATTERNS):
            continue

        # Skip very long emails (likely garbage)
        if len(email) > 60:
            continue

        # Skip emails that look like file paths
        if '/' in email or '\\' in email:
            continue

        valid_emails.append(email)

    # Deduplicate
    valid_emails = list(dict.fromkeys(valid_emails))

    # Score and sort
    def email_score(e):
        domain = e.split('@')[1] if '@' in e else ''
        score = 0
        # Prefer educational domains
        for pref in PREFERRED_EMAIL_DOMAINS:
            if domain.endswith(pref):
                score -= 100
                break
        # Deprioritize generic domains
        if domain in GENERIC_EMAIL_DOMAINS:
            score += 50
        # Prefer shorter local parts (likely official)
        local = e.split('@')[0]
        if any(kw in local for kw in ['info', 'contact', 'office', 'admin', 'principal', 'registrar']):
            score -= 50
        return score

    valid_emails.sort(key=email_score)
    return valid_emails[:5]


# ─── Principal Name Extraction ────────────────────────────────────

PRINCIPAL_KEYWORDS = [
    'principal', 'director', 'head of institution', 'head of the institution',
    'chairman', 'chairperson', 'dean', 'vice-chancellor', 'vice chancellor',
    'president', 'chief executive', 'superintendent',
]

TITLE_PREFIXES = [
    r"(?:Dr|Prof|Shri|Smt|Sri|Mr|Mrs|Ms|Thiru|Selvi|Er|CA|Adv)\.?\s*",
]

NAME_PATTERN = re.compile(
    r'(?:' + '|'.join(TITLE_PREFIXES) + r')?' +
    r'([A-Z][a-zA-Z\.\s]{2,40})',
    re.MULTILINE
)


def extract_principal(text):
    """
    Extract principal/director name from text.
    Looks for keywords and extracts the nearby name.
    """
    if not text:
        return "Not Found"

    text_lower = text.lower()

    # Strategy 1: Look for "Principal: Name" or "Principal - Name" patterns
    for keyword in PRINCIPAL_KEYWORDS:
        # Pattern: keyword followed by name on same line or nearby
        patterns = [
            # "Principal: Dr. John Smith"
            rf'{keyword}\s*[:\-–—]\s*((?:(?:Dr|Prof|Shri|Smt|Sri|Mr|Mrs|Ms|Er)\.?\s+)?[A-Z][a-zA-Z\.\s]{{2,40}})',
            # "Principal Dr. John Smith"
            rf'{keyword}\s+((?:(?:Dr|Prof|Shri|Smt|Sri|Mr|Mrs|Ms|Er)\.?\s+)?[A-Z][a-zA-Z\.\s]{{2,40}})',
            # "Dr. John Smith, Principal"
            rf'((?:(?:Dr|Prof|Shri|Smt|Sri|Mr|Mrs|Ms|Er)\.?\s+)?[A-Z][a-zA-Z\.\s]{{2,40}})\s*[,\-–—]\s*{keyword}',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                name = clean_name(match)
                if is_valid_name(name):
                    return name

    # Strategy 2: Look for title prefixes near principal keywords
    for keyword in PRINCIPAL_KEYWORDS:
        # Find position of keyword
        pos = text_lower.find(keyword)
        if pos == -1:
            continue

        # Extract surrounding context (200 chars after keyword)
        context = text[pos:pos + 200]

        # Look for names with titles in context
        title_pattern = re.compile(
            r'(?:Dr|Prof|Shri|Smt|Sri|Mr|Mrs|Ms|Er)\.?\s+([A-Z][a-zA-Z\.\s]{2,40})',
            re.MULTILINE
        )
        matches = title_pattern.findall(context)
        for match in matches:
            name = clean_name(match)
            if is_valid_name(name):
                # Add back the title
                title_match = re.search(r'((?:Dr|Prof|Shri|Smt|Sri|Mr|Mrs|Ms|Er)\.?)\s+' + re.escape(match.strip()[:10]), context)
                if title_match:
                    return f"{title_match.group(1)}. {name}" if not title_match.group(1).endswith('.') else f"{title_match.group(1)} {name}"
                return name

    # Strategy 3: Look for "Message from Principal" sections
    message_patterns = [
        r"(?:message|desk|words?)\s+(?:from|of)\s+(?:the\s+)?(?:principal|director)[^.]*?(?:Dr|Prof|Shri|Smt|Sri|Mr|Mrs|Ms|Er)\.?\s+([A-Z][a-zA-Z\.\s]{2,40})",
    ]
    for pattern in message_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            name = clean_name(match)
            if is_valid_name(name):
                return name

    return "Not Found"


def clean_name(name):
    """Clean extracted name — remove extra whitespace, trailing junk."""
    if not name:
        return ""
    name = name.strip()
    # Remove trailing punctuation
    name = re.sub(r'[\.,;:\-–—]+$', '', name).strip()
    # Remove common trailing words
    trailing = ['is', 'has', 'was', 'the', 'and', 'for', 'with', 'from', 'our', 'who']
    words = name.split()
    while words and words[-1].lower() in trailing:
        words.pop()
    name = ' '.join(words)
    # Collapse whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def is_valid_name(name):
    """Check if extracted name looks like a real person name."""
    if not name:
        return False
    if len(name) < 3 or len(name) > 50:
        return False

    words = name.split()
    if len(words) < 1 or len(words) > 6:
        return False

    # Must start with uppercase
    if not name[0].isupper():
        return False

    # Should not contain digits
    if re.search(r'\d', name):
        return False

    # Should not be common words
    common_words = ['college', 'university', 'school', 'institute', 'education',
                    'department', 'more', 'click', 'here', 'read', 'view', 'about',
                    'contact', 'home', 'page', 'welcome', 'login', 'menu']
    if name.lower() in common_words or all(w.lower() in common_words for w in words):
        return False

    return True


# ─── Status Classification ───────────────────────────────────────

def classify_status(phone, email, website_reachable):
    """
    Classify college status based on extracted data.
    Returns "Active", "Inactive", or "Not Found".
    """
    has_phone = phone and phone != "Not Found"
    has_email = email and email != "Not Found"

    if (has_phone or has_email) and website_reachable:
        return "Active"
    elif website_reachable and not has_phone and not has_email:
        return "Inactive"
    elif has_phone or has_email:
        return "Active"  # Found contact info even without website
    else:
        return "Not Found"


# ─── Master Extraction ───────────────────────────────────────────

def extract_all(texts):
    """
    Run all extractors on a list of page texts.
    Returns dict with best phone, email, principal, and raw lists.
    """
    all_phones = []
    all_emails = []
    principal = "Not Found"

    combined_text = " ".join(texts) if texts else ""

    # Extract from each page text
    for text in texts:
        phones = extract_phones(text)
        emails = extract_emails(text)
        all_phones.extend(phones)
        all_emails.extend(emails)

    # Extract principal from combined text (better context)
    principal = extract_principal(combined_text)

    # Deduplicate
    seen_phones = set()
    unique_phones = []
    for p in all_phones:
        norm = re.sub(r'[\s.\-\(\)\+]', '', p)[-10:]
        if norm not in seen_phones:
            seen_phones.add(norm)
            unique_phones.append(p)

    unique_emails = list(dict.fromkeys(all_emails))

    # Re-sort emails
    unique_emails = extract_emails(combined_text) if not unique_emails else unique_emails

    return {
        'phone': unique_phones[0] if unique_phones else "Not Found",
        'email': unique_emails[0] if unique_emails else "Not Found",
        'principal': principal,
        'all_phones': unique_phones[:5],
        'all_emails': unique_emails[:5],
    }

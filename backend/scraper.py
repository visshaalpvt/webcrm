"""
Web scraper module for College Data Enrichment CRM.
Handles website fetching with stealth headers, contact page discovery, and rate limiting.
"""

import requests
import time
import random
import re
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# ─── User-Agent Pool ─────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0",
]

# Common contact page paths to check
CONTACT_PAGE_PATHS = [
    '/contact', '/contact-us', '/contact.html', '/contact-us.html', '/contactus',
    '/about', '/about-us', '/about.html', '/about-us.html', '/aboutus',
    '/faculty', '/faculty.html', '/administration', '/administration.html',
    '/staff', '/people', '/leadership', '/management',
    '/reach-us', '/reach', '/get-in-touch',
]

# ─── Stealth Request Session ─────────────────────────────────────

def get_session():
    """Create a requests session with stealth headers."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    })
    return session


def normalize_url(url):
    """Normalize a URL — add https:// if missing, strip trailing slash."""
    if not url:
        return None
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    # Remove trailing slash
    url = url.rstrip('/')
    return url


def is_valid_url(url):
    """Check if URL is structurally valid."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def check_robots_txt(base_url, session=None):
    """Check robots.txt — returns True if scraping is allowed."""
    if not session:
        session = get_session()
    try:
        robots_url = urljoin(base_url, '/robots.txt')
        resp = session.get(robots_url, timeout=5)
        if resp.status_code == 200:
            text = resp.text.lower()
            # Very basic check — if Disallow: / is present, be cautious
            if 'disallow: /' in text and 'disallow: /\n' not in text:
                return False
        return True
    except Exception:
        return True  # If we can't reach robots.txt, assume OK


def fetch_page(url, session=None, timeout=8, retries=2):
    """
    Fetch a web page with retries and error handling.
    Returns (html_content, final_url, error_message).
    """
    if not session:
        session = get_session()

    url = normalize_url(url)
    if not url or not is_valid_url(url):
        return None, url, "Invalid URL"

    for attempt in range(retries + 1):
        try:
            # Rotate user agent on retry
            if attempt > 0:
                session.headers['User-Agent'] = random.choice(USER_AGENTS)
                time.sleep(random.uniform(2, 5))

            resp = session.get(url, timeout=timeout, allow_redirects=True, verify=False)
            resp.raise_for_status()

            # Try to detect encoding properly
            if resp.encoding and resp.encoding.lower() == 'iso-8859-1':
                resp.encoding = resp.apparent_encoding

            return resp.text, resp.url, None

        except requests.exceptions.Timeout:
            if attempt < retries:
                continue
            return None, url, "Timeout after retries"

        except requests.exceptions.ConnectionError:
            if attempt < retries:
                continue
            return None, url, "Connection refused"

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else 0
            if status_code == 403:
                return None, url, "Access forbidden (403)"
            elif status_code == 429:
                # Rate limited — wait and retry
                if attempt < retries:
                    time.sleep(30)
                    continue
                return None, url, "Rate limited (429)"
            return None, url, f"HTTP error {status_code}"

        except Exception as e:
            return None, url, str(e)[:200]

    return None, url, "Max retries exceeded"


def detect_captcha(html):
    """Check if page contains CAPTCHA."""
    if not html:
        return False
    captcha_indicators = [
        'captcha', 'recaptcha', 'g-recaptcha', 'h-captcha', 'hcaptcha',
        'cf-challenge', 'challenge-form', 'bot-detection'
    ]
    html_lower = html.lower()
    return any(indicator in html_lower for indicator in captcha_indicators)


def parse_html(html):
    """Parse HTML and return BeautifulSoup object."""
    if not html:
        return None
    try:
        return BeautifulSoup(html, 'lxml')
    except Exception:
        try:
            return BeautifulSoup(html, 'html5lib')
        except Exception:
            return BeautifulSoup(html, 'html.parser')


def get_page_text(soup):
    """Extract visible text from a BeautifulSoup object."""
    if not soup:
        return ""
    # Remove script and style elements
    for element in soup(['script', 'style', 'nav', 'footer', 'header', 'noscript']):
        element.decompose()
    text = soup.get_text(separator=' ', strip=True)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text)
    return text


def discover_contact_pages(base_url, soup=None, session=None):
    """
    Discover contact/about/faculty pages from a website.
    Returns list of URLs to scrape.
    """
    urls_to_scrape = []
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc

    # 1. Try common paths
    for path in CONTACT_PAGE_PATHS:
        full_url = urljoin(base_url, path)
        urls_to_scrape.append(full_url)

    # 2. If we have a soup, scan links for relevant pages
    if soup:
        contact_keywords = [
            'contact', 'about', 'faculty', 'administration', 'staff',
            'reach', 'principal', 'director', 'leadership', 'management',
            'people', 'team', 'phone', 'email'
        ]
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            link_text = link.get_text(strip=True).lower()
            href_lower = href.lower()

            # Check if link text or href contains contact keywords
            if any(kw in href_lower or kw in link_text for kw in contact_keywords):
                full_url = urljoin(base_url, href)
                # Only keep same-domain links
                if urlparse(full_url).netloc == base_domain:
                    urls_to_scrape.append(full_url)

    # Deduplicate while preserving order
    seen = set()
    unique_urls = []
    for url in urls_to_scrape:
        normalized = url.rstrip('/').lower()
        if normalized not in seen:
            seen.add(normalized)
            unique_urls.append(url)

    return unique_urls[:20]  # Max 20 pages


def scrape_college_website(website_url, session=None, delay_range=(1, 2)):
    """
    Scrape a college website — homepage + discovered contact pages.
    Returns dict with:
        - texts: list of page texts
        - htmls: list of raw HTML
        - pages_scraped: count
        - errors: list of error messages
        - website_reachable: bool
    """
    if not session:
        session = get_session()

    result = {
        'texts': [],
        'htmls': [],
        'pages_scraped': 0,
        'errors': [],
        'website_reachable': False,
        'final_url': None,
    }

    website_url = normalize_url(website_url)
    if not website_url:
        result['errors'].append("No URL provided")
        return result

    # 1. Fetch homepage
    html, final_url, error = fetch_page(website_url, session)
    result['final_url'] = final_url

    if error:
        result['errors'].append(f"Homepage: {error}")
        return result

    if detect_captcha(html):
        result['errors'].append("CAPTCHA detected on homepage")
        return result

    result['website_reachable'] = True
    soup = parse_html(html)
    page_text = get_page_text(soup)

    if page_text:
        result['texts'].append(page_text)
        result['htmls'].append(html)
        result['pages_scraped'] += 1

    # 2. Discover and scrape contact pages
    contact_urls = discover_contact_pages(website_url, soup, session)

    for url in contact_urls:
        # Don't re-scrape the homepage
        if url.rstrip('/').lower() == website_url.rstrip('/').lower():
            continue
        if url.rstrip('/').lower() == (final_url or '').rstrip('/').lower():
            continue

        # Rate limit
        time.sleep(random.uniform(*delay_range))

        page_html, _, page_error = fetch_page(url, session, timeout=5, retries=1)

        if page_error:
            # Don't log 404s for common paths — that's expected
            if '404' not in str(page_error):
                result['errors'].append(f"{url}: {page_error}")
            continue

        if detect_captcha(page_html):
            result['errors'].append(f"CAPTCHA at {url}")
            continue

        page_soup = parse_html(page_html)
        text = get_page_text(page_soup)

        if text and len(text) > 50:  # Skip near-empty pages
            result['texts'].append(text)
            result['htmls'].append(page_html)
            result['pages_scraped'] += 1

        # Stop if we have enough text
        if result['pages_scraped'] >= 10:
            break

    return result

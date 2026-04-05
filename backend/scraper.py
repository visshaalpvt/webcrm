import asyncio
import aiohttp
import random
import re
import time
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ─── Resilience State ──────────────────────────────────────────────
domain_failures = {}      # domain -> consecutive failure count
domain_cooldown = {}      # domain -> timestamp when it can be retried

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

CONTACT_PAGE_PATHS = [
    '/contact', '/contact-us', '/contact.html', '/contact-us.html', '/contactus',
    '/about', '/about-us', '/about.html', '/about-us.html', '/aboutus',
    '/faculty', '/staff', '/reach-us', '/get-in-touch',
]

def normalize_url(url):
    if not url: return None
    url = url.strip()
    if not url.startswith(('http://', 'https://')): url = 'https://' + url
    return url.rstrip('/')

def is_valid_url(url):
    try:
        res = urlparse(url)
        return all([res.scheme, res.netloc])
    except: return False

def get_domain(url):
    try: return urlparse(url).netloc.lower()
    except: return ""

# ─── Resilient Fetching ───────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    reraise=True
)
async def fetch_page_resilient(url, session, timeout=10):
    """Fetch with exponential backoff and retry logic."""
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout), 
                              headers=headers, allow_redirects=True, ssl=False) as resp:
            
            # Rate limiting check
            if resp.status == 429:
                # Wait and let tenacity retry
                await asyncio.sleep(5)
                raise aiohttp.ClientError(f"Status 429: Rate limited")
            
            if resp.status >= 500:
                raise aiohttp.ClientError(f"Status {resp.status}: Server error")
                
            resp.raise_for_status()
            return await resp.text(), str(resp.url), None
            
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        # Tenacity will handle the catch and retry
        raise

async def fetch_page(url, session, timeout=10, retries=1):
    """Wrapper that calls the resilient fetcher and handles the final failure."""
    domain = get_domain(url)
    
    # Circuit Breaker check
    if domain in domain_cooldown:
        if time.time() < domain_cooldown[domain]:
            return None, url, "Domain on cooldown (Circuit Open)"
            
    try:
        text, final_url, err = await fetch_page_resilient(url, session, timeout=timeout)
        # Clear failure count on success
        domain_failures[domain] = 0
        return text, final_url, err
    except Exception as e:
        # Record failure for circuit breaker
        failures = domain_failures.get(domain, 0) + 1
        domain_failures[domain] = failures
        
        if failures >= 3:
            # Trip the circuit for 1 minute
            domain_cooldown[domain] = time.time() + 60
            return None, url, f"Connection interrupted ({str(e)[:50]}) - Circuit Tripped"
            
        return None, url, f"Connection interrupted ({str(e)[:50]})"

# ─── Content Helpers ──────────────────────────────────────────────

def detect_captcha(html):
    if not html: return False
    inds = ['captcha', 'recaptcha', 'g-recaptcha', 'h-captcha', 'cf-challenge', 'bot-detection']
    return any(i in html.lower() for i in inds)

def parse_html(html):
    if not html: return None
    for parser in ['lxml', 'html.parser']:
        try: return BeautifulSoup(html, parser)
        except: continue
    return None

def get_page_text(soup):
    if not soup: return ""
    for el in soup(['script', 'style', 'nav', 'footer', 'header', 'noscript']):
        el.decompose()
    return re.sub(r'\s+', ' ', soup.get_text(separator=' ', strip=True))

def discover_contact_pages(base_url, soup=None):
    urls = [urljoin(base_url, p) for p in CONTACT_PAGE_PATHS]
    domain = get_domain(base_url)
    if soup:
        kws = ['contact', 'about', 'faculty', 'administration', 'staff', 'reach', 'principal', 'leadership', 'team', 'phone', 'email']
        for a in soup.find_all('a', href=True):
            href = a.get('href', '').lower()
            text = a.get_text(strip=True).lower()
            if any(kw in href or kw in text for kw in kws):
                full = urljoin(base_url, a.get('href'))
                if get_domain(full) == domain:
                    urls.append(full)
    
    res, seen = [], set()
    for u in urls:
        norm = u.rstrip('/').lower()
        if norm not in seen:
            seen.add(norm); res.append(u)
    return res[:15]

# ─── Scraper Engine ───────────────────────────────────────────────

async def scrape_college_website(website_url, session):
    res = {'texts': [], 'htmls': [], 'pages_scraped': 0, 'errors': [], 'website_reachable': False, 'final_url': None}
    website_url = normalize_url(website_url)
    if not website_url: return res

    # 1. Homepage
    html, final_url, err = await fetch_page(website_url, session)
    res['final_url'] = final_url
    if err:
        res['errors'].append(f"Homepage: {err}")
        return res
    if detect_captcha(html):
        res['errors'].append("CAPTCHA detected")
        return res

    res['website_reachable'] = True
    soup = parse_html(html)
    txt = get_page_text(soup)
    if txt:
        res['texts'].append(txt)
        res['htmls'].append(html)
        res['pages_scraped'] += 1

    # 2. Contact Pages
    contact_urls = discover_contact_pages(final_url or website_url, soup)
    async def fetch_one(u):
        h, _, e = await fetch_page(u, session, timeout=5)
        if e or detect_captcha(h): return None
        s = parse_html(h)
        t = get_page_text(s)
        return (t, h) if t and len(t) > 50 else None

    tasks = [asyncio.create_task(fetch_one(u)) for u in contact_urls 
             if u.rstrip('/').lower() != (final_url or website_url).rstrip('/').lower()]
    
    for fut in asyncio.as_completed(tasks):
        r = await fut
        if r:
            res['texts'].append(r[0])
            res['htmls'].append(r[1])
            res['pages_scraped'] += 1
            if res['pages_scraped'] >= 8: break

    return res

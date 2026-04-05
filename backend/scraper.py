import asyncio
import aiohttp
import random
import re
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
]

CONTACT_PAGE_PATHS = [
    '/contact', '/contact-us', '/contact.html', '/contact-us.html', '/contactus',
    '/about', '/about-us', '/about.html', '/about-us.html', '/aboutus',
    '/faculty', '/faculty.html', '/administration', '/administration.html',
    '/staff', '/people', '/leadership', '/management',
    '/reach-us', '/reach', '/get-in-touch',
]

def normalize_url(url):
    if not url: return None
    url = url.strip()
    if not url.startswith(('http://', 'https://')): url = 'https://' + url
    return url.rstrip('/')

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

async def fetch_page(url, session, timeout=8, retries=1):
    url = normalize_url(url)
    if not url or not is_valid_url(url):
        return None, url, "Invalid URL"

    for attempt in range(retries + 1):
        try:
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'DNT': '1',
            }
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout), headers=headers, allow_redirects=True, ssl=False) as resp:
                resp.raise_for_status()
                text = await resp.text()
                return text, str(resp.url), None
        except asyncio.TimeoutError:
            if attempt < retries: continue
            return None, url, "Timeout"
        except Exception as e:
            if attempt < retries: continue
            return None, url, str(e)[:200]
    return None, url, "Max retries exceeded"

def detect_captcha(html):
    if not html: return False
    captcha_indicators = ['captcha', 'recaptcha', 'g-recaptcha', 'h-captcha', 'hcaptcha', 'cf-challenge', 'bot-detection']
    html_lower = html.lower()
    return any(indicator in html_lower for indicator in captcha_indicators)

def parse_html(html):
    if not html: return None
    try: return BeautifulSoup(html, 'lxml')
    except Exception:
        try: return BeautifulSoup(html, 'html5lib')
        except Exception: return BeautifulSoup(html, 'html.parser')

def get_page_text(soup):
    if not soup: return ""
    for element in soup(['script', 'style', 'nav', 'footer', 'header', 'noscript']):
        element.decompose()
    text = soup.get_text(separator=' ', strip=True)
    return re.sub(r'\s+', ' ', text)

def discover_contact_pages(base_url, soup=None):
    urls_to_scrape = [urljoin(base_url, path) for path in CONTACT_PAGE_PATHS]
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc
    
    if soup:
        contact_keywords = ['contact', 'about', 'faculty', 'administration', 'staff', 'reach', 'principal', 'leadership', 'management', 'people', 'team', 'phone', 'email']
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            link_text = link.get_text(strip=True).lower()
            if any(kw in href or kw in link_text for kw in contact_keywords):
                full_url = urljoin(base_url, link.get('href'))
                if urlparse(full_url).netloc == base_domain:
                    urls_to_scrape.append(full_url)
                    
    seen = set()
    unique_urls = []
    for url in urls_to_scrape:
        normalized = url.rstrip('/').lower()
        if normalized not in seen:
            seen.add(normalized)
            unique_urls.append(url)
    return unique_urls[:20]

async def scrape_college_website(website_url, session):
    result = {'texts': [], 'htmls': [], 'pages_scraped': 0, 'errors': [], 'website_reachable': False, 'final_url': None}
    website_url = normalize_url(website_url)
    if not website_url: return result

    html, final_url, error = await fetch_page(website_url, session)
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

    contact_urls = discover_contact_pages(final_url or website_url, soup)

    async def fetch_contact(url):
        page_html, _, page_error = await fetch_page(url, session, timeout=5, retries=0)
        if page_error or detect_captcha(page_html): return None
        page_soup = parse_html(page_html)
        text = get_page_text(page_soup)
        if text and len(text) > 50:
            return text, page_html
        return None

    tasks = []
    for url in contact_urls:
        if url.rstrip('/').lower() == (final_url or website_url).rstrip('/').lower():
            continue
        tasks.append(asyncio.create_task(fetch_contact(url)))
        
    responses = await asyncio.gather(*tasks)
    
    for res in responses:
        if res:
            result['texts'].append(res[0])
            result['htmls'].append(res[1])
            result['pages_scraped'] += 1
            if result['pages_scraped'] >= 10:
                break

    return result

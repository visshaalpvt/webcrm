"""
Search utilities for College Data Enrichment CRM.
Provides Google and Bing search fallbacks for finding college websites.
"""

import re
import time
import random
import requests
from urllib.parse import quote_plus, urlparse
from bs4 import BeautifulSoup

# ─── Domain Scoring ──────────────────────────────────────────────

PREFERRED_TLDS = ['.edu.in', '.ac.in', '.edu', '.org.in', '.gov.in', '.nic.in']
BLOCKED_DOMAINS = [
    'wikipedia.org', 'facebook.com', 'twitter.com', 'instagram.com',
    'linkedin.com', 'youtube.com', 'quora.com', 'reddit.com',
    'shiksha.com', 'collegedunia.com', 'careers360.com', 'justdial.com',
    'indiatoday.in', 'ndtv.com', 'getmyuni.com', 'collegedekho.com',
    'google.com', 'bing.com', 'yahoo.com'
]


def score_domain(url, college_name=""):
    """Score how likely a URL is to be an official college website."""
    if not url:
        return -1

    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    score = 0

    # Block known aggregators
    for blocked in BLOCKED_DOMAINS:
        if blocked in domain:
            return -100

    # Prefer educational TLDs
    for tld in PREFERRED_TLDS:
        if domain.endswith(tld):
            score += 50
            break

    # Prefer shorter domains (more likely to be official)
    if len(domain) < 30:
        score += 10

    # Check if college name words appear in domain
    if college_name:
        name_words = re.findall(r'\w+', college_name.lower())
        for word in name_words:
            if len(word) > 3 and word in domain:
                score += 20

    # Prefer .in domains for Indian colleges
    if domain.endswith('.in'):
        score += 15

    return score


def filter_and_rank_results(urls, college_name=""):
    """Filter out non-official sites and rank by relevance."""
    scored = [(url, score_domain(url, college_name)) for url in urls]
    # Remove blocked domains
    scored = [(url, s) for url, s in scored if s > -100]
    # Sort by score descending
    scored.sort(key=lambda x: x[1], reverse=True)
    return [url for url, _ in scored]


# ─── Google Search ────────────────────────────────────────────────

def google_search(query, num_results=5):
    """
    Search Google using googlesearch-python.
    Returns list of URLs.
    """
    try:
        from googlesearch import search
        results = list(search(query, num_results=num_results, sleep_interval=2))
        return results
    except ImportError:
        return []
    except Exception as e:
        # Google might block us — that's OK
        return []


# ─── Bing Search (Fallback) ──────────────────────────────────────

def bing_search(query, num_results=5):
    """
    Search Bing by scraping search results page.
    Returns list of URLs.
    """
    try:
        encoded_query = quote_plus(query)
        url = f"https://www.bing.com/search?q={encoded_query}&count={num_results}"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, 'lxml')
        results = []

        # Bing search results are in <li class="b_algo">
        for item in soup.find_all('li', class_='b_algo'):
            link = item.find('a', href=True)
            if link and link['href'].startswith('http'):
                results.append(link['href'])
                if len(results) >= num_results:
                    break

        return results

    except Exception as e:
        return []


# ─── DuckDuckGo Search (Extra Fallback) ──────────────────────────

def duckduckgo_search(query, num_results=5):
    """
    Search DuckDuckGo HTML version as another fallback.
    Returns list of URLs.
    """
    try:
        encoded_query = quote_plus(query)
        url = f"https://html.duckduckgo.com/html/?q={encoded_query}"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        }

        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, 'lxml')
        results = []

        for link in soup.find_all('a', class_='result__a', href=True):
            href = link['href']
            if href.startswith('http'):
                results.append(href)
                if len(results) >= num_results:
                    break

        return results

    except Exception:
        return []


# ─── Main Search Function ────────────────────────────────────────

def find_college_website(college_name, state="", district=""):
    """
    Find the official website for a college using multiple search engines.
    Returns (best_url, search_method) or (None, 'none').
    """
    # Build search queries
    base_query = f"{college_name}"
    if state:
        base_query += f" {state}"
    if district:
        base_query += f" {district}"

    queries = [
        f"{base_query} official website",
        f"{base_query} college contact",
    ]

    # Try Google first
    for query in queries:
        results = google_search(query, num_results=5)
        if results:
            ranked = filter_and_rank_results(results, college_name)
            if ranked:
                return ranked[0], 'google'
        time.sleep(random.uniform(2, 4))

    # Try Bing
    for query in queries:
        results = bing_search(query, num_results=5)
        if results:
            ranked = filter_and_rank_results(results, college_name)
            if ranked:
                return ranked[0], 'bing'
        time.sleep(random.uniform(1, 3))

    # Try DuckDuckGo
    for query in queries[:1]:  # Only first query for DDG
        results = duckduckgo_search(query, num_results=5)
        if results:
            ranked = filter_and_rank_results(results, college_name)
            if ranked:
                return ranked[0], 'duckduckgo'

    return None, 'none'

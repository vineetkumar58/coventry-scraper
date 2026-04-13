"""
Coventry University Course Scraper
====================================
Discovers course URLs by crawling the official A-Z postgraduate listing at
https://www.coventry.ac.uk/study-at-coventry/postgraduate-study/az-course-list/
then scrapes structured data for the first 5 unique course pages found.

Data source : https://www.coventry.ac.uk/  (ONLY — no third-party sites)
Output file : coventry_courses.json
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
import logging
from typing import Optional


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


BASE_URL        = "https://www.coventry.ac.uk"
UNIVERSITY_NAME = "Coventry University"
COUNTRY         = "United Kingdom"
OUTPUT_FILE     = "coventry_courses.json"
MAX_COURSES     = 5
CRAWL_DELAY     = 1.5   # seconds between requests (polite crawling)

# Starting points for URL discovery
LISTING_URLS = [
    "https://www.coventry.ac.uk/study-at-coventry/postgraduate-study/az-course-list/",
    "https://www.coventry.ac.uk/study-at-coventry/undergraduate-study/course-finder/",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Pattern that all Coventry course pages share in their URL path
COURSE_URL_PATTERN = re.compile(
    r"https://www\.coventry\.ac\.uk/course-structure/(ug|pg)/[^/]+/[^/?#]+"
)



def fetch_page(url: str, retries: int = 3, delay: float = 2.0) -> Optional[BeautifulSoup]:
    """Fetch *url* and return a BeautifulSoup, or None after *retries* failures."""
    for attempt in range(1, retries + 1):
        try:
            log.info(f"  [attempt {attempt}] GET {url}")
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as exc:
            log.warning(f"    failed ({exc}); retrying in {delay}s ...")
            time.sleep(delay)
    log.error(f"  All {retries} attempts failed for: {url}")
    return None



def discover_course_urls(max_courses: int = MAX_COURSES) -> list:
    
    discovered = []
    seen = set()

    for listing_url in LISTING_URLS:
        if len(discovered) >= max_courses:
            break

        log.info(f"Crawling listing page: {listing_url}")
        soup = fetch_page(listing_url)
        if soup is None:
            continue

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()

            # Make relative URLs absolute
            if href.startswith("/"):
                href = BASE_URL + href

            # Strip query parameters to get canonical course URL
            href_clean = href.split("?")[0].rstrip("/")

            if not COURSE_URL_PATTERN.match(href_clean):
                continue

            # Canonicalise: add trailing slash
            canonical = href_clean + "/"

            if canonical in seen:
                continue
            seen.add(canonical)

            # Add ?term=2025-26 so we always hit the current intake page
            full_url = canonical + "?term=2025-26"
            discovered.append(full_url)
            log.info(f"  Found course URL: {full_url}")

            if len(discovered) >= max_courses:
                break

        time.sleep(CRAWL_DELAY)

    if len(discovered) < max_courses:
        log.warning(
            f"Only found {len(discovered)} course URLs "
            f"(wanted {max_courses}). Listing pages may have changed."
        )

    return discovered[:max_courses]



def clean(text):
    """Normalise whitespace; return 'NA' for empty/None."""
    if not text:
        return "NA"
    stripped = " ".join(text.split())
    return stripped if stripped else "NA"


def find_label_value(soup, label_text):
    """
    Search the page for a visible label that contains *label_text*
    (case-insensitive) and return the immediately adjacent value text.
    Handles <dt>/<dd>, <th>/<td>, and <h3>/<p> patterns.
    """
    label_lower = label_text.lower()
    for tag in soup.find_all(["dt", "th", "h3", "h4", "strong", "span", "p"]):
        if label_lower not in tag.get_text().lower():
            continue
        # <dt> -> <dd>
        if tag.name == "dt":
            dd = tag.find_next_sibling("dd")
            if dd:
                return clean(dd.get_text())
        # <th> -> <td>
        if tag.name == "th":
            td = tag.find_next_sibling("td")
            if td:
                return clean(td.get_text())
        # Generic: sibling tag
        sib = tag.find_next_sibling()
        if sib:
            val = clean(sib.get_text())
            if val and val.lower() != label_lower:
                return val
        # Parent text minus the label
        parent = tag.parent
        if parent:
            full = clean(parent.get_text())
            label_part = clean(tag.get_text())
            value = full.replace(label_part, "", 1).strip(" :-")
            if value:
                return value
    return "NA"


def extract_section_text(soup, heading_keywords):
    """
    Locate a heading containing any keyword in *heading_keywords*, then
    collect all sibling text until the next heading.
    Returns joined text or 'NA'.
    """
    for heading in soup.find_all(["h2", "h3", "h4"]):
        htext = heading.get_text().lower()
        if not any(kw.lower() in htext for kw in heading_keywords):
            continue
        parts = []
        for sibling in heading.find_next_siblings():
            if sibling.name in ["h2", "h3", "h4"]:
                break
            text = sibling.get_text(separator=" ", strip=True)
            if text:
                parts.append(text)
        result = " | ".join(parts)
        return clean(result) if result else "NA"
    return "NA"


def parse_course_features(soup):
    """
    Parse the structured 'Course features' block on every Coventry course page.
    Returns a dict with keys: location, study_mode, duration, start_dates, course_code
    """
    data = {k: "NA" for k in ["location", "study_mode", "duration", "start_dates", "course_code"]}
    feature_map = {
        "location":    ["location"],
        "study_mode":  ["study mode"],
        "duration":    ["duration"],
        "start_dates": ["start date"],
        "course_code": ["course code"],
    }
    for key, keywords in feature_map.items():
        for h in soup.find_all(["h3", "dt", "strong", "b"]):
            htext = h.get_text().strip().lower()
            if any(kw in htext for kw in keywords):
                sib = h.find_next_sibling()
                if sib:
                    data[key] = clean(sib.get_text())
                    break
                parent_sib = h.parent.find_next_sibling() if h.parent else None
                if parent_sib:
                    data[key] = clean(parent_sib.get_text())
                    break
    return data


def extract_fees(soup):
    """
    Extract tuition fee information from the page.
    Looks for pound (GBP) amounts near fee-related words.
    Returns raw text as scraped.
    """
    text = soup.get_text(separator=" ")

    # Strategy 1: find sentences containing a pound sign and fee-related words
    sentences = re.split(r'(?<=[.!?])\s+', text)
    fee_sentences = [
        s for s in sentences
        if (u'\u00a3' in s or '&pound;' in s.lower()) and
           any(w in s.lower() for w in ["fee", "tuition", "cost", "per year", "annual"])
    ]
    if fee_sentences:
        return clean(" | ".join(fee_sentences[:3]))

    # Strategy 2: find all raw GBP amount patterns
    raw_amounts = re.findall(r'\u00a3[\d,]+(?:\.\d{2})?(?:\s*(?:per year|a year|/year))?', text)
    if raw_amounts:
        return clean(" | ".join(list(dict.fromkeys(raw_amounts[:4]))))

    # Strategy 3: heading-anchored section
    return extract_section_text(soup, ["tuition fee", "fees", "course cost"])


def extract_english_req(soup, test_name):
    """
    Return a short context string containing the minimum score for *test_name*.
    Returns 'NA' if not found.
    """
    text = soup.get_text(separator=" ")
    pattern = re.compile(
        r'{}[^.\n]{{0,120}}(\d+\.?\d*)'.format(re.escape(test_name)),
        re.IGNORECASE
    )
    m = pattern.search(text)
    if m:
        start = max(0, m.start() - 5)
        end   = min(len(text), m.end() + 60)
        return clean(text[start:end])
    return "NA"


CAMPUS_MAP = {
    "coventry university (coventry)": {
        "campus": "Coventry",
        "address": "Priory Street, Coventry, CV1 5FB, United Kingdom",
    },
    "coventry university (vauxhall": {
        "campus": "London (Vauxhall)",
        "address": "Mile Lane, Coventry / Vauxhall, London Campus, United Kingdom",
    },
    "coventry university london": {
        "campus": "London",
        "address": "1 Goswell Road, London, EC1V 7DD, United Kingdom",
    },
    "cu coventry": {
        "campus": "CU Coventry",
        "address": "Priory Street, Coventry, CV1 5FB, United Kingdom",
    },
    "cu scarborough": {
        "campus": "CU Scarborough",
        "address": "Ashburn Road, Scarborough, YO11 2JW, United Kingdom",
    },
}


def resolve_campus(location_raw):
    """Map a raw location string to (campus_name, postal_address)."""
    key = location_raw.lower()
    for pattern, info in CAMPUS_MAP.items():
        if pattern in key:
            return info["campus"], info["address"]
    return clean(location_raw) or "Coventry", \
           "Priory Street, Coventry, CV1 5FB, United Kingdom"

# ── Step 3 — Per-course extractor ────────────────────────────────────────────

EMPTY_RECORD_KEYS = [
    "program_course_name", "university_name", "course_website_url",
    "campus", "country", "address", "study_level", "course_duration",
    "all_intakes_available", "mandatory_documents_required",
    "yearly_tuition_fee", "scholarship_availability",
    "gre_gmat_mandatory_min_score", "indian_regional_institution_restrictions",
    "class_12_boards_accepted", "gap_year_max_accepted",
    "min_duolingo", "english_waiver_class12", "english_waiver_moi",
    "min_ielts", "kaplan_test_of_english", "min_pte", "min_toefl",
    "ug_academic_min_gpa", "twelfth_pass_min_cgpa",
    "mandatory_work_exp", "max_backlogs",
]


def extract_course_data(url):
    """
    Fetch a single Coventry course page and return a fully populated
    record dict matching the required schema.  Missing fields -> 'NA'.
    """
    record = {k: "NA" for k in EMPTY_RECORD_KEYS}
    record["course_website_url"] = url
    record["university_name"]    = UNIVERSITY_NAME
    record["country"]            = COUNTRY

    soup = fetch_page(url)
    if soup is None:
        log.error(f"  Skipping (fetch failed): {url}")
        return record

    # Course name
    h1 = soup.find("h1")
    record["program_course_name"] = clean(h1.get_text()) if h1 else "NA"

    # Study level
    level_raw = find_label_value(soup, "Study level")
    if level_raw == "NA":
        for a in soup.select("nav a, .breadcrumb a, li a"):
            t = a.get_text().lower()
            if "postgraduate" in t:
                level_raw = "Postgraduate"; break
            if "undergraduate" in t:
                level_raw = "Undergraduate"; break
        if level_raw == "NA":
            level_raw = "Postgraduate" if "/pg/" in url else \
                        "Undergraduate" if "/ug/" in url else "NA"
    record["study_level"] = level_raw

    # Course features sidebar
    features = parse_course_features(soup)
    record["course_duration"]       = features["duration"]
    record["all_intakes_available"] = features["start_dates"]
    campus, address = resolve_campus(features["location"])
    record["campus"]  = campus
    record["address"] = address

    # Tuition fees
    record["yearly_tuition_fee"] = extract_fees(soup)

    # Scholarships
    record["scholarship_availability"] = extract_section_text(
        soup, ["scholarship", "bursary", "funding", "financial support"]
    )

    # English language requirements
    record["min_ielts"]    = extract_english_req(soup, "IELTS")
    record["min_toefl"]    = extract_english_req(soup, "TOEFL")
    record["min_pte"]      = extract_english_req(soup, "PTE")
    record["min_duolingo"] = extract_english_req(soup, "Duolingo")

    # Academic entry requirements
    entry_text = extract_section_text(
        soup, ["entry requirement", "academic requirement", "what we're looking for"]
    )
    record["ug_academic_min_gpa"] = entry_text

    # Work experience
    record["mandatory_work_exp"] = extract_section_text(
        soup, ["work experience", "professional experience", "relevant experience"]
    )

    # Mandatory documents
    record["mandatory_documents_required"] = extract_section_text(
        soup, ["document", "supporting document", "what you'll need", "what to include"]
    )

    # GRE / GMAT
    gre  = extract_english_req(soup, "GRE")
    gmat = extract_english_req(soup, "GMAT")
    if gre != "NA" or gmat != "NA":
        record["gre_gmat_mandatory_min_score"] = f"GRE: {gre} | GMAT: {gmat}"

    # Fields not published by Coventry
    for field in [
        "indian_regional_institution_restrictions",
        "class_12_boards_accepted",
        "gap_year_max_accepted",
        "english_waiver_class12",
        "english_waiver_moi",
        "kaplan_test_of_english",
        "twelfth_pass_min_cgpa",
        "max_backlogs",
    ]:
        record[field] = "NA"

    log.info(f"  Extracted: {record['program_course_name']}  [{campus}]")
    return record

# ── Step 4 — Orchestrator ─────────────────────────────────────────────────────

def run_scraper(max_courses=MAX_COURSES, output_file=OUTPUT_FILE):
    """
    Full pipeline:
      1. Discover course URLs from the official listing pages.
      2. Scrape each course page for structured data.
      3. Save results as JSON.
    """
    log.info("=" * 60)
    log.info("  Coventry University Course Scraper")
    log.info("=" * 60)

    # 1. Discover URLs
    log.info(f"Step 1: Discovering {max_courses} course URLs ...")
    course_urls = discover_course_urls(max_courses)

    if not course_urls:
        log.error("No course URLs discovered. Aborting.")
        return

    log.info(f"  -> {len(course_urls)} course URL(s) found.")

    # 2. Scrape each course
    log.info("Step 2: Scraping course pages ...")
    results = []
    for i, url in enumerate(course_urls, 1):
        log.info(f"  [{i}/{len(course_urls)}] {url}")
        record = extract_course_data(url)
        results.append(record)
        time.sleep(CRAWL_DELAY)

    # 3. Save output
    log.info(f"Step 3: Saving {len(results)} records -> {output_file}")
    with open(output_file, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, ensure_ascii=False)

    log.info("=" * 60)
    log.info(f"  Done.  Output: {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    run_scraper()
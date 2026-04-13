# Coventry University Course Scraper

Extracts structured data for **5 courses** directly from the official
Coventry University website (`https://www.coventry.ac.uk/`).

No third-party sources (Shiksha, Yocket, etc.) are used. All data is
crawled and extracted only from `coventry.ac.uk`.

---

## Setup

### Requirements
- Python 3.9 or higher

### Install dependencies

```bash
pip install -r requirements.txt
```

---

## How to Run

```bash
python scraper.py
```

### What it does (3 steps automatically):

**Step 1 — Discover course URLs**
Crawls the official A-Z course listing pages:
- `https://www.coventry.ac.uk/study-at-coventry/postgraduate-study/az-course-list/`
- `https://www.coventry.ac.uk/study-at-coventry/undergraduate-study/course-finder/`

Finds all links matching the pattern `/course-structure/(ug|pg)/...` and
collects the first 5 unique course URLs. No URLs are hardcoded.

**Step 2 — Scrape each course page**
For each discovered URL, fetches the official course page and extracts
all 27 required fields.

**Step 3 — Save output**
Writes `coventry_courses.json` in the same directory — exactly 5 records.

---

## Dependencies

| Package        | Purpose          |
|----------------|------------------|
| requests       | HTTP page fetching |
| beautifulsoup4 | HTML parsing     |

```bash
pip install requests beautifulsoup4
```

---

## Output Format

**File:** `coventry_courses.json`  
**Format:** JSON array, exactly 5 objects.

### Sample record structure:

```json
{
  "program_course_name": "Data Science MSc",
  "university_name": "Coventry University",
  "course_website_url": "https://www.coventry.ac.uk/course-structure/pg/ees/data-science-msc/?term=2025-26",
  "campus": "Coventry",
  "country": "United Kingdom",
  "address": "Priory Street, Coventry, CV1 5FB, United Kingdom",
  "study_level": "Postgraduate",
  "course_duration": "1 year full-time | Up to 2 years with placement",
  "all_intakes_available": "March 2026 | May 2026 | July 2026",
  "mandatory_documents_required": "...",
  "yearly_tuition_fee": "£19,250 per year (international)",
  "scholarship_availability": "...",
  "gre_gmat_mandatory_min_score": "NA",
  "indian_regional_institution_restrictions": "NA",
  "class_12_boards_accepted": "NA",
  "gap_year_max_accepted": "NA",
  "min_duolingo": "NA",
  "english_waiver_class12": "NA",
  "english_waiver_moi": "NA",
  "min_ielts": "IELTS 6.0 overall with no component below 5.5",
  "kaplan_test_of_english": "NA",
  "min_pte": "NA",
  "min_toefl": "NA",
  "ug_academic_min_gpa": "2:2 undergraduate degree or equivalent...",
  "twelfth_pass_min_cgpa": "NA",
  "mandatory_work_exp": "NA",
  "max_backlogs": "NA"
}
```

### Field notes

| Field | Source on page |
|-------|---------------|
| `program_course_name` | `<h1>` tag |
| `course_website_url` | Final official course page URL |
| `campus` | "Location" in the Course Features block |
| `address` | Mapped from campus name |
| `study_level` | "Study level" label; fallback to breadcrumb / URL path |
| `course_duration` | "Duration" in Course Features block |
| `all_intakes_available` | "Start date" in Course Features block |
| `yearly_tuition_fee` | £ amounts found near fee-related words on page |
| `scholarship_availability` | Text under scholarship/funding headings |
| `min_ielts / min_toefl / min_pte / min_duolingo` | Regex extraction from entry requirements section |
| `ug_academic_min_gpa` | Text under "Entry requirements" heading |
| `mandatory_documents_required` | Text under "Documents" / "What you'll need" heading |
| Fields returning `"NA"` | Not published on Coventry.ac.uk (e.g. Indian board restrictions, max backlogs) |

---

## Scraper Architecture

```
scraper.py
|
+-- fetch_page()               HTTP GET with 3-attempt retry + delay
|
+-- discover_course_urls()     Crawls listing pages, returns 5 course URLs
|
+-- extract_course_data()      Master extractor for one course page
|   +-- parse_course_features()   Duration / start dates / campus sidebar
|   +-- extract_fees()            GBP amount extractor (regex + section)
|   +-- extract_english_req()     Per-test score extractor (IELTS/TOEFL/PTE)
|   +-- extract_section_text()    Heading-anchored section text
|   +-- find_label_value()        Label->value (dt/dd, th/td, strong/p)
|   +-- resolve_campus()          Raw location -> campus name + address
|
+-- run_scraper()              Orchestrates all steps, writes JSON
```

---

## Behaviour

- **Polite crawling**: 1.5-second delay between all requests
- **Retry logic**: 3 attempts per URL with 2-second backoff
- **Missing values**: all missing fields return `"NA"` — never crashes
- **No duplicates**: URL deduplication built into `discover_course_urls()`
- **No manual intervention**: fully automated end-to-end
- **Source restriction**: only `coventry.ac.uk` URLs are fetched
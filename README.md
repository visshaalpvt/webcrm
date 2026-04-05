# College Data Enrichment CRM

A production-ready web application that enriches college datasets by automatically finding missing contact numbers, email IDs, principal names, and operational status. **100% free** — no paid APIs required.

## Features

- **Drag & Drop Upload** — Upload Excel/CSV files with college data
- **Automated Scraping** — Scrapes college websites for contact info
- **Search Fallback** — Uses Google/Bing/DuckDuckGo when no website is available
- **Real-time Progress** — Live progress bar, log console, and stats via SSE
- **Smart Extraction** — Regex-based phone/email extraction + keyword-based principal name detection
- **Status Classification** — Active / Inactive / Not Found based on data availability
- **Pause/Resume/Cancel** — Full job control during processing
- **Download Results** — Export enriched data as Excel file
- **Dark/Light Theme** — Premium UI with glassmorphism design

## Quick Start

### 1. Install Python Dependencies

```bash
cd webcrm
pip install -r requirements.txt
```

### 2. Run the Server

```bash
python backend/app.py
```

### 3. Open Browser

Navigate to [http://localhost:5000](http://localhost:5000)

### 4. Upload & Process

- Upload your CSV/Excel file with college data
- Click "Start Processing"
- Watch real-time progress
- Download enriched results

## Input File Format

Your file should have these columns (at minimum `College Name` is required):

| Column | Required | Description |
|--------|----------|-------------|
| College Name | ✅ | Name of the college |
| College Type | ❌ | Government/Private/Autonomous |
| State | ❌ | State name |
| District | ❌ | District name |
| Website | ❌ | College website URL |
| Contact Number | ❌ | To be filled |
| Mail ID | ❌ | To be filled |
| Principal Name | ❌ | To be filled |
| Status | ❌ | To be filled |

## Project Structure

```
webcrm/
├── backend/
│   ├── app.py           # Flask server + API endpoints
│   ├── database.py      # SQLite database layer
│   ├── scraper.py       # Website scraping engine
│   ├── search_utils.py  # Google/Bing/DDG search fallbacks
│   └── extractor.py     # Phone/email/principal extraction
├── frontend/
│   ├── index.html       # Single-page application
│   ├── style.css        # Premium dark theme CSS
│   └── script.js        # Frontend controller
├── uploads/             # Uploaded files (auto-created)
├── outputs/             # Generated Excel files (auto-created)
├── data/                # SQLite database (auto-created)
├── requirements.txt     # Python dependencies
├── sample_colleges.csv  # Test dataset
└── README.md
```

## Tech Stack

- **Backend**: Python / Flask
- **Frontend**: HTML5 / CSS3 / JavaScript (no frameworks)
- **Scraping**: BeautifulSoup4 + requests
- **Search**: googlesearch-python / Bing / DuckDuckGo
- **Database**: SQLite
- **Real-time**: Server-Sent Events (SSE)

## License

MIT

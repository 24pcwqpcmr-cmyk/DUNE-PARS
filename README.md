# Dune Table Scraper — Chrome Extension

Fast & beautiful Chrome extension for scraping [Dune Analytics](https://dune.com) query result tables and exporting them to CSV.

![Manifest V3](https://img.shields.io/badge/Manifest-V3-blue)
![Chrome Extension](https://img.shields.io/badge/Chrome-Extension-green)

---

## Features

- **One-click scrape** — scrape the current page or all pages with a single button
- **Smart pagination** — automatically clicks through pages with randomized delays
- **Anti-block protection** — human-like behavior with random delays, scrolling, and no parallel requests
- **Beautiful dark UI** — premium interface with purple/cyan gradient theme matching Dune's aesthetic
- **CSV export** — instant download with BOM for Excel compatibility
- **Deduplication** — automatically removes duplicate rows across pages
- **Configurable delays** — adjust min/max delay between page turns
- **Custom filenames** — template-based filenames with `{query}`, `{date}`, `{rows}` placeholders
- **Progress tracking** — real-time progress bar with row count and page number
- **Lifetime stats** — tracks total exports and rows scraped across sessions
- **Data preview** — see the first few rows before downloading

---

## Installation

1. Clone or download this repository
2. Open Chrome and navigate to `chrome://extensions/`
3. Enable **Developer mode** (toggle in the top right)
4. Click **Load unpacked**
5. Select the `DUNE-PARS` folder (the one containing `manifest.json`)
6. The extension icon will appear in your toolbar

---

## Usage

1. Navigate to any Dune Analytics query page with results (e.g., `https://dune.com/queries/...`)
2. Click the **Dune Scraper** extension icon in your toolbar
3. The extension will detect the table and show column/row counts
4. Choose:
   - **This Page Only** — scrapes just the visible page
   - **All Pages → CSV** — scrapes all pages with smart pagination
5. Set **Max pages** to limit scraping (0 = unlimited)
6. Once complete, preview the data and click **Download CSV**

---

## Settings

Click the gear icon to configure:

| Setting | Default | Description |
|---------|---------|-------------|
| Min Delay | 800 ms | Minimum wait between page navigations |
| Max Delay | 2500 ms | Maximum wait between page navigations |
| Filename Template | `dune_{query}_{date}` | CSV filename pattern |

### Filename placeholders

- `{query}` — Query name from the URL
- `{date}` — Current date (YYYY-MM-DD)
- `{rows}` — Total row count

---

## Anti-Block Strategy

The extension avoids IP blocks by:

1. **No API calls** — scrapes from the existing rendered DOM, using your authenticated browser session
2. **Randomized delays** — each page turn waits a random interval between min/max delay
3. **Human-like scrolling** — occasionally scrolls the table area before clicking next
4. **Sequential navigation** — never makes parallel requests; one page at a time
5. **Stop anytime** — cancel scraping mid-way without losing collected data

---

## Project Structure

```
DUNE-PARS/
├── manifest.json           # Extension manifest (V3)
├── background/
│   └── background.js       # Service worker — handles downloads & settings
├── content/
│   ├── content.js          # Content script — table detection, scraping, pagination
│   └── content.css         # Toast notifications & table highlights
├── popup/
│   ├── popup.html          # Extension popup UI
│   ├── popup.css           # Dark theme styles
│   └── popup.js            # Popup controller logic
├── icons/
│   ├── icon16.png
│   ├── icon48.png
│   └── icon128.png
└── README.md
```

---

## License

MIT

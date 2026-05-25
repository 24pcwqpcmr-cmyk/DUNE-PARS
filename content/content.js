/**
 * Dune Table Scraper — Content Script
 * Scrapes Dune Analytics query result tables with smart pagination.
 *
 * Strategy to avoid IP blocks:
 *   - Randomized delays between page turns (configurable)
 *   - Human-like scroll behavior before clicking next
 *   - Batch processing with pauses
 *   - No parallel requests — sequential page navigation only
 *   - Respects the existing browser session (no extra API calls)
 */

(() => {
  'use strict';

  // ── State ──────────────────────────────────────────────────
  let isRunning = false;
  let shouldStop = false;
  let collectedRows = [];
  let headers = [];
  let settings = {};
  let totalRowsExpected = 0;

  // ── Utilities ──────────────────────────────────────────────

  function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  function randomDelay(min, max) {
    const delay = min + Math.random() * (max - min);
    return sleep(delay);
  }

  function humanScroll(element) {
    if (!element) return;
    const scrollAmount = 50 + Math.random() * 100;
    element.scrollBy({ top: scrollAmount, behavior: 'smooth' });
  }

  // ── Toast Notifications ────────────────────────────────────

  function showToast(title, message, progress) {
    let toast = document.querySelector('.dune-scraper-toast');
    if (!toast) {
      toast = document.createElement('div');
      toast.className = 'dune-scraper-toast';
      toast.innerHTML = `
        <div class="dune-scraper-toast-title">
          <span class="dune-scraper-toast-icon">⚡</span>
          <span class="dune-scraper-toast-title-text"></span>
        </div>
        <div class="dune-scraper-toast-message"></div>
        <div class="dune-scraper-toast-progress">
          <div class="dune-scraper-toast-progress-bar"></div>
        </div>
      `;
      document.body.appendChild(toast);
      requestAnimationFrame(() => toast.classList.add('visible'));
    }
    toast.querySelector('.dune-scraper-toast-title-text').textContent = title;
    toast.querySelector('.dune-scraper-toast-message').textContent = message || '';
    if (progress !== undefined) {
      const bar = toast.querySelector('.dune-scraper-toast-progress-bar');
      bar.style.width = Math.min(100, progress) + '%';
    }
    toast.classList.add('visible');
  }

  function hideToast() {
    const toast = document.querySelector('.dune-scraper-toast');
    if (toast) {
      toast.classList.remove('visible');
      setTimeout(() => toast.remove(), 400);
    }
  }

  // ── Table Detection ────────────────────────────────────────

  function findDuneTable() {
    // Dune renders tables inside specific containers
    // Try multiple selectors for different Dune UI versions
    const selectors = [
      'table',
      '[class*="TableContainer"] table',
      '[class*="table-container"] table',
      '[class*="ResultsTable"] table',
      '[class*="results"] table',
      '[data-testid*="table"]',
      '.table-results table',
      '[class*="QueryResults"] table'
    ];

    for (const selector of selectors) {
      const tables = document.querySelectorAll(selector);
      if (tables.length > 0) {
        // Return the largest table (most rows)
        let best = tables[0];
        for (const t of tables) {
          if (t.rows.length > best.rows.length) best = t;
        }
        return best;
      }
    }
    return null;
  }

  function extractHeaders(table) {
    const headerRow = table.querySelector('thead tr') || table.querySelector('tr');
    if (!headerRow) return [];
    return Array.from(headerRow.querySelectorAll('th, td')).map(cell => {
      return cell.textContent.trim();
    });
  }

  function extractRows(table) {
    const tbody = table.querySelector('tbody') || table;
    const rows = tbody.querySelectorAll('tr');
    const data = [];
    for (const row of rows) {
      const cells = row.querySelectorAll('td');
      if (cells.length === 0) continue;
      const rowData = Array.from(cells).map(cell => cell.textContent.trim());
      data.push(rowData);
    }
    return data;
  }

  function getTotalRows() {
    // Try to find total row count from Dune UI
    // Dune usually shows "X rows" or "X,XXX rows" somewhere
    const patterns = [
      /(\d[\d\s,\.]*)\s*rows/i,
      /(\d[\d\s,\.]*)\s*results/i,
      /showing\s+\d+.*?of\s+(\d[\d\s,\.]*)/i
    ];

    const textElements = document.querySelectorAll(
      '[class*="row"], [class*="Row"], [class*="count"], [class*="Count"], ' +
      '[class*="result"], [class*="Result"], [class*="pagination"], [class*="Pagination"], ' +
      'span, p, div'
    );

    for (const el of textElements) {
      const text = el.textContent.trim();
      if (text.length > 200) continue;
      for (const pattern of patterns) {
        const match = text.match(pattern);
        if (match) {
          const num = parseInt(match[1].replace(/[\s,\.]/g, ''), 10);
          if (num > 0 && num < 100000000) return num;
        }
      }
    }
    return 0;
  }

  // ── Pagination ─────────────────────────────────────────────

  function findNextPageButton() {
    // Try multiple strategies to find the "next page" button
    const selectors = [
      // Direct next/arrow buttons
      'button[aria-label*="next" i]',
      'button[aria-label*="Next" i]',
      'a[aria-label*="next" i]',
      '[class*="pagination"] button:last-child',
      '[class*="Pagination"] button:last-child',
      // SVG arrow buttons (Dune uses these)
      '[class*="pagination"] button svg',
      // Generic right arrow / chevron
      'button[class*="next" i]',
      'button[class*="Next" i]',
      'a[class*="next" i]',
    ];

    for (const selector of selectors) {
      const elements = document.querySelectorAll(selector);
      for (const el of elements) {
        const btn = el.closest('button') || el.closest('a') || el;
        if (btn && !btn.disabled && btn.offsetParent !== null) {
          return btn;
        }
      }
    }

    // Fallback: look for ">" or "›" text in pagination area
    const paginationArea = document.querySelector(
      '[class*="pagination" i], [class*="Pagination"]'
    );
    if (paginationArea) {
      const buttons = paginationArea.querySelectorAll('button, a');
      for (const btn of buttons) {
        const text = btn.textContent.trim();
        if (text === '>' || text === '›' || text === '→' || text === '»') {
          if (!btn.disabled) return btn;
        }
      }
      // Also try the last button as potential "next"
      const allBtns = Array.from(buttons).filter(b => !b.disabled);
      if (allBtns.length >= 2) {
        return allBtns[allBtns.length - 1];
      }
    }

    return null;
  }

  function findPaginationInfo() {
    // Try to detect current page and total pages
    const paginationArea = document.querySelector(
      '[class*="pagination" i], [class*="Pagination"]'
    );
    if (!paginationArea) return { current: 1, total: 1 };

    const text = paginationArea.textContent;

    // "1 2 ... 164690" pattern
    const pageNumbers = text.match(/\d+/g);
    if (pageNumbers && pageNumbers.length > 0) {
      const nums = pageNumbers.map(Number);
      const total = Math.max(...nums);
      // Current page is usually highlighted or active
      const activeBtn = paginationArea.querySelector(
        'button[aria-current="page"], button[class*="active" i], ' +
        'button[class*="Active"], button[class*="current" i], ' +
        'a[class*="active" i], [aria-current="true"]'
      );
      const current = activeBtn ? parseInt(activeBtn.textContent.trim(), 10) || 1 : 1;
      return { current, total };
    }

    return { current: 1, total: 1 };
  }

  function isLastPage() {
    const nextBtn = findNextPageButton();
    if (!nextBtn) return true;
    if (nextBtn.disabled) return true;
    if (nextBtn.getAttribute('aria-disabled') === 'true') return true;

    const info = findPaginationInfo();
    if (info.total > 1 && info.current >= info.total) return true;

    return false;
  }

  async function clickNextPage() {
    const btn = findNextPageButton();
    if (!btn) return false;

    // Human-like: scroll the button into view first
    btn.scrollIntoView({ behavior: 'smooth', block: 'center' });
    await sleep(200 + Math.random() * 300);

    // Click
    btn.click();

    // Wait for the table to re-render
    await sleep(500 + Math.random() * 500);

    return true;
  }

  // ── Deduplication ──────────────────────────────────────────

  function rowKey(row) {
    return row.join('|||');
  }

  // ── Main Scraping Logic ────────────────────────────────────

  async function scrapeAllPages(maxPages) {
    isRunning = true;
    shouldStop = false;
    collectedRows = [];

    const seenKeys = new Set();

    // Load settings
    try {
      settings = await new Promise(resolve => {
        chrome.runtime.sendMessage({ action: 'getSettings' }, resolve);
      });
    } catch (e) {
      settings = { minDelay: 800, maxDelay: 2500 };
    }

    const minDelay = settings.minDelay || 800;
    const maxDelay = settings.maxDelay || 2500;

    // Detect table
    const table = findDuneTable();
    if (!table) {
      showToast('No Table Found', 'Navigate to a Dune query with results and try again.');
      await sleep(3000);
      hideToast();
      isRunning = false;
      return null;
    }

    // Extract headers
    headers = extractHeaders(table);
    totalRowsExpected = getTotalRows();
    const pageInfo = findPaginationInfo();

    showToast(
      'Scraping Started',
      totalRowsExpected > 0
        ? `Found ~${totalRowsExpected.toLocaleString()} rows across ~${pageInfo.total.toLocaleString()} pages`
        : `Scanning pages... (${pageInfo.total.toLocaleString()} pages detected)`,
      0
    );

    let pageCount = 0;
    const effectiveMaxPages = maxPages || 999999;

    while (pageCount < effectiveMaxPages && !shouldStop) {
      // Scrape current page
      const currentTable = findDuneTable();
      if (!currentTable) {
        showToast('Table Lost', 'Could not find table on current page. Stopping.');
        break;
      }

      const rows = extractRows(currentTable);
      let newCount = 0;
      for (const row of rows) {
        const key = rowKey(row);
        if (!seenKeys.has(key)) {
          seenKeys.add(key);
          collectedRows.push(row);
          newCount++;
        }
      }

      pageCount++;
      const progress = totalRowsExpected > 0
        ? (collectedRows.length / totalRowsExpected) * 100
        : (pageCount / Math.max(pageInfo.total, 1)) * 100;

      showToast(
        `Page ${pageCount}`,
        `${collectedRows.length.toLocaleString()} rows collected (+${newCount} new)`,
        progress
      );

      // Check if we're on the last page
      if (isLastPage()) {
        showToast('Scraping Complete', `All ${collectedRows.length.toLocaleString()} rows collected!`, 100);
        break;
      }

      // Random delay before next page (anti-detection)
      await randomDelay(minDelay, maxDelay);

      // Optionally do a small scroll for more human-like behavior
      if (Math.random() > 0.5) {
        humanScroll(document.querySelector('[class*="table" i]') || document.documentElement);
        await sleep(100 + Math.random() * 200);
      }

      // Click next page
      const navigated = await clickNextPage();
      if (!navigated) {
        showToast('Navigation Failed', 'Could not find next page button. Stopping.');
        break;
      }

      // Wait for table to update
      await sleep(600 + Math.random() * 400);
    }

    isRunning = false;

    if (shouldStop) {
      showToast('Stopped', `Collected ${collectedRows.length.toLocaleString()} rows before stopping.`, 100);
    }

    return { headers, rows: collectedRows, pageCount };
  }

  // ── CSV Generation ─────────────────────────────────────────

  function generateCSV(headersList, rowsList) {
    function escapeCSV(value) {
      if (value == null) return '';
      const str = String(value);
      if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
        return '"' + str.replace(/"/g, '""') + '"';
      }
      return str;
    }

    const lines = [];
    if (headersList && headersList.length > 0) {
      lines.push(headersList.map(escapeCSV).join(','));
    }
    for (const row of rowsList) {
      lines.push(row.map(escapeCSV).join(','));
    }
    return lines.join('\r\n');
  }

  // ── Quick Scrape (current page only) ───────────────────────

  function scrapeCurrentPage() {
    const table = findDuneTable();
    if (!table) return null;

    const h = extractHeaders(table);
    const r = extractRows(table);
    return { headers: h, rows: r, pageCount: 1 };
  }

  // ── Message Handling ───────────────────────────────────────

  chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
    if (message.action === 'ping') {
      sendResponse({ alive: true, isRunning });
      return;
    }

    if (message.action === 'detectTable') {
      const table = findDuneTable();
      const info = {
        found: !!table,
        rowCount: 0,
        colCount: 0,
        totalRows: 0,
        pagination: { current: 1, total: 1 },
        headers: []
      };
      if (table) {
        const h = extractHeaders(table);
        const r = extractRows(table);
        info.rowCount = r.length;
        info.colCount = h.length;
        info.headers = h;
        info.totalRows = getTotalRows();
        info.pagination = findPaginationInfo();
      }
      sendResponse(info);
      return;
    }

    if (message.action === 'scrapeCurrentPage') {
      const result = scrapeCurrentPage();
      if (result) {
        const csv = generateCSV(result.headers, result.rows);
        sendResponse({ success: true, csv, rowCount: result.rows.length, headers: result.headers });
      } else {
        sendResponse({ success: false, error: 'No table found on this page.' });
      }
      return;
    }

    if (message.action === 'scrapeAllPages') {
      // Async operation — respond immediately, send result via separate message
      scrapeAllPages(message.maxPages).then(result => {
        if (result) {
          const csv = generateCSV(result.headers, result.rows);
          chrome.runtime.sendMessage({
            action: 'scrapeComplete',
            csv,
            rowCount: result.rows.length,
            pageCount: result.pageCount,
            headers: result.headers
          });
        } else {
          chrome.runtime.sendMessage({
            action: 'scrapeComplete',
            error: 'Scrape failed or no table found.'
          });
        }
        // Hide toast after a delay
        setTimeout(hideToast, 5000);
      });
      sendResponse({ started: true });
      return;
    }

    if (message.action === 'stopScraping') {
      shouldStop = true;
      sendResponse({ stopped: true });
      return;
    }

    if (message.action === 'getStatus') {
      sendResponse({
        isRunning,
        rowCount: collectedRows.length,
        totalExpected: totalRowsExpected
      });
      return;
    }
  });
})();

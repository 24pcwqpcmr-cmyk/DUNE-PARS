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

  // Install fetch interceptor early to capture execution_id from Dune's own API calls
  (function earlyInterceptor() {
    const script = document.createElement('script');
    script.textContent = `
      (function() {
        if (window.__duneScraperInterceptorInstalled) return;
        window.__duneScraperInterceptorInstalled = true;
        var origFetch = window.fetch;
        window.fetch = function() {
          var url = typeof arguments[0] === 'string' ? arguments[0] : (arguments[0] && arguments[0].url) || '';
          if (url.indexOf('core-api.dune.com/public/execution') !== -1 && arguments[1] && arguments[1].body) {
            try {
              var body = JSON.parse(arguments[1].body);
              if (body.execution_id) {
                window.__duneScraperExecId = body.execution_id;
                window.dispatchEvent(new CustomEvent('dune-exec-id', { detail: body.execution_id }));
              }
            } catch(e) {}
          }
          return origFetch.apply(this, arguments);
        };
      })();
    `;
    document.documentElement.appendChild(script);
    script.remove();

    window.addEventListener('dune-exec-id', (e) => {
      window.__duneScraperExecId = e.detail;
    });
  })();

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
    // Strategy 1: Dune uses SVG with aria-label inside button
    const svgNext = document.querySelector('svg[aria-label="Next page"], svg[aria-label="next page"]');
    if (svgNext) {
      const btn = svgNext.closest('button');
      if (btn && !btn.disabled) return btn;
    }

    // Strategy 2: Direct aria-label on button/link
    const selectors = [
      'button[aria-label*="next" i]',
      'a[aria-label*="next" i]',
      '[class*="pagination"] button:last-child',
      '[class*="Pagination"] button:last-child',
      'button[class*="next" i]',
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

    // Strategy 3: Find pagination footer and get last enabled button
    const footer = findPaginationFooter();
    if (footer) {
      const buttons = footer.querySelectorAll('button, a');
      for (const btn of buttons) {
        const text = btn.textContent.trim();
        if (text === '>' || text === '›' || text === '→' || text === '»') {
          if (!btn.disabled) return btn;
        }
      }
    }

    return null;
  }

  function findPaginationFooter() {
    // Dune uses CSS module classes like TableFooter-module__*__footer
    // Strategy 1: Find by class substring
    const byClass = document.querySelector(
      '[class*="TableFooter"], [class*="pagination" i], [class*="Pagination"]'
    );
    if (byClass) return byClass;

    // Strategy 2: Find the container that has "rows" text + page buttons
    const svgPrev = document.querySelector('svg[aria-label="Previous page"]');
    if (svgPrev) {
      let el = svgPrev.closest('ul');
      if (el) el = el.closest('ul') || el.parentElement;
      return el;
    }

    // Strategy 3: Look for span containing "rows" text near page numbers
    const spans = document.querySelectorAll('span');
    for (const span of spans) {
      if (/^\d[\d,]*\s*rows$/i.test(span.textContent.trim())) {
        return span.closest('ul') || span.parentElement;
      }
    }

    return null;
  }

  function findPaginationInfo() {
    const footer = findPaginationFooter();

    // Strategy 1: Find page buttons in the footer
    if (footer) {
      const text = footer.textContent;
      const pageNumbers = text.match(/\d+/g);
      if (pageNumbers && pageNumbers.length > 0) {
        const nums = pageNumbers.map(Number);
        const total = Math.max(...nums);
        // Current page: input field value or active button
        const pageInput = footer.querySelector('input[type="text"], input[type="number"], input');
        if (pageInput) {
          const current = parseInt(pageInput.value || pageInput.getAttribute('text') || '1', 10) || 1;
          return { current, total };
        }
        const activeBtn = footer.querySelector(
          '[aria-current="page"], [aria-current="true"], ' +
          'button[class*="active" i], a[class*="active" i]'
        );
        const current = activeBtn ? parseInt(activeBtn.textContent.trim(), 10) || 1 : 1;
        return { current, total };
      }
    }

    // Strategy 2: Compute from total rows and rows per page
    const totalRows = getTotalRows();
    const table = findDuneTable();
    if (totalRows > 0 && table) {
      const tbody = table.querySelector('tbody') || table;
      const visibleRows = tbody.querySelectorAll('tr td').length > 0
        ? tbody.querySelectorAll('tr').length : 0;
      if (visibleRows > 0) {
        const total = Math.ceil(totalRows / visibleRows);
        return { current: 1, total };
      }
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

  // ── Fast API Mode ─────────────────────────────────────────
  // Uses Dune's internal public API (core-api.dune.com) to fetch
  // data in bulk — no page clicking, no DOM parsing per page.
  // Works via browser session (no paid API key needed).

  const DUNE_API_URL = 'https://core-api.dune.com/public/execution';
  const HMAC_KEY = 'public-01K4W0KHXNC30MKZMRZY91HKM6';

  async function generateSignature(params) {
    const encoder = new TextEncoder();
    const key = await crypto.subtle.importKey(
      'raw',
      encoder.encode(HMAC_KEY),
      { name: 'HMAC', hash: 'SHA-256' },
      false,
      ['sign']
    );
    const limit = params.pagination?.limit ?? 0;
    const offset = params.pagination?.offset ?? 0;
    const message = `${params.ts.toString()}${params.execution_id}${params.query_id.toString()}${limit.toString()}${offset.toString()}`;
    const signature = await crypto.subtle.sign('HMAC', key, encoder.encode(message));
    return btoa(String.fromCharCode(...new Uint8Array(signature)))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/g, '');
  }

  function getQueryIdFromURL() {
    const match = window.location.pathname.match(/queries\/(\d+)/);
    return match ? parseInt(match[1], 10) : null;
  }

  function getExecutionIdFromPage() {
    // Intercept: we capture execution_id from Dune's own API calls
    return window.__duneScraperExecId || null;
  }

  function installFetchInterceptor() {
    if (window.__duneScraperInterceptorInstalled) return;
    window.__duneScraperInterceptorInstalled = true;

    const script = document.createElement('script');
    script.textContent = `
      (function() {
        var origFetch = window.fetch;
        window.fetch = function() {
          var url = typeof arguments[0] === 'string' ? arguments[0] : (arguments[0] && arguments[0].url) || '';
          if (url.indexOf('core-api.dune.com/public/execution') !== -1 && arguments[1] && arguments[1].body) {
            try {
              var body = JSON.parse(arguments[1].body);
              if (body.execution_id) {
                window.__duneScraperExecId = body.execution_id;
                window.dispatchEvent(new CustomEvent('dune-exec-id', { detail: body.execution_id }));
              }
            } catch(e) {}
          }
          return origFetch.apply(this, arguments);
        };
      })();
    `;
    document.documentElement.appendChild(script);
    script.remove();

    window.addEventListener('dune-exec-id', (e) => {
      window.__duneScraperExecId = e.detail;
    });
  }

  async function waitForExecutionId(timeoutMs = 15000) {
    // First check if we already have it
    if (window.__duneScraperExecId) return window.__duneScraperExecId;

    // Install interceptor and wait for next page navigation to capture it
    installFetchInterceptor();

    // Try triggering a page fetch by clicking next then back
    const nextBtn = findNextPageButton();
    if (nextBtn) {
      nextBtn.click();
      await sleep(1500);
      // Capture should have happened
      if (window.__duneScraperExecId) {
        // Go back to page 1
        const prevSvg = document.querySelector('svg[aria-label="Previous page"]');
        if (prevSvg) {
          const prevBtn = prevSvg.closest('button');
          if (prevBtn && !prevBtn.disabled) prevBtn.click();
          await sleep(1000);
        }
        return window.__duneScraperExecId;
      }
    }

    // Fallback: wait for user to navigate
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Timeout waiting for execution_id')), timeoutMs);
      window.addEventListener('dune-exec-id', (e) => {
        clearTimeout(timeout);
        resolve(e.detail);
      }, { once: true });
    });
  }

  async function fetchAPIPage(executionId, queryId, limit, offset) {
    const ts = Date.now();
    const params = {
      execution_id: executionId,
      query_id: queryId,
      parameters: [],
      pagination: { limit, offset },
      ts
    };
    params.s = await generateSignature(params);

    const response = await fetch(DUNE_API_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params)
    });

    if (!response.ok) {
      throw new Error(`API error: ${response.status} ${response.statusText}`);
    }

    return response.json();
  }

  async function scrapeViaAPI(maxRows) {
    isRunning = true;
    shouldStop = false;
    collectedRows = [];

    const queryId = getQueryIdFromURL();
    if (!queryId) {
      showToast('Error', 'Could not detect query ID from URL.');
      isRunning = false;
      return null;
    }

    try {
      settings = await new Promise(resolve => {
        chrome.runtime.sendMessage({ action: 'getSettings' }, resolve);
      });
    } catch (e) {
      settings = { minDelay: 300, maxDelay: 800 };
    }

    const minDelay = Math.max(settings.minDelay || 300, 100);
    const maxDelay = Math.max(settings.maxDelay || 800, 200);
    const batchSize = 1000; // rows per API call

    showToast('Fast Mode', 'Detecting execution ID...', 0);

    let executionId;
    try {
      executionId = await waitForExecutionId();
    } catch (e) {
      showToast('Error', 'Could not detect execution ID. Try navigating to another page first.');
      isRunning = false;
      return null;
    }

    totalRowsExpected = getTotalRows() || 0;
    const effectiveMax = maxRows || totalRowsExpected || 999999;

    showToast('Fast Mode', `Fetching data via API... (${totalRowsExpected.toLocaleString()} rows total)`, 0);

    // First request to get headers
    let offset = 0;
    let batchNum = 0;

    try {
      while (offset < effectiveMax && !shouldStop) {
        const limit = Math.min(batchSize, effectiveMax - offset);
        const data = await fetchAPIPage(executionId, queryId, limit, offset);

        if (data.execution_succeeded) {
          const result = data.execution_succeeded;

          // Extract headers from first batch
          if (batchNum === 0 && result.columns) {
            headers = result.columns;
          }

          // Extract rows
          const rows = result.data || [];
          if (rows.length === 0) break; // no more data

          for (const row of rows) {
            const rowArr = headers.map(h => {
              const val = row[h];
              return val != null ? String(val) : '';
            });
            collectedRows.push(rowArr);
          }

          offset += rows.length;
          batchNum++;

          const progress = totalRowsExpected > 0
            ? (collectedRows.length / Math.min(totalRowsExpected, effectiveMax)) * 100
            : 0;

          showToast(
            `Fast Mode — Batch ${batchNum}`,
            `${collectedRows.length.toLocaleString()} / ${Math.min(totalRowsExpected, effectiveMax).toLocaleString()} rows`,
            Math.min(progress, 100)
          );

          // If we got fewer rows than requested, we've reached the end
          if (rows.length < limit) break;

          // Anti-detection delay between batches
          await randomDelay(minDelay, maxDelay);

        } else if (data.execution_running || data.execution_queued) {
          showToast('Waiting', 'Query is still running... retrying in 3s');
          await sleep(3000);
        } else {
          showToast('Error', 'Unexpected API response');
          break;
        }
      }
    } catch (e) {
      showToast('API Error', e.message);
      if (collectedRows.length === 0) {
        isRunning = false;
        return null;
      }
    }

    isRunning = false;

    if (shouldStop) {
      showToast('Stopped', `Collected ${collectedRows.length.toLocaleString()} rows before stopping.`, 100);
    } else {
      showToast('Complete!', `${collectedRows.length.toLocaleString()} rows fetched via API!`, 100);
    }

    return { headers, rows: collectedRows, pageCount: batchNum };
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
      const useFastMode = message.fastMode === true;
      const scrapeFn = useFastMode
        ? () => scrapeViaAPI(message.maxRows)
        : () => scrapeAllPages(message.maxPages);

      scrapeFn().then(result => {
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

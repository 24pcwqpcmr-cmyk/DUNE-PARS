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

  // Execution ID is captured by background.js via chrome.webRequest.
  // No page-world injection needed — fully reliable.

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

  function escapeCSV(value) {
    if (value == null) return '';
    const str = String(value);
    if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
      return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
  }

  function generateCSV(headersList, rowsList) {
    const lines = [];
    if (headersList && headersList.length > 0) {
      lines.push(headersList.map(escapeCSV).join(','));
    }
    for (const row of rowsList) {
      lines.push(row.map(escapeCSV).join(','));
    }
    return lines.join('\r\n');
  }

  // Stream-friendly CSV blob for large datasets (avoids huge string concat)
  function generateCSVBlob(headersList, rowsList) {
    const CHUNK = 50000; // rows per chunk
    const parts = ['\uFEFF']; // BOM for Excel

    if (headersList && headersList.length > 0) {
      parts.push(headersList.map(escapeCSV).join(',') + '\r\n');
    }

    for (let i = 0; i < rowsList.length; i += CHUNK) {
      const end = Math.min(i + CHUNK, rowsList.length);
      const lines = [];
      for (let j = i; j < end; j++) {
        lines.push(rowsList[j].map(escapeCSV).join(','));
      }
      parts.push(lines.join('\r\n') + '\r\n');
    }

    return new Blob(parts, { type: 'text/csv;charset=utf-8' });
  }

  function getFilename() {
    const match = window.location.pathname.match(/queries\/(\d+)/);
    const queryId = match ? match[1] : 'export';
    const date = new Date().toISOString().slice(0, 10);
    return `dune_${queryId}_${date}.csv`;
  }

  // Direct download from content script — no size limits
  function downloadCSVDirect(headersList, rowsList) {
    showToast('Generating CSV', `Building file for ${rowsList.length.toLocaleString()} rows...`, 95);
    const blob = generateCSVBlob(headersList, rowsList);
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = getFilename();
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 30000);
    showToast('Downloaded!', `${getFilename()} — ${(blob.size / 1024 / 1024).toFixed(1)} MB`, 100);
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

  // Ask background.js for the execution_id (captured via webRequest)
  function getExecIdFromBackground() {
    return new Promise((resolve) => {
      chrome.runtime.sendMessage({ action: 'getExecutionId' }, (resp) => {
        resolve(resp?.executionId || null);
      });
    });
  }

  async function waitForExecutionId() {
    // Strategy 1: Background already captured it from page load
    let execId = await getExecIdFromBackground();
    if (execId) return execId;

    // Strategy 2: Trigger a page navigation to force an API call
    showToast('Fast Mode', 'Detecting execution ID...', 0);
    const nextBtn = findNextPageButton();
    if (nextBtn) {
      nextBtn.click();
      for (let i = 0; i < 30; i++) {
        await sleep(500);
        execId = await getExecIdFromBackground();
        if (execId) {
          // Go back to page 1
          await sleep(300);
          const prevSvg = document.querySelector('svg[aria-label="Previous page"]');
          if (prevSvg) {
            const prevBtn = prevSvg.closest('button');
            if (prevBtn && !prevBtn.disabled) {
              prevBtn.click();
              await sleep(1000);
            }
          }
          return execId;
        }
      }
    }

    // Strategy 3: Try clicking page 2 directly
    const footer = findPaginationFooter();
    if (footer) {
      const buttons = footer.querySelectorAll('button');
      for (const btn of buttons) {
        if (btn.textContent.trim() === '2') {
          btn.click();
          for (let i = 0; i < 30; i++) {
            await sleep(500);
            execId = await getExecIdFromBackground();
            if (execId) {
              await sleep(300);
              const btn1 = Array.from(footer.querySelectorAll('button'))
                .find(b => b.textContent.trim() === '1');
              if (btn1) { btn1.click(); await sleep(1000); }
              return execId;
            }
          }
          break;
        }
      }
    }

    throw new Error('Could not detect execution ID. Please switch to a different page and back, then try again.');
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

  function processAPIResult(result) {
    const rows = result.data || [];
    const processed = [];
    for (const row of rows) {
      const rowArr = headers.map(h => {
        const val = row[h];
        return val != null ? String(val) : '';
      });
      processed.push(rowArr);
    }
    return processed;
  }

  async function scrapeViaAPI(maxRows, turbo) {
    isRunning = true;
    shouldStop = false;
    collectedRows = [];

    const queryId = getQueryIdFromURL();
    if (!queryId) {
      showToast('Error', 'Could not detect query ID from URL.');
      isRunning = false;
      return null;
    }

    // Turbo mode: parallel requests, no delays, larger batches
    const PARALLEL = turbo ? 10 : 1;
    const BATCH_SIZE = turbo ? 5000 : 1000;

    showToast(turbo ? 'TURBO Mode' : 'Fast Mode', 'Detecting execution ID...', 0);

    let executionId;
    try {
      executionId = await waitForExecutionId();
    } catch (e) {
      showToast('Error', e.message || 'Could not detect execution ID.');
      isRunning = false;
      return null;
    }

    totalRowsExpected = getTotalRows() || 0;
    const effectiveMax = maxRows || totalRowsExpected || 999999;
    const modeLabel = turbo ? 'TURBO' : 'Fast';

    showToast(`${modeLabel} Mode`, `Fetching ${effectiveMax.toLocaleString()} rows (${PARALLEL} parallel, ${BATCH_SIZE}/batch)...`, 0);

    // First request — get headers
    let offset = 0;
    let batchNum = 0;

    try {
      const firstData = await fetchAPIPage(executionId, queryId, BATCH_SIZE, 0);
      if (firstData.execution_running || firstData.execution_queued) {
        showToast('Waiting', 'Query still running... retrying in 3s');
        await sleep(3000);
      }
      if (firstData.execution_succeeded) {
        const result = firstData.execution_succeeded;
        if (result.columns) headers = result.columns;
        const rows = processAPIResult(result);
        collectedRows.push(...rows);
        offset = rows.length;
        batchNum = 1;
        if (rows.length < BATCH_SIZE) {
          // All data fits in one batch
          isRunning = false;
          showToast('Complete!', `${collectedRows.length.toLocaleString()} rows fetched!`, 100);
          return { headers, rows: collectedRows, pageCount: 1 };
        }
      } else {
        showToast('Error', 'Unexpected API response on first batch');
        isRunning = false;
        return null;
      }

      // Parallel fetching loop
      while (offset < effectiveMax && !shouldStop) {
        const tasks = [];
        for (let i = 0; i < PARALLEL && offset + i * BATCH_SIZE < effectiveMax; i++) {
          const taskOffset = offset + i * BATCH_SIZE;
          const taskLimit = Math.min(BATCH_SIZE, effectiveMax - taskOffset);
          tasks.push(
            fetchAPIPage(executionId, queryId, taskLimit, taskOffset)
              .then(data => ({ offset: taskOffset, data, error: null }))
              .catch(error => ({ offset: taskOffset, data: null, error }))
          );
        }

        const results = await Promise.all(tasks);
        results.sort((a, b) => a.offset - b.offset);

        let hitEnd = false;
        let totalNewRows = 0;
        for (const res of results) {
          if (shouldStop) break;
          if (res.error) {
            // On error in turbo, just skip and continue
            if (!turbo) throw res.error;
            continue;
          }
          if (res.data.execution_succeeded) {
            const rows = processAPIResult(res.data.execution_succeeded);
            collectedRows.push(...rows);
            totalNewRows += rows.length;
            if (rows.length < BATCH_SIZE) { hitEnd = true; break; }
          }
        }

        offset += PARALLEL * BATCH_SIZE;
        batchNum += results.length;

        const progress = totalRowsExpected > 0
          ? (collectedRows.length / Math.min(totalRowsExpected, effectiveMax)) * 100
          : 0;

        const speed = turbo ? ` (~${(totalNewRows * PARALLEL).toLocaleString()} rows/wave)` : '';
        showToast(
          `${modeLabel} — Wave ${Math.ceil(batchNum / PARALLEL)}`,
          `${collectedRows.length.toLocaleString()} / ${effectiveMax.toLocaleString()} rows${speed}`,
          Math.min(progress, 100)
        );

        if (hitEnd) break;

        // Small delay only in non-turbo mode
        if (!turbo) {
          const minD = Math.max(settings.minDelay || 300, 100);
          const maxD = Math.max(settings.maxDelay || 800, 200);
          await randomDelay(minD, maxD);
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
      showToast('Complete!', `${collectedRows.length.toLocaleString()} rows fetched via ${modeLabel}!`, 100);
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
      const turbo = message.turbo === true;
      const scrapeFn = useFastMode
        ? () => scrapeViaAPI(message.maxRows, turbo)
        : () => scrapeAllPages(message.maxPages);

      scrapeFn().then(result => {
        if (result && result.rows.length > 0) {
          // Auto-download directly from content script (no message size limits)
          downloadCSVDirect(result.headers, result.rows);

          // Update stats
          chrome.runtime.sendMessage({
            action: 'updateStats',
            rowCount: result.rows.length
          });

          // Notify popup (metadata only, no CSV data)
          chrome.runtime.sendMessage({
            action: 'scrapeComplete',
            rowCount: result.rows.length,
            pageCount: result.pageCount,
            headers: result.headers,
            autoDownloaded: true
          });
        } else {
          chrome.runtime.sendMessage({
            action: 'scrapeComplete',
            error: 'Scrape failed or no table found.'
          });
        }
        setTimeout(hideToast, 8000);
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

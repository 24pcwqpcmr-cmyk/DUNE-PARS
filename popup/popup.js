/**
 * Dune Table Scraper — Popup Controller
 * Manages the extension popup UI and communicates with content/background scripts.
 */

(() => {
  'use strict';

  // ── DOM Elements ───────────────────────────────────────
  const statusDot = document.querySelector('.status-dot');
  const statusText = document.querySelector('.status-text');
  const tableInfo = document.getElementById('tableInfo');
  const colCount = document.getElementById('colCount');
  const visibleRows = document.getElementById('visibleRows');
  const totalRows = document.getElementById('totalRows');
  const pageCount = document.getElementById('pageCount');

  const progressPanel = document.getElementById('progressPanel');
  const progressBar = document.getElementById('progressBar');
  const progressPct = document.getElementById('progressPct');
  const progressRows = document.getElementById('progressRows');
  const progressPages = document.getElementById('progressPages');

  const scrapePageBtn = document.getElementById('scrapePageBtn');
  const scrapeAllBtn = document.getElementById('scrapeAllBtn');
  const stopBtn = document.getElementById('stopBtn');
  const actionsPanel = document.getElementById('actionsPanel');
  const maxPagesInput = document.getElementById('maxPages');
  const maxRowsInput = document.getElementById('maxRows');

  const modeDOMBtn = document.getElementById('modeDOM');
  const modeFastBtn = document.getElementById('modeFast');
  const domOptions = document.getElementById('domOptions');
  const fastOptions = document.getElementById('fastOptions');
  const turboCheck = document.getElementById('turboCheck');
  const fastHint = document.getElementById('fastHint');

  const resultsPanel = document.getElementById('resultsPanel');
  const resultRowsEl = document.getElementById('resultRows');
  const resultPagesEl = document.getElementById('resultPages');
  const resultsPreview = document.getElementById('resultsPreview');
  const downloadBtn = document.getElementById('downloadBtn');

  const settingsBtn = document.getElementById('settingsBtn');
  const settingsPanel = document.getElementById('settingsPanel');
  const closeSettingsBtn = document.getElementById('closeSettingsBtn');
  const saveSettingsBtn = document.getElementById('saveSettingsBtn');
  const settingMinDelay = document.getElementById('settingMinDelay');
  const settingMaxDelay = document.getElementById('settingMaxDelay');
  const settingFilename = document.getElementById('settingFilename');

  const lifetimeExports = document.getElementById('lifetimeExports');
  const lifetimeRows = document.getElementById('lifetimeRows');

  // ── State ──────────────────────────────────────────────
  let currentTabId = null;
  let lastCSV = null;
  let lastHeaders = [];
  let lastRowCount = 0;
  let lastPageCount = 0;
  let pollInterval = null;
  let fastMode = true; // default to fast mode

  // ── Initialize ─────────────────────────────────────────

  async function init() {
    // Get current tab
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    currentTabId = tab?.id;

    if (!tab?.url?.includes('dune.com')) {
      setStatus('offline', 'Not on a Dune Analytics page');
      return;
    }

    // Load settings
    loadSettings();
    loadStats();

    // Detect table
    detectTable();
  }

  // ── Table Detection ────────────────────────────────────

  async function detectTable() {
    if (!currentTabId) return;

    try {
      const response = await sendToContent({ action: 'detectTable' });
      if (response?.found) {
        setStatus('online', 'Table detected — ready to scrape');
        tableInfo.classList.remove('hidden');
        colCount.textContent = response.colCount;
        visibleRows.textContent = response.rowCount.toLocaleString();
        totalRows.textContent = response.totalRows > 0
          ? response.totalRows.toLocaleString()
          : '~' + response.rowCount.toLocaleString();
        pageCount.textContent = response.pagination.total > 1
          ? response.pagination.total.toLocaleString()
          : '1';

        scrapePageBtn.disabled = false;
        scrapeAllBtn.disabled = false;
      } else {
        setStatus('offline', 'No table found on this page');
        tableInfo.classList.add('hidden');
      }
    } catch (err) {
      setStatus('offline', 'Cannot connect to page (try refreshing)');
    }
  }

  // ── Status ─────────────────────────────────────────────

  function setStatus(state, text) {
    statusDot.className = 'status-dot ' + state;
    statusText.textContent = text;
  }

  // ── Communication ──────────────────────────────────────

  function sendToContent(message) {
    return new Promise((resolve, reject) => {
      chrome.tabs.sendMessage(currentTabId, message, (response) => {
        if (chrome.runtime.lastError) {
          reject(chrome.runtime.lastError);
        } else {
          resolve(response);
        }
      });
    });
  }

  // ── Scrape Current Page ────────────────────────────────

  async function scrapeCurrentPage() {
    if (!currentTabId) return;

    scrapePageBtn.disabled = true;
    scrapeAllBtn.disabled = true;
    setStatus('running', 'Scraping current page...');

    try {
      const response = await sendToContent({ action: 'scrapeCurrentPage' });
      if (response?.success) {
        lastCSV = response.csv;
        lastHeaders = response.headers;
        lastRowCount = response.rowCount;
        lastPageCount = 1;
        showResults(response.headers, response.rowCount, 1, response.csv);
        setStatus('online', 'Scrape complete!');
      } else {
        setStatus('offline', response?.error || 'Failed to scrape');
      }
    } catch (err) {
      setStatus('offline', 'Error: ' + err.message);
    }

    scrapePageBtn.disabled = false;
    scrapeAllBtn.disabled = false;
  }

  // ── Scrape All Pages ───────────────────────────────────

  async function scrapeAllPages() {
    if (!currentTabId) return;

    scrapePageBtn.disabled = true;
    scrapeAllBtn.classList.add('hidden');
    stopBtn.classList.remove('hidden');
    progressPanel.classList.remove('hidden');
    resultsPanel.classList.add('hidden');

    const turbo = fastMode && turboCheck.checked;
    const msg = { action: 'scrapeAllPages', fastMode, turbo };
    if (fastMode) {
      msg.maxRows = parseInt(maxRowsInput.value, 10) || 0;
      setStatus('running', turbo ? 'TURBO — max speed, parallel fetch...' : 'Fast Mode — fetching via API...');
    } else {
      msg.maxPages = parseInt(maxPagesInput.value, 10) || 0;
      setStatus('running', 'DOM Mode — scraping pages...');
    }
    updateProgress(0, 0, 0);

    try {
      await sendToContent(msg);
      startPolling();
    } catch (err) {
      setStatus('offline', 'Error: ' + err.message);
      resetUI();
    }
  }

  // ── Polling for progress ───────────────────────────────

  function startPolling() {
    pollInterval = setInterval(async () => {
      try {
        const status = await sendToContent({ action: 'getStatus' });
        if (status) {
          const totalExpected = status.totalExpected || 1;
          const pct = status.totalExpected > 0
            ? Math.min(100, (status.rowCount / totalExpected) * 100)
            : 0;
          updateProgress(pct, status.rowCount, 0);

          if (!status.isRunning) {
            stopPolling();
          }
        }
      } catch (e) {
        // Tab might have closed
        stopPolling();
      }
    }, 1000);
  }

  function stopPolling() {
    if (pollInterval) {
      clearInterval(pollInterval);
      pollInterval = null;
    }
  }

  function updateProgress(pct, rows, pages) {
    progressBar.style.width = pct + '%';
    progressPct.textContent = Math.round(pct) + '%';
    progressRows.textContent = (rows || 0).toLocaleString() + ' rows';
    if (pages) progressPages.textContent = 'Page ' + pages;
  }

  // ── Stop Scraping ─────────────────────────────────────

  async function stopScraping() {
    try {
      await sendToContent({ action: 'stopScraping' });
    } catch (e) {
      // ignore
    }
    stopPolling();
    setStatus('online', 'Scraping stopped');
  }

  // ── Listen for scrape completion ───────────────────────

  chrome.runtime.onMessage.addListener((message) => {
    if (message.action === 'scrapeComplete') {
      stopPolling();
      if (message.error) {
        setStatus('offline', message.error);
        resetUI();
      } else {
        lastCSV = message.csv;
        lastHeaders = message.headers;
        lastRowCount = message.rowCount;
        lastPageCount = message.pageCount;
        showResults(message.headers, message.rowCount, message.pageCount, message.csv);
        setStatus('online', 'Scrape complete!');
        resetUI();
      }
    }
  });

  // ── Show Results ───────────────────────────────────────

  function showResults(hdrs, rowCount, pgCount, csv) {
    resultsPanel.classList.remove('hidden');
    resultRowsEl.textContent = rowCount.toLocaleString();
    resultPagesEl.textContent = pgCount;

    // Build preview table (first 5 rows)
    const lines = csv.split('\r\n').filter(l => l.trim());
    const previewLines = lines.slice(0, 6); // header + 5 rows

    let html = '<table><thead><tr>';
    if (hdrs.length > 0) {
      for (const h of hdrs.slice(0, 6)) {
        html += '<th>' + escapeHtml(h) + '</th>';
      }
      if (hdrs.length > 6) html += '<th>...</th>';
    }
    html += '</tr></thead><tbody>';

    for (let i = 1; i < previewLines.length; i++) {
      html += '<tr>';
      const cells = parseCSVLine(previewLines[i]);
      for (let j = 0; j < Math.min(cells.length, 6); j++) {
        html += '<td>' + escapeHtml(cells[j]) + '</td>';
      }
      if (cells.length > 6) html += '<td>...</td>';
      html += '</tr>';
    }
    html += '</tbody></table>';
    resultsPreview.innerHTML = html;
  }

  function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  function parseCSVLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (inQuotes) {
        if (ch === '"' && line[i + 1] === '"') {
          current += '"';
          i++;
        } else if (ch === '"') {
          inQuotes = false;
        } else {
          current += ch;
        }
      } else {
        if (ch === '"') {
          inQuotes = true;
        } else if (ch === ',') {
          result.push(current);
          current = '';
        } else {
          current += ch;
        }
      }
    }
    result.push(current);
    return result;
  }

  // ── Download CSV ───────────────────────────────────────

  async function downloadCSV() {
    if (!lastCSV) return;

    // Generate filename
    let settings;
    try {
      settings = await new Promise(resolve => {
        chrome.runtime.sendMessage({ action: 'getSettings' }, resolve);
      });
    } catch (e) {
      settings = { filenameTemplate: 'dune_{query}_{date}' };
    }

    const queryName = extractQueryName();
    const date = new Date().toISOString().slice(0, 10);
    const template = settings?.filenameTemplate || 'dune_{query}_{date}';
    const filename = template
      .replace('{query}', queryName)
      .replace('{date}', date)
      .replace('{rows}', String(lastRowCount));

    // Send to background for download
    chrome.runtime.sendMessage({
      action: 'downloadCSV',
      data: lastCSV,
      filename: filename
    });

    // Update stats
    chrome.runtime.sendMessage({
      action: 'updateStats',
      rowCount: lastRowCount
    }, (stats) => {
      if (stats) {
        lifetimeExports.textContent = stats.totalExports;
        lifetimeRows.textContent = stats.totalRows.toLocaleString();
      }
    });

    // Visual feedback
    downloadBtn.textContent = 'Downloaded!';
    downloadBtn.style.background = 'var(--accent-green)';
    setTimeout(() => {
      downloadBtn.innerHTML = `
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
          <polyline points="7 10 12 15 17 10"/>
          <line x1="12" y1="15" x2="12" y2="3"/>
        </svg>
        Download CSV
      `;
      downloadBtn.style.background = '';
    }, 2000);
  }

  function extractQueryName() {
    // Try to get query name from the page URL or title
    try {
      const url = new URL(window.location !== window.parent.location ? document.referrer : '');
      const parts = url.pathname.split('/');
      // Dune URLs look like: /queries/12345/query-name
      const nameIdx = parts.findIndex(p => p === 'queries');
      if (nameIdx >= 0 && parts[nameIdx + 2]) {
        return parts[nameIdx + 2].replace(/-/g, '_');
      }
    } catch (e) {
      // Popup can't access tab URL directly, use a generic name
    }
    return 'table_export';
  }

  // ── Reset UI ───────────────────────────────────────────

  function resetUI() {
    scrapePageBtn.disabled = false;
    scrapeAllBtn.classList.remove('hidden');
    scrapeAllBtn.disabled = false;
    stopBtn.classList.add('hidden');
    progressPanel.classList.add('hidden');
  }

  // ── Settings ───────────────────────────────────────────

  function loadSettings() {
    chrome.runtime.sendMessage({ action: 'getSettings' }, (settings) => {
      if (settings) {
        settingMinDelay.value = settings.minDelay || 800;
        settingMaxDelay.value = settings.maxDelay || 2500;
        settingFilename.value = settings.filenameTemplate || 'dune_{query}_{date}';
      }
    });
  }

  function saveSettings() {
    const newSettings = {
      minDelay: parseInt(settingMinDelay.value, 10) || 800,
      maxDelay: parseInt(settingMaxDelay.value, 10) || 2500,
      filenameTemplate: settingFilename.value || 'dune_{query}_{date}'
    };
    chrome.runtime.sendMessage({ action: 'saveSettings', settings: newSettings }, () => {
      settingsPanel.classList.add('hidden');
    });
  }

  function loadStats() {
    chrome.runtime.sendMessage({ action: 'getStats' }, (stats) => {
      if (stats) {
        lifetimeExports.textContent = stats.totalExports || 0;
        lifetimeRows.textContent = (stats.totalRows || 0).toLocaleString();
      }
    });
  }

  // ── Mode Toggle ─────────────────────────────────────────

  function setMode(mode) {
    fastMode = mode === 'fast';
    modeFastBtn.classList.toggle('active', fastMode);
    modeDOMBtn.classList.toggle('active', !fastMode);
    fastOptions.classList.toggle('hidden', !fastMode);
    domOptions.classList.toggle('hidden', fastMode);
    scrapeAllBtn.innerHTML = fastMode
      ? `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
           <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
         </svg>
         Fast Export → CSV`
      : `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
           <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/>
           <polyline points="13 2 13 9 20 9"/>
         </svg>
         All Pages → CSV`;
  }

  // ── Event Listeners ────────────────────────────────────

  modeDOMBtn.addEventListener('click', () => setMode('dom'));
  modeFastBtn.addEventListener('click', () => setMode('fast'));
  turboCheck.addEventListener('change', () => {
    fastHint.textContent = turboCheck.checked
      ? 'TURBO: 10 parallel \u00d7 5000/batch \u00b7 no delays \u00b7 max speed'
      : 'Fast: 1 request \u00d7 1000/batch \u00b7 with delays \u00b7 stealth';
  });
  scrapePageBtn.addEventListener('click', scrapeCurrentPage);
  scrapeAllBtn.addEventListener('click', scrapeAllPages);
  stopBtn.addEventListener('click', stopScraping);
  downloadBtn.addEventListener('click', downloadCSV);
  settingsBtn.addEventListener('click', () => settingsPanel.classList.remove('hidden'));
  closeSettingsBtn.addEventListener('click', () => settingsPanel.classList.add('hidden'));
  saveSettingsBtn.addEventListener('click', saveSettings);

  // ── Start ──────────────────────────────────────────────
  setMode('fast');
  init();
})();

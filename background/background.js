/**
 * Dune Table Scraper — Background Service Worker
 * Handles CSV download requests and manages extension state.
 */

// Default settings
const DEFAULT_SETTINGS = {
  minDelay: 800,
  maxDelay: 2500,
  batchSize: 50,
  autoSaveDesktop: true,
  filenameTemplate: 'dune_{query}_{date}',
  maxConcurrentPages: 1
};

// Initialize settings on install
chrome.runtime.onInstalled.addListener(async () => {
  const stored = await chrome.storage.local.get('settings');
  if (!stored.settings) {
    await chrome.storage.local.set({ settings: DEFAULT_SETTINGS });
  }
});

// Listen for messages from popup/content scripts
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'downloadCSV') {
    handleCSVDownload(message.data, message.filename);
    sendResponse({ success: true });
  }

  if (message.action === 'getSettings') {
    chrome.storage.local.get('settings').then(result => {
      sendResponse(result.settings || DEFAULT_SETTINGS);
    });
    return true; // async
  }

  if (message.action === 'saveSettings') {
    chrome.storage.local.set({ settings: message.settings }).then(() => {
      sendResponse({ success: true });
    });
    return true;
  }

  if (message.action === 'getStats') {
    chrome.storage.local.get('stats').then(result => {
      sendResponse(result.stats || { totalExports: 0, totalRows: 0 });
    });
    return true;
  }

  if (message.action === 'updateStats') {
    chrome.storage.local.get('stats').then(result => {
      const stats = result.stats || { totalExports: 0, totalRows: 0 };
      stats.totalExports += 1;
      stats.totalRows += message.rowCount || 0;
      chrome.storage.local.set({ stats });
      sendResponse(stats);
    });
    return true;
  }
});

/**
 * Download CSV data as a file
 */
function handleCSVDownload(csvContent, filename) {
  const blob = new Blob(['\uFEFF' + csvContent], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);

  const sanitizedFilename = filename
    .replace(/[<>:"/\\|?*]/g, '_')
    .replace(/\s+/g, '_');

  chrome.downloads.download({
    url: url,
    filename: sanitizedFilename + '.csv',
    saveAs: false
  }, (downloadId) => {
    if (chrome.runtime.lastError) {
      console.error('Download error:', chrome.runtime.lastError);
    }
    // Clean up blob URL after a delay
    setTimeout(() => URL.revokeObjectURL(url), 10000);
  });
}

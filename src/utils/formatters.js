/**
 * Format bytes to human-readable format
 */
export function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes';

  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

/**
 * Format large numbers to human-readable format
 */
export function formatNumber(num) {
  if (num === 0) return '0';

  const absNum = Math.abs(num);
  
  if (absNum >= 1e12) {
    return (num / 1e12).toFixed(2) + 'T';
  }
  if (absNum >= 1e9) {
    return (num / 1e9).toFixed(2) + 'B';
  }
  if (absNum >= 1e6) {
    return (num / 1e6).toFixed(2) + 'M';
  }
  if (absNum >= 1e3) {
    return (num / 1e3).toFixed(2) + 'K';
  }
  
  return num.toLocaleString();
}

/**
 * Format date to readable string
 */
export function formatDate(date) {
  return new Date(date).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  });
}

/**
 * Calculate percentage
 */
export function calculatePercentage(current, total) {
  if (!total || total === 0) return 0;
  return Math.min((current / total) * 100, 100);
}

/**
 * Format HTTP requests (shows K for < 1M, M for >= 1M)
 */
export function formatRequests(requests, decimals = 2) {
  if (requests === 0) return '0';
  
  if (requests >= 1e6) {
    const millions = requests / 1e6;
    return millions.toFixed(decimals) + 'M';
  } else {
    const thousands = requests / 1e3;
    return thousands.toFixed(decimals) + 'K';
  }
}

/**
 * Format bandwidth (shows GB for < 1TB, TB for >= 1TB)
 * Uses decimal units (1 TB = 1000^4 bytes) as is standard for bandwidth/storage
 */
export function formatBandwidthTB(bytes, decimals = 2) {
  if (bytes === 0) return '0 GB';
  
  const tb = bytes / (1000 ** 4);
  
  if (tb >= 1) {
    return tb.toFixed(decimals) + ' TB';
  } else {
    const gb = bytes / (1000 ** 3);
    return gb.toFixed(decimals) + ' GB';
  }
}

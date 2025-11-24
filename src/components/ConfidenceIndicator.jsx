import React from 'react';

/**
 * Confidence Indicator Component
 * Displays data quality/accuracy based on sampling confidence intervals
 * 
 * @param {number} percent - Confidence percentage (0-100)
 * @param {number} sampleSize - Number of samples used
 * @param {string} size - Display size: 'sm', 'md', 'lg'
 */
export default function ConfidenceIndicator({ percent, sampleSize, size = 'sm' }) {
  if (!percent && percent !== 0) return null;
  
  // Determine color based on confidence level
  const getColorClasses = (pct) => {
    if (pct >= 95) return {
      bg: 'bg-green-100',
      text: 'text-green-700',
      border: 'border-green-300',
      dot: 'bg-green-500'
    };
    if (pct >= 90) return {
      bg: 'bg-blue-100',
      text: 'text-blue-700',
      border: 'border-blue-300',
      dot: 'bg-blue-500'
    };
    if (pct >= 80) return {
      bg: 'bg-yellow-100',
      text: 'text-yellow-700',
      border: 'border-yellow-300',
      dot: 'bg-yellow-500'
    };
    return {
      bg: 'bg-orange-100',
      text: 'text-orange-700',
      border: 'border-orange-300',
      dot: 'bg-orange-500'
    };
  };
  
  const colors = getColorClasses(percent);
  
  // Size variants
  const sizeClasses = {
    sm: {
      container: 'px-2 py-0.5 text-xs',
      dot: 'h-1.5 w-1.5',
      text: 'text-xs'
    },
    md: {
      container: 'px-2.5 py-1 text-sm',
      dot: 'h-2 w-2',
      text: 'text-sm'
    },
    lg: {
      container: 'px-3 py-1.5 text-base',
      dot: 'h-2.5 w-2.5',
      text: 'text-base'
    }
  };
  
  const sizeClass = sizeClasses[size] || sizeClasses.sm;
  
  return (
    <div 
      className={`inline-flex items-center gap-1.5 ${colors.bg} ${colors.border} border rounded-full ${sizeClass.container}`}
    >
      <span className={`${colors.dot} ${sizeClass.dot} rounded-full`}></span>
      <span className={`${colors.text} font-medium ${sizeClass.text}`}>
        {percent}%
      </span>
    </div>
  );
}

/**
 * Format large numbers for display
 */
function formatNumber(value, metricName) {
  if (!value) return 'N/A';
  
  // If this is a bytes metric, convert to GB/TB
  if (metricName?.toLowerCase().includes('transfer') || metricName?.toLowerCase().includes('bandwidth')) {
    const gb = value / (1024 ** 3);
    if (gb >= 1000) {
      return `${(gb / 1024).toFixed(2)} TB`;
    }
    return `${gb.toFixed(2)} GB`;
  }
  
  // For requests/counts, use abbreviated format
  if (value >= 1000000000) {
    return `${(value / 1000000000).toFixed(2)}B`;
  }
  if (value >= 1000000) {
    return `${(value / 1000000).toFixed(2)}M`;
  }
  if (value >= 1000) {
    return `${(value / 1000).toFixed(2)}K`;
  }
  
  return value.toLocaleString();
}

/**
 * Confidence Badge with detailed tooltip
 * Shows confidence level with expandable details
 */
export function ConfidenceBadge({ 
  confidence, 
  metricName, 
  isZoneFiltered = false,
  actualValue = null,
  metricType = 'HTTP requests'
}) {
  if (!confidence || !confidence.percent) return null;
  
  const { percent, sampleSize, estimate, lower, upper } = confidence;
  
  // Use actual value if provided (for zone-filtered metrics), otherwise use confidence estimate
  const displayEstimate = actualValue !== null ? actualValue : estimate;
  const displayLower = actualValue !== null ? actualValue : lower;
  const displayUpper = actualValue !== null ? actualValue : upper;
  
  return (
    <div className="confidence-badge-group group relative inline-block z-10 hover:z-[9999]">
      <ConfidenceIndicator percent={percent} sampleSize={sampleSize} size="sm" />
      
      {/* Tooltip on hover - centered below badge */}
      <div className="invisible group-hover:visible absolute z-[9999] w-72 p-3 mt-2 text-sm bg-gray-900 text-white rounded-lg shadow-lg left-1/2 -translate-x-1/2">
        <div className="mb-2">
          <div className="font-semibold text-white mb-1">{metricName} Confidence</div>
          <div className="text-gray-300 text-xs">
            {metricType.includes('(measuring') ? (
              <>
                Based on bytes from {sampleSize?.toLocaleString()} sampled HTTP requests
              </>
            ) : (
              <>
                Based on {sampleSize?.toLocaleString()} sampled {metricType}
              </>
            )}
            {isZoneFiltered && <div className="text-gray-400 mt-0.5">(filtered to configured zones)</div>}
          </div>
        </div>
        
        <div className="space-y-1.5 text-xs">
          <div className="flex justify-between items-baseline gap-2">
            <span className="text-gray-400 flex-shrink-0">Estimate:</span>
            <span className="font-mono text-right">{formatNumber(displayEstimate, metricName)}</span>
          </div>
          <div className="flex justify-between items-baseline gap-2">
            <span className="text-gray-400 flex-shrink-0">Range:</span>
            <span className="font-mono text-right">{formatNumber(displayLower, metricName)} - {formatNumber(displayUpper, metricName)}</span>
          </div>
          <div className="flex justify-between items-baseline gap-2">
            <span className="text-gray-400 flex-shrink-0">Confidence:</span>
            <span className="font-mono text-right font-semibold text-green-400">{percent}%</span>
          </div>
        </div>
        
        <div className="mt-2 pt-2 border-t border-gray-700 text-xs text-gray-400">
          95% confidence interval from Cloudflare's adaptive sampling
        </div>
      </div>
    </div>
  );
}

import React from 'react';
import { Globe, Activity, Database, TrendingUp, TrendingDown, AlertCircle, Shield, CheckCircle, Network } from 'lucide-react';

function MetricCard({ 
  title, 
  subtitle,
  value, 
  formatted, 
  threshold, 
  percentage, 
  previousValue,
  icon,
  unit,
  isPrevious = false,
  compact = false,
  zoneBreakdown = null,
  primaryZones = null,
  secondaryZones = null,
  requestBreakdown = null
}) {
  const getIcon = () => {
    switch (icon) {
      case 'zones':
        return <Globe className={compact ? "w-5 h-5" : "w-6 h-6"} />;
      case 'requests':
        return <Activity className={compact ? "w-5 h-5" : "w-6 h-6"} />;
      case 'bandwidth':
        return <Database className={compact ? "w-5 h-5" : "w-6 h-6"} />;
      case 'shield':
        return <Shield className={compact ? "w-5 h-5" : "w-6 h-6"} />;
      case 'check':
        return <CheckCircle className={compact ? "w-5 h-5" : "w-6 h-6"} />;
      case 'dns':
        return <Network className={compact ? "w-5 h-5" : "w-6 h-6"} />;
      default:
        return <Activity className={compact ? "w-5 h-5" : "w-6 h-6"} />;
    }
  };

  const formatThreshold = (value) => {
    if (!value) return '0';
    
    // Format based on metric type
    if (icon === 'bandwidth') {
      // Format as TB or GB
      const tb = value / (1024 ** 4);
      if (tb >= 1) {
        return `${tb.toFixed(2)} TB`;
      } else {
        const gb = value / (1024 ** 3);
        return `${gb.toFixed(2)} GB`;
      }
    } else if (icon === 'requests' || icon === 'check' || icon === 'dns') {
      // Format as M or K
      if (value >= 1e6) {
        return `${(value / 1e6).toFixed(2)}M`;
      } else {
        return `${(value / 1e3).toFixed(2)}K`;
      }
    } else {
      // Default: just add commas
      return value.toLocaleString();
    }
  };

  const getChangePercentage = () => {
    if (isPrevious || !previousValue || previousValue === 0) return null;
    const change = ((value - previousValue) / previousValue) * 100;
    return change;
  };

  const change = getChangePercentage();
  
  // Ensure percentage is a number
  const numPercentage = Number(percentage) || 0;
  
  // Red if over 100%, amber if 90-100%, blue otherwise
  const isOverThreshold = threshold && numPercentage > 100;
  const isWarning = threshold && numPercentage >= 90 && numPercentage <= 100;

  return (
    <div 
      className={`rounded-lg shadow-sm border-2 transition-all duration-200 ${
        isOverThreshold ? 'border-red-300 bg-red-50' : 
        isWarning ? 'border-orange-300 bg-orange-50' : 
        'border-gray-200 bg-white hover:shadow-lg hover:-translate-y-0.5'
      }`}
    >
      <div className={compact ? "p-4" : "p-6"}>
        {/* Header with Zone Breakdown */}
        <div className="flex items-start justify-between mb-4">
          <div className="flex-1">
            {/* Title - Large */}
            <div className="flex items-center space-x-2 mb-2">
              <h3 className={`${compact ? 'text-base' : 'text-xl'} font-medium text-gray-900`}>
                {title}
              </h3>
              {isOverThreshold && (
                <AlertCircle className={`${compact ? "w-4 h-4" : "w-5 h-5"} text-red-600 flex-shrink-0`} />
              )}
            </div>
            {subtitle && (
              <p className="text-sm text-gray-600 mb-2">{subtitle}</p>
            )}
          </div>

          {/* Zone Breakdown - Top Right */}
          {zoneBreakdown && (primaryZones || secondaryZones) && (
            <div className="ml-4 bg-gray-50 border border-gray-200 rounded-lg px-3 py-2">
              <div className="space-y-1 text-xs">
                <div className="flex items-center space-x-2">
                  <span className="text-gray-700 whitespace-nowrap">🟢 Primary:</span>
                  <span className="font-semibold text-gray-900">{zoneBreakdown.primary || 0}/{primaryZones || 0}</span>
                </div>
                <div className="flex items-center space-x-2">
                  <span className="text-gray-700 whitespace-nowrap">🔵 Secondary:</span>
                  <span className="font-semibold text-gray-900">{zoneBreakdown.secondary || 0}/{secondaryZones || 0}</span>
                </div>
              </div>
            </div>
          )}

          {/* Request Breakdown - Top Right */}
          {requestBreakdown && (
            <div className="ml-4 bg-gray-50 border border-gray-200 rounded-lg px-3 py-2">
              <div className="space-y-1 text-xs">
                <div className="flex items-center space-x-2">
                  <span className="text-gray-700 whitespace-nowrap">📊 Total:</span>
                  <span className="font-semibold text-gray-900">
                    {(requestBreakdown.total / 1e6).toFixed(2)}M
                  </span>
                </div>
                <div className="flex items-center space-x-2">
                  <span className="text-gray-700 whitespace-nowrap">🛡️ Blocked:</span>
                  <span className="font-semibold text-gray-900">
                    {(requestBreakdown.blocked / 1e6).toFixed(2)}M
                  </span>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Value */}
        <div className={`flex items-baseline justify-between ${compact ? 'mb-2' : 'mb-4'}`}>
          <p className={`${compact ? 'text-2xl' : 'text-3xl'} font-bold text-gray-900`}>{formatted}</p>
          {change !== null && (
            <div className={`flex items-center space-x-1 text-sm font-medium ${
              change >= 0 ? 'text-green-600' : 'text-red-600'
            }`}>
              {change >= 0 ? (
                <TrendingUp className="w-4 h-4" />
              ) : (
                <TrendingDown className="w-4 h-4" />
              )}
              <span>{Math.abs(change).toFixed(1)}%</span>
            </div>
          )}
        </div>

        {/* Progress Bar */}
        {threshold && !isPrevious && (
          <div className="space-y-2">
            <div className="flex items-center justify-between text-xs text-gray-600">
              <span>Usage</span>
              <span className={`font-medium ${
                isOverThreshold ? 'text-red-600' :
                isWarning ? 'text-orange-700' :
                'text-gray-900'
              }`}>
                {percentage.toFixed(1)}%
              </span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2 overflow-hidden">
              <div
                className={`h-full rounded-full transition-all ${
                  isOverThreshold ? 'bg-gradient-to-r from-red-500 to-red-600' :
                  isWarning ? 'bg-gradient-to-r from-orange-500 to-orange-600' :
                  'bg-gradient-to-r from-blue-500 to-blue-600'
                }`}
                style={{ width: `${Math.min(percentage, 100)}%` }}
              />
            </div>
            <div className="flex items-center justify-between text-xs text-gray-500">
              <span>0</span>
              <span className="font-medium text-gray-700">
                Threshold: {formatThreshold(threshold)}
              </span>
            </div>
          </div>
        )}

        {/* Previous Period Label */}
        {isPrevious && (
          <div className="mt-4 pt-4 border-t border-gray-200">
            <p className="text-xs text-gray-500">Previous billing period</p>
          </div>
        )}

        {/* Alert Message */}
        {isOverThreshold && !isPrevious && (
          <div className="mt-4 pt-4 border-t border-red-200">
            <p className="text-xs text-red-700 font-medium flex items-center space-x-1">
              <AlertCircle className="w-3 h-3" />
              <span>Threshold exceeded!</span>
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

export default MetricCard;

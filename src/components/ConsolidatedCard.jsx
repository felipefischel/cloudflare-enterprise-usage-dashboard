import React from 'react';
import { Globe, Activity, Database, AlertCircle, Shield, Network, Users, TrendingUp, TrendingDown } from 'lucide-react';
import { ConfidenceBadge } from './ConfidenceIndicator';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';
import { format } from 'date-fns';
import { formatNumber } from '../utils/formatters';

function ConsolidatedCard({
  title,
  subtitle,
  value,
  formatted,
  threshold,
  percentage,
  icon,
  unit,
  color = '#3b82f6',
  timeSeries,
  dataKey,
  chartFormatter,
  yAxisLabel,
  confidence = null,
  isZoneFiltered = false,
  confidenceMetricType = 'HTTP requests',
  summaryBadge = null,
  zoneBreakdown = null,
  primaryZones = null,
  secondaryZones = null,
}) {
  const getIcon = () => {
    switch (icon) {
      case 'zones': return <Globe className="w-5 h-5" />;
      case 'requests': return <Activity className="w-5 h-5" />;
      case 'bandwidth': return <Database className="w-5 h-5" />;
      case 'shield': return <Shield className="w-5 h-5" />;
      case 'dns': return <Network className="w-5 h-5" />;
      case 'users': return <Users className="w-5 h-5" />;
      case 'table': return <Database className="w-5 h-5" />;
      case 'storage-mb': return <Database className="w-5 h-5" />;
      case 'minutes': return <Activity className="w-5 h-5" />;
      case 'images': return <Globe className="w-5 h-5" />;
      case 'neurons': return <Activity className="w-5 h-5" />;
      case 'operations': return <Activity className="w-5 h-5" />;
      case 'events': return <Activity className="w-5 h-5" />;
      case 'connections': return <Network className="w-5 h-5" />;
      case 'endpoints': return <Network className="w-5 h-5" />;
      case 'hostnames': return <Globe className="w-5 h-5" />;
      case 'mb-days': return <Database className="w-5 h-5" />;
      case 'cr-storage': return <Database className="w-5 h-5" />;
      default: return <Activity className="w-5 h-5" />;
    }
  };

  const formatThreshold = (value) => {
    if (!value) return '0';
    const cleanNumber = (num) => {
      const rounded = Math.round(num * 100) / 100;
      return parseFloat(rounded.toFixed(2)).toString();
    };
    const formatLargeNumber = (val, unitLabel) => {
      if (val >= 1e6) return `${cleanNumber(val / 1e6)}M ${unitLabel}`;
      if (val >= 1e3) return `${cleanNumber(val / 1e3)}K ${unitLabel}`;
      return `${cleanNumber(val)} ${unitLabel}`;
    };
    if (unit === 'Mbps') {
      if (value >= 1000) return `${cleanNumber(value / 1000)} Gbps`;
      return `${cleanNumber(value)} Mbps`;
    } else if (icon === 'bandwidth') {
      const tb = value / (1000 ** 4);
      if (tb >= 1) return `${cleanNumber(tb)} TB`;
      return `${cleanNumber(value / (1000 ** 3))} GB`;
    } else if (icon === 'dns') {
      return formatLargeNumber(value, 'queries');
    } else if (icon === 'requests' || icon === 'check' || icon === 'traffic' || icon === 'shield' || icon === 'activity') {
      return formatLargeNumber(value, 'requests');
    } else if (icon === 'upload' || icon === 'download') {
      return formatLargeNumber(value, 'ops');
    } else if (icon === 'cpu') {
      return formatLargeNumber(value, 'ms');
    } else if (icon === 'table') {
      if (value >= 1e9) return `${cleanNumber(value / 1e9)}B`;
      if (value >= 1e6) return `${cleanNumber(value / 1e6)}M`;
      if (value >= 1e3) return `${cleanNumber(value / 1e3)}K`;
      return cleanNumber(value);
    } else if (icon === 'storage-mb') {
      if (value >= 1000) return `${cleanNumber(value / 1000)} GB`;
      return `${cleanNumber(value)} MB`;
    } else if (icon === 'minutes') {
      return formatLargeNumber(value, 'min');
    } else if (icon === 'images') {
      return formatLargeNumber(value, 'images');
    } else if (icon === 'neurons') {
      return formatLargeNumber(value, 'neurons');
    } else if (icon === 'operations') {
      return formatLargeNumber(value, 'ops');
    } else if (icon === 'events') {
      return formatLargeNumber(value, 'events');
    } else if (icon === 'connections') {
      return formatLargeNumber(value, 'conns');
    } else if (icon === 'mb-days') {
      if (value >= 1e6) return `${cleanNumber(value / 1e6)} TB-days`;
      if (value >= 1e3) return `${cleanNumber(value / 1e3)} GB-days`;
      return `${cleanNumber(value)} MB-days`;
    } else if (icon === 'cr-storage') {
      if (value >= 1000) return `${cleanNumber(value / 1000)} PB`;
      if (value >= 1) return `${cleanNumber(value)} TB`;
      return `${cleanNumber(value * 1000)} GB`;
    } else if (icon === 'database') {
      return `${cleanNumber(value)} GB`;
    } else if (icon === 'zones') {
      return `${value.toLocaleString()} zones`;
    } else if (icon === 'endpoints') {
      return `${value.toLocaleString()} endpoints`;
    } else if (icon === 'users') {
      return `${value.toLocaleString()} seats`;
    }
    return value.toLocaleString();
  };

  const numPercentage = Number(percentage) || 0;
  const isOverThreshold = threshold && numPercentage > 100;
  const isWarning = threshold && numPercentage >= 90 && numPercentage <= 100;

  const gradientId = `gradient-${color.replace('#', '')}-${dataKey || 'default'}`;

  const formatXAxis = (timestamp) => {
    try {
      return format(new Date(timestamp), 'MMM yy');
    } catch {
      return timestamp;
    }
  };

  const formatTooltipValue = (val) => {
    if (chartFormatter) return chartFormatter(val);
    return formatNumber(val);
  };

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const dataPoint = payload[0].payload;
      const monthLabel = dataPoint.month
        ? format(new Date(dataPoint.timestamp), 'MMMM yyyy')
        : format(new Date(label), 'MMM dd, yyyy');
      return (
        <div className="bg-white border border-gray-200 rounded-lg shadow-lg p-3">
          <p className="text-sm font-medium text-gray-900 mb-1">{monthLabel}</p>
          <p className="text-sm text-gray-600">{formatTooltipValue(payload[0].value)}</p>
        </div>
      );
    }
    return null;
  };

  const getYAxisDomain = () => {
    if (!threshold || !timeSeries || timeSeries.length === 0) return ['auto', 'auto'];
    const maxDataValue = Math.max(...timeSeries.map(d => d[dataKey] || 0));
    const maxValue = Math.max(maxDataValue, threshold) * 1.1;
    return [0, maxValue];
  };

  return (
    <div className={`bg-white rounded-xl border-2 shadow-sm ${
      isOverThreshold ? 'border-red-300' :
      isWarning ? 'border-orange-300' :
      'border-gray-200'
    }`}>
      <div className="p-6">
        <div className="flex items-start justify-between mb-3">
          <div className="flex items-center space-x-3">
            <div className="p-2 rounded-lg" style={{ backgroundColor: `${color}15` }}>
              <div style={{ color }}>{getIcon()}</div>
            </div>
            <div>
              <div className="flex items-center space-x-2">
                <h3 className="text-lg font-semibold text-gray-900">{title}</h3>
                {isOverThreshold && <AlertCircle className="w-4 h-4 text-red-600" />}
                {confidence && (
                  <ConfidenceBadge
                    confidence={confidence}
                    metricName={title}
                    isZoneFiltered={isZoneFiltered}
                    actualValue={isZoneFiltered ? value : null}
                    metricType={confidenceMetricType}
                  />
                )}
              </div>
              {subtitle && <p className="text-sm text-gray-500">{subtitle}</p>}
            </div>
          </div>
          {summaryBadge && <div className="ml-4">{summaryBadge}</div>}
          {!summaryBadge && zoneBreakdown && (primaryZones || secondaryZones) && (
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
        </div>

        <div className="flex items-baseline space-x-3 mb-2">
          <p className="text-3xl font-bold text-gray-900">{formatted}</p>
          <span className="text-sm text-gray-500">month to date</span>
        </div>

        {threshold && (
          <div className="mb-4">
            <div className="flex items-center justify-between text-xs text-gray-600 mb-1">
              <span>Utilization</span>
              <span className={`font-medium ${
                isOverThreshold ? 'text-red-600' :
                isWarning ? 'text-orange-700' :
                'text-gray-900'
              }`}>
                {numPercentage < 0.1 && numPercentage > 0 ? '<0.1' : numPercentage.toFixed(1)}%
              </span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2.5 overflow-hidden">
              <div
                className={`h-full rounded-full transition-all ${
                  isOverThreshold ? 'bg-gradient-to-r from-red-500 to-red-600' :
                  isWarning ? 'bg-gradient-to-r from-orange-500 to-orange-600' :
                  'bg-gradient-to-r from-blue-500 to-blue-600'
                }`}
                style={{ width: `${Math.min(numPercentage, 100)}%` }}
              />
            </div>
            <div className="flex items-center justify-between text-xs text-gray-500 mt-1">
              <span>0</span>
              <span className="font-medium text-gray-700">Threshold: {formatThreshold(threshold)}</span>
            </div>
          </div>
        )}

        {isOverThreshold && (
          <div className="mb-4 bg-red-50 border border-red-200 rounded-lg px-3 py-2">
            <p className="text-xs text-red-700 font-medium flex items-center space-x-1">
              <AlertCircle className="w-3 h-3" />
              <span>Threshold exceeded!</span>
            </p>
          </div>
        )}

        {timeSeries && timeSeries.length > 0 && dataKey && (
          <div className="mt-4 pt-4 border-t border-gray-100">
            <h4 className="text-sm font-medium text-gray-700 mb-3">Monthly Usage</h4>
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={timeSeries}>
                <defs>
                  <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={color} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={color} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis
                  dataKey="timestamp"
                  tickFormatter={formatXAxis}
                  stroke="#6b7280"
                  style={{ fontSize: '11px' }}
                />
                <YAxis
                  domain={getYAxisDomain()}
                  tickFormatter={(val) => {
                    if (chartFormatter) return chartFormatter(val);
                    return val >= 1000000 ? `${(val / 1000000).toFixed(0)}M` : val.toLocaleString();
                  }}
                  stroke="#6b7280"
                  style={{ fontSize: '11px' }}
                  label={{
                    value: yAxisLabel || 'Usage',
                    angle: -90,
                    position: 'left',
                    offset: 10,
                    style: { fontSize: '10px', fill: '#6b7280', textAnchor: 'middle' }
                  }}
                />
                <Tooltip content={<CustomTooltip />} />
                {threshold && (
                  <ReferenceLine
                    y={threshold}
                    stroke="#4b5563"
                    strokeDasharray="5 5"
                    strokeWidth={2}
                  />
                )}
                <Area
                  type="monotone"
                  dataKey={dataKey}
                  stroke={color}
                  strokeWidth={2}
                  fill={`url(#${gradientId})`}
                />
              </AreaChart>
            </ResponsiveContainer>
            {threshold && (
              <div className="mt-2 flex items-center justify-center">
                <div className="flex items-center space-x-2">
                  <div className="w-6 border-t-2 border-dashed border-gray-600"></div>
                  <span className="text-xs text-gray-600">Threshold</span>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export default ConsolidatedCard;

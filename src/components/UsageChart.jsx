import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Area,
  AreaChart,
  ReferenceLine,
} from 'recharts';
import { format } from 'date-fns';
import { formatNumber } from '../utils/formatters';

function UsageChart({ data, dataKey, title, color, formatter, threshold, yAxisLabel }) {
  // Create unique gradient ID based on color (not dataKey, since multiple charts share same dataKey)
  const gradientId = `gradient-${color.replace('#', '')}`;
  
  const formatXAxis = (timestamp) => {
    try {
      // Format as "MMM YYYY" for monthly data (e.g., "Oct 2025")
      return format(new Date(timestamp), 'MMM yy');
    } catch {
      return timestamp;
    }
  };

  const formatTooltipValue = (value) => {
    if (formatter) {
      return formatter(value);
    }
    return formatNumber(value);
  };

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      // Get the month string from the data point
      const dataPoint = payload[0].payload;
      const monthLabel = dataPoint.month 
        ? format(new Date(dataPoint.timestamp), 'MMMM yyyy')
        : format(new Date(label), 'MMM dd, yyyy');
      
      return (
        <div className="bg-white border border-gray-200 rounded-lg shadow-lg p-3">
          <p className="text-sm font-medium text-gray-900 mb-1">
            {monthLabel}
          </p>
          <p className="text-sm text-gray-600">
            {formatTooltipValue(payload[0].value)}
          </p>
        </div>
      );
    }
    return null;
  };

  // Calculate Y-axis domain to include threshold
  const getYAxisDomain = () => {
    if (!threshold || !data || data.length === 0) {
      return ['auto', 'auto'];
    }
    
    // Find max value in data
    const maxDataValue = Math.max(...data.map(d => d[dataKey] || 0));
    
    // Set max to whichever is higher: data or threshold (with 10% padding)
    const maxValue = Math.max(maxDataValue, threshold) * 1.1;
    
    return [0, maxValue];
  };

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <h3 className="text-lg font-semibold text-gray-900 mb-4">{title}</h3>
      <ResponsiveContainer width="100%" height={300}>
        <AreaChart data={data}>
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
            style={{ fontSize: '12px' }}
            label={{ value: 'Month', position: 'insideBottom', offset: -5, style: { fontSize: '12px', fill: '#6b7280' } }}
          />
          <YAxis
            domain={getYAxisDomain()}
            tickFormatter={(value) => {
              if (formatter) {
                return formatter(value);
              }
              return value >= 1000000 ? `${(value / 1000000).toFixed(0)}M` : value.toLocaleString();
            }}
            stroke="#6b7280"
            style={{ fontSize: '12px' }}
            label={{ 
              value: yAxisLabel || 'Usage', 
              angle: -90, 
              position: 'left',
              offset: 10,
              style: { fontSize: '11px', fill: '#6b7280', textAnchor: 'middle' } 
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
        <div className="mt-3 pt-3 border-t border-gray-200">
          <div className="flex items-center justify-center">
            <div className="flex items-center space-x-2">
              <div className="w-8 border-t-2 border-dashed border-gray-600"></div>
              <span className="text-xs font-medium text-gray-700">
                Threshold: {formatter ? formatter(threshold) : formatNumber(threshold)}
              </span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default UsageChart;

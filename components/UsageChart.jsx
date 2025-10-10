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
} from 'recharts';
import { format } from 'date-fns';
import { formatNumber } from '../utils/formatters';

function UsageChart({ data, dataKey, title, color, formatter }) {
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

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <h3 className="text-lg font-semibold text-gray-900 mb-4">{title}</h3>
      <ResponsiveContainer width="100%" height={300}>
        <AreaChart data={data}>
          <defs>
            <linearGradient id={`gradient-${dataKey}`} x1="0" y1="0" x2="0" y2="1">
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
          />
          <YAxis
            tickFormatter={(value) => {
              if (formatter) {
                return formatter(value);
              }
              return value >= 1000000 ? `${(value / 1000000).toFixed(0)}M` : value.toLocaleString();
            }}
            stroke="#6b7280"
            style={{ fontSize: '12px' }}
          />
          <Tooltip content={<CustomTooltip />} />
          <Area
            type="monotone"
            dataKey={dataKey}
            stroke={color}
            strokeWidth={2}
            fill={`url(#gradient-${dataKey})`}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

export default UsageChart;

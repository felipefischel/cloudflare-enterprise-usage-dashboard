# SKU Implementation Guide

**Purpose:** This document provides guidelines for implementing new SKUs (billable products/features) in the Cloudflare Enterprise Usage Dashboard. Use this as a reference when building new features.

---

## Table of Contents
1. [Overview](#overview)
2. [Configuration](#configuration)
3. [Backend Implementation](#backend-implementation)
4. [Frontend Implementation](#frontend-implementation)
5. [Implementation Checklist](#implementation-checklist)
6. [Best Practices](#best-practices)
7. [Common Pitfalls](#common-pitfalls)

---

## Overview

### What is a SKU?

Each SKU represents a billable Cloudflare product (e.g., HTTP Requests, Bot Management, API Shield). All SKUs must be:

- **Configurable**: Enable/disable via settings
- **Measurable**: Track usage metrics over time
- **Alertable**: Support threshold-based Slack notifications
- **Scalable**: Support multi-account aggregation
- **Cacheable**: Integrate with 6-hour pre-warming system

### Service Categories

SKUs are organized into categories:
- **Application Services**: Core (Zones, HTTP, Bandwidth, DNS), Bot Management, API Shield, Page Shield, Advanced Rate Limiting
- **Zero Trust**: (future)
- **Network Services**: (future)
- **Developer Services**: (future)

---

## Configuration

### 1. Config Structure

Every SKU must have a configuration entry in the `config:default` KV key.

**Example:**
```javascript
{
  "accountIds": ["account-1", "account-2"],
  "applicationServices": {
    "skuName": {
      "enabled": true,           // Required: Enable/disable flag
      "threshold": 1000000000,   // Optional: Alert threshold
      "zones": ["zone-id-1"]     // Optional: Zone filters (if zone-specific)
    }
  },
  "slackWebhook": "https://...",
  "alertsEnabled": true
}
```

**Requirements:**
- ✅ Must have `enabled` boolean field
- ✅ Must be nested under service category (e.g., `applicationServices`)
- ✅ Should include threshold for alerting
- ✅ Should include zone filters if zone-specific

### 2. Settings UI (ConfigFormNew.jsx)

Add configuration form in settings:

```jsx
<div className="border-b border-gray-200 pb-6">
  {/* Header with Enable/Disable Toggle */}
  <div className="flex items-start justify-between mb-4">
    <div>
      <h4 className="text-lg font-semibold text-gray-900">SKU Name</h4>
      <p className="text-sm text-gray-600 mt-1">
        Brief description of what this SKU measures
      </p>
    </div>
    <label className="flex items-center space-x-2 cursor-pointer">
      <input
        type="checkbox"
        checked={formData.category.skuName.enabled}
        onChange={(e) => {
          setFormData(prev => ({
            ...prev,
            category: {
              ...prev.category,
              skuName: {
                ...prev.category.skuName,
                enabled: e.target.checked
              }
            }
          }));
        }}
        className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
      />
      <span className="text-sm font-medium text-gray-700">Enable</span>
    </label>
  </div>

  {/* Inputs (only shown when enabled) */}
  {formData.category.skuName.enabled && (
    <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Threshold
        </label>
        <input
          type="number"
          value={formData.category.skuName.threshold}
          onChange={(e) => {/* update state */}}
          className="w-full px-4 py-2 border border-gray-300 rounded-lg"
        />
      </div>
      
      {/* Zone selector if zone-specific */}
    </div>
  )}
</div>
```

**Requirements:**
- ✅ Enable/disable checkbox prominently displayed
- ✅ Conditional rendering of inputs based on `enabled` state
- ✅ Clear labels and descriptions
- ✅ Validation for threshold values

---

## Backend Implementation

### 1. Selective Fetching

Only fetch metrics that are enabled:

```javascript
// In worker.js - Progressive Loading Phase 3
async function fetchMetrics(config, env) {
  let skuMetrics = null;
  
  // Only fetch if enabled
  if (config?.category?.skuName?.enabled) {
    console.log('🔧 Fetching SKU metrics...');
    const skuConfig = config.category.skuName;
    
    const skuPromises = accountIds.map(accountId =>
      fetchSKUForAccount(apiKey, accountId, skuConfig, env)
        .then(data => ({ accountId, data })) // Include accountId
    );
    
    const skuResults = await Promise.allSettled(skuPromises);
    const skuData = skuResults
      .filter(result => result.status === 'fulfilled' && result.value?.data)
      .map(result => result.value);
    
    if (skuData.length > 0) {
      skuMetrics = aggregateSKUData(skuData, skuConfig);
    }
  } else {
    console.log('⏭️ SKU disabled - skipping fetch');
  }
  
  return skuMetrics;
}
```

### 2. Per-Account Fetching

Create a function to fetch data for a single account:

```javascript
async function fetchSKUForAccount(apiKey, accountId, skuConfig, env) {
  if (!skuConfig || !skuConfig.enabled) {
    return null; // Skip if disabled
  }
  
  // Calculate date ranges
  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59);
  
  // Fetch current month data
  const currentData = await fetchCurrentMonthData(apiKey, accountId, currentMonthStart, now);
  
  // Fetch previous month data (check cache first)
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;
  let previousData = await env.CONFIG_KV.get(
    `monthly-sku-stats:${accountId}:${previousMonthKey}`,
    'json'
  );
  
  if (!previousData && now.getDate() >= 2) {
    // Fetch and cache
    previousData = await fetchPreviousMonthData(apiKey, accountId, previousMonthStart, previousMonthEnd);
    await env.CONFIG_KV.put(
      `monthly-sku-stats:${accountId}:${previousMonthKey}`,
      JSON.stringify(previousData),
      { expirationTtl: 31536000 } // 1 year
    );
  }
  
  // Load historical data for timeSeries
  const historicalData = await getHistoricalSKUData(env, accountId);
  
  // Build timeSeries
  const timeSeries = [
    ...historicalData,
    {
      month: previousMonthKey,
      timestamp: previousMonthStart.toISOString(),
      mainMetric: previousData?.mainMetric || 0,
    },
    {
      month: `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`,
      timestamp: currentMonthStart.toISOString(),
      mainMetric: currentData.mainMetric,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  return {
    current: currentData,
    previous: previousData || { mainMetric: 0, zones: [] },
    timeSeries: timeSeries,
  };
}
```

### 3. Multi-Account Aggregation

Aggregate data across accounts and store per-account breakdown:

```javascript
function aggregateSKUData(skuData, skuConfig) {
  // Merge timeSeries across accounts
  const timeSeriesMap = new Map();
  skuData.forEach(({ accountId, data }) => {
    if (data.timeSeries) {
      data.timeSeries.forEach(entry => {
        const existing = timeSeriesMap.get(entry.month);
        if (existing) {
          existing.mainMetric += entry.mainMetric || 0;
        } else {
          timeSeriesMap.set(entry.month, {
            month: entry.month,
            timestamp: entry.timestamp,
            mainMetric: entry.mainMetric || 0,
          });
        }
      });
    }
  });

  const mergedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    enabled: true,
    threshold: skuConfig.threshold,
    current: {
      mainMetric: skuData.reduce((sum, {data}) => sum + (data.current.mainMetric || 0), 0),
      zones: skuData.flatMap(({data}) => data.current.zones || []),
    },
    previous: {
      mainMetric: skuData.reduce((sum, {data}) => sum + (data.previous.mainMetric || 0), 0),
      zones: skuData.flatMap(({data}) => data.previous.zones || []),
    },
    timeSeries: mergedTimeSeries,
    
    // CRITICAL: Store per-account data for dashboard filtering
    perAccountData: skuData.map(({accountId, data}) => ({
      accountId,
      current: data.current,
      previous: data.previous,
      timeSeries: data.timeSeries,
    })),
  };
}
```

**Requirements:**
- ✅ Return `null` if SKU is disabled
- ✅ Fetch both current and previous month
- ✅ Cache completed months in KV (1 year TTL)
- ✅ Build timeSeries with historical data
- ✅ Store `perAccountData` for dashboard account filtering
- ✅ Use `Promise.allSettled` for reliability

### 4. Cache Integration

**Pre-warming (Cron Trigger):**
```javascript
// In prewarmCache() function
if (config?.category?.skuName?.enabled) {
  skuMetrics = await fetchMetrics(config, env);
  console.log(`Pre-warm: SKU data cached`);
}

// Add to cache
await env.CONFIG_KV.put(
  `pre-warmed:${accountIds.join(',')}`,
  JSON.stringify({
    timestamp: Date.now(),
    data: {
      ...coreMetrics,
      ...(skuMetrics && { skuName: skuMetrics }),
    }
  }),
  { expirationTtl: 21600 } // 6 hours
);
```

**Cache Validation:**
```javascript
// Check if cache is complete
if (config?.category?.skuName?.enabled) {
  if (!cachedData.data.skuName || !cachedData.data.skuName.timeSeries) {
    console.log('⚠️ Cache incomplete: SKU missing');
    cacheIsComplete = false;
  }
}
```

### 5. Alert Integration

Add threshold checking:

```javascript
function checkThresholds(metrics, config) {
  const alerts = [];
  
  // Check SKU threshold
  if (metrics?.category?.skuName?.enabled && metrics.category.skuName.threshold) {
    const current = metrics.category.skuName.current.mainMetric;
    const threshold = metrics.category.skuName.threshold;
    const percentage = (current / threshold) * 100;
    
    if (percentage >= 90) {
      alerts.push({
        metric: 'SKU Name',
        current: formatNumber(current),
        threshold: formatNumber(threshold),
        percentage: percentage.toFixed(1),
      });
    }
  }
  
  return alerts;
}
```

---

## Frontend Implementation

### 1. Dashboard Metrics Card (Dashboard.jsx)

Display in "Usage Metrics" section:

```jsx
{/* Only render if enabled */}
{displayMetrics?.category?.skuName && 
 displayMetrics.category.skuName.enabled && (
  <MetricCard
    title="SKU Metric Name"
    value={usageViewMode === 'current' 
      ? displayMetrics.category.skuName.current.mainMetric 
      : displayMetrics.category.skuName.previous.mainMetric}
    formatted={formatNumber(usageViewMode === 'current' 
      ? displayMetrics.category.skuName.current.mainMetric 
      : displayMetrics.category.skuName.previous.mainMetric)}
    threshold={displayMetrics.category.skuName.threshold}
    percentage={calculatePercentage(
      usageViewMode === 'current' 
        ? displayMetrics.category.skuName.current.mainMetric 
        : displayMetrics.category.skuName.previous.mainMetric,
      displayMetrics.category.skuName.threshold
    )}
    icon="shield"
    unit="M"
    accentColor="#9333ea"
  />
)}
```

### 2. Monthly Trends Chart

Add to "Monthly Usage Trends" section:

```jsx
{displayMetrics?.category?.skuName && 
 displayMetrics.category.skuName.enabled && 
 displayMetrics.category.skuName.timeSeries && (
  <UsageChart
    data={displayMetrics.category.skuName.timeSeries}
    dataKey="mainMetric"
    title="SKU Name: Metric by Month"
    color="#9333ea"
    formatter={formatNumber}
  />
)}
```

### 3. Zone Breakdown Table

Add option to "Breakdown by Zones" dropdown and table:

```jsx
{/* Add to dropdown */}
<select value={zoneBreakdownSKU} onChange={(e) => setZoneBreakdownSKU(e.target.value)}>
  <option value="appServices">App Services</option>
  {displayMetrics?.category?.skuName?.enabled && (
    <option value="skuName">SKU Name</option>
  )}
</select>

{/* Add table */}
{zoneBreakdownSKU === 'skuName' && (() => {
  const zones = zonesViewMode === 'current' 
    ? displayMetrics?.category?.skuName?.current?.zones 
    : displayMetrics?.category?.skuName?.previous?.zones;
  
  if (!zones || zones.length === 0) {
    return <div className="text-gray-500 text-center py-8">No zone data available</div>;
  }
  
  return (
    <div className="bg-gray-50 border border-gray-200 rounded-lg overflow-hidden">
      <div className="max-h-96 overflow-y-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-100 sticky top-0 z-10">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Zone</th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">Metric</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {zones.map((zone) => (
              <tr key={zone.zoneId} className="hover:bg-gray-50">
                <td className="px-6 py-4 text-sm text-gray-900">{zone.zoneName || zone.zoneId}</td>
                <td className="px-6 py-4 text-sm text-gray-900 text-right">{formatNumber(zone.metric)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
})()}
```

### 4. Account Filtering Support

Implement filtering in `getFilteredData()`:

```javascript
const getFilteredData = () => {
  if (selectedAccount === 'all') {
    return displayMetrics; // Return aggregated data
  }
  
  // Filter SKU to specific account
  if (displayMetrics?.category?.skuName?.perAccountData) {
    const accountData = displayMetrics.category.skuName.perAccountData.find(
      d => d.accountId === selectedAccount
    );
    
    if (accountData) {
      return {
        ...displayMetrics,
        category: {
          ...displayMetrics.category,
          skuName: {
            ...displayMetrics.category.skuName,
            current: accountData.current,
            previous: accountData.previous,
            timeSeries: accountData.timeSeries,
          }
        }
      };
    }
  }
  
  return displayMetrics;
};
```

---

## Implementation Checklist

### Configuration (ConfigFormNew.jsx)
- [ ] Add SKU to config schema in `useState` initialization
- [ ] Add enable/disable checkbox with clear label
- [ ] Add threshold input (conditional on enabled)
- [ ] Add zone selector if zone-specific (conditional on enabled)
- [ ] Add validation in `validate()` function
- [ ] Update `handleSubmit()` to save config

### Backend (worker.js)
- [ ] Create `fetchSKUForAccount(apiKey, accountId, skuConfig, env)` function
- [ ] Fetch current month data from Cloudflare API
- [ ] Fetch/cache previous month data (KV: `monthly-sku-stats:{accountId}:YYYY-MM`)
- [ ] Create `getHistoricalSKUData(env, accountId)` function for loading history
- [ ] Build timeSeries array (historical + previous + current)
- [ ] Create aggregation function with `perAccountData`
- [ ] Add to Phase 3 progressive loading with selective fetch check
- [ ] Add to `prewarmCache()` function with selective fetch check
- [ ] Add to cache validation check
- [ ] Add threshold check in alert system

### Frontend (Dashboard.jsx)
- [ ] Add MetricCard in Usage Metrics section (conditional rendering)
- [ ] Add UsageChart in Monthly Trends section (conditional rendering)
- [ ] Add option to zone breakdown dropdown (conditional)
- [ ] Add zone breakdown table (conditional)
- [ ] Update `getFilteredData()` to support account filtering
- [ ] Add console logging for debugging

### Testing
- [ ] Test enable/disable toggle in settings
- [ ] Test with single account
- [ ] Test with multiple accounts
- [ ] Test account filtering dropdown
- [ ] Test threshold alerts (90% trigger)
- [ ] Test zone breakdown table
- [ ] Test monthly trends chart
- [ ] Test cache hit/miss scenarios
- [ ] Test pre-warming via cron trigger

---

## Best Practices

### Naming Conventions
- **Config keys:** `camelCase` (e.g., `botManagement`, `apiShield`)
- **KV keys:** `kebab-case` (e.g., `monthly-bot-stats`, `historical-api-shield`)
- **Display names:** Title Case (e.g., "Bot Management", "API Shield")

### Error Handling
- Always use `Promise.allSettled` for multiple accounts
- Return `null` if SKU is disabled
- Log errors with clear messages (use emojis for visual scanning: 🔧 ⏭️ ❌ ✅)
- Don't crash on errors - show user-friendly messages

### Performance
- Cache historical data for 6 hours (TTL: 21600)
- Store completed months with 1 year TTL (TTL: 31536000)
- Use progressive loading (don't block UI)
- Aggregate efficiently (use Maps for deduplication)

### User Experience
- Clear enable/disable controls prominently displayed
- Conditional rendering based on enabled state
- Loading states and progress indicators
- Meaningful error messages
- Always include zone names (not just IDs) in zone data

### Code Quality
- Consistent code structure across SKUs
- Reusable functions and utilities
- Clear comments explaining complex logic
- Follow existing patterns in the codebase
- Variable scope matters - declare shared variables at function scope

---

## Common Pitfalls

### 1. ❌ Forgetting perAccountData
**Problem:** Account filtering dropdown doesn't work for the SKU

**Solution:** Always include `perAccountData` in aggregated metrics:
```javascript
return {
  enabled: true,
  current: aggregatedCurrent,
  previous: aggregatedPrevious,
  timeSeries: mergedTimeSeries,
  perAccountData: skuData.map(({accountId, data}) => ({
    accountId,
    current: data.current,
    previous: data.previous,
    timeSeries: data.timeSeries,
  })),
};
```

### 2. ❌ Missing Zone Names
**Problem:** Zone breakdown tables show IDs instead of friendly names

**Solution:** Create zone name lookup and include in all zone data:
```javascript
const zoneNameMap = {};
enterpriseZones.forEach(z => {
  zoneNameMap[z.id] = z.name;
});

zones.push({
  zoneId: zone.id,
  zoneName: zoneNameMap[zone.id] || zone.id, // Always include zoneName
  metric: zone.metric,
});
```

### 3. ❌ Only Including Current Month in TimeSeries
**Problem:** Charts don't show previous month data

**Solution:** Include both months we already fetched:
```javascript
const timeSeries = [
  ...historicalData,
  // Previous month (we fetch this!)
  {
    month: previousMonthKey,
    timestamp: previousMonthStart.toISOString(),
    metric: previousTotal,
  },
  // Current month
  {
    month: currentMonthKey,
    timestamp: currentMonthStart.toISOString(),
    metric: currentTotal,
  }
].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
```

### 4. ❌ Wrong Variable Scope
**Problem:** Add-ons can't access core metrics data

**Solution:** Declare variables at function scope (not inside if blocks):
```javascript
// ✅ CORRECT: Function scope
let coreMetrics = null;
let successfulMetrics = [];

if (config?.applicationServices?.core?.enabled !== false) {
  successfulMetrics = await fetchMetrics(); // Assignment (no const/let)
}

// Now accessible to add-ons
if (config?.addOn?.enabled && successfulMetrics.length > 0) {
  // Can use successfulMetrics here
}
```

### 5. ❌ Duplicate Gradient IDs in Charts
**Problem:** Multiple charts show wrong colors

**Solution:** Use color-based gradient IDs (not dataKey):
```javascript
// ✅ CORRECT
const gradientId = `gradient-${color.replace('#', '')}`; // gradient-9333ea
<linearGradient id={gradientId}>
  <stop offset="5%" stopColor={color} stopOpacity={0.3} />
</linearGradient>
<Area fill={`url(#${gradientId})`} />
```

### 6. ❌ Refetching Data for Zone-Based Add-ons
**Problem:** Making unnecessary API calls, wasting resources

**Solution:** Reuse existing zone data from App Services Core:
```javascript
// ✅ CORRECT: Filter existing data
const currentZones = accountData.zoneBreakdown.zones
  .filter(zone => configuredZones.has(zone.zoneTag))
  .map(zone => ({
    zoneId: zone.zoneTag,
    zoneName: zone.zoneName,
    requests: zone.requests, // Already fetched!
  }));

// NO new API calls needed!
```

---

## Example: Bot Management Implementation

Bot Management is a complete reference implementation. Key files:
- **Backend:** `src/worker.js` - Functions starting with `fetchBotManagementForAccount`
- **Config:** `src/components/ConfigFormNew.jsx` - Bot Management section
- **Dashboard:** `src/components/Dashboard.jsx` - Bot Management metrics and charts

Study this implementation as a template for new SKUs.

---

## Summary

**When implementing a new SKU:**
1. Add config structure with `enabled` flag
2. Create settings UI with enable/disable toggle
3. Implement `fetchSKUForAccount()` with current + previous month
4. Build timeSeries with historical data
5. Aggregate across accounts with `perAccountData`
6. Add to Phase 3 and pre-warming (with selective fetch checks)
7. Add MetricCard, UsageChart, and zone breakdown to Dashboard
8. Add threshold checking for alerts
9. Test thoroughly with single and multiple accounts

**Key principle:** Only fetch what's enabled, store what's needed, display what's relevant.

import React, { useState, useEffect } from 'react';
import MetricCard from './MetricCard';
import UsageChart from './UsageChart';
import ZonesList from './ZonesList';
import { RefreshCw, Calendar, AlertCircle, Bell, BellOff, Filter } from 'lucide-react';
import { formatNumber, formatRequests, formatBandwidthTB, formatBytes } from '../utils/formatters';

function Dashboard({ config }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [metrics, setMetrics] = useState(null);
  const [zones, setZones] = useState(null);
  const [alertsEnabled, setAlertsEnabled] = useState(config?.alertsEnabled || false);
  const [lastChecked, setLastChecked] = useState(null);
  const [usageViewMode, setUsageViewMode] = useState('current'); // 'current' or 'previous'
  const [zonesViewMode, setZonesViewMode] = useState('previous'); // 'current' or 'previous'
  const [selectedAccount, setSelectedAccount] = useState('all'); // 'all' or specific accountId

  useEffect(() => {
    // Load alerts state from config
    if (config?.alertsEnabled !== undefined) {
      setAlertsEnabled(config.alertsEnabled);
    }
  }, [config?.alertsEnabled]);

  useEffect(() => {
    fetchData();
    // Auto-refresh every 5 minutes
    const interval = setInterval(fetchData, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, [config]);

  const fetchData = async () => {
    // Support both old accountId and new accountIds format
    const accountIds = config?.accountIds || (config?.accountId ? [config.accountId] : []);
    
    // Don't fetch if config is missing or incomplete
    if (!config || !config.apiKey || accountIds.length === 0) {
      setError('API credentials not configured. Please configure them in Settings.');
      setLoading(false);
      return;
    }

    setLoading(true);
    setError(null);

    try {
      // Fetch metrics and zones in parallel
      const [metricsResponse, zonesResponse] = await Promise.all([
        fetch('/api/metrics', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            apiKey: config.apiKey,
            accountIds: accountIds,
            // Legacy fallback
            accountId: accountIds[0],
          }),
        }),
        fetch('/api/zones', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            apiKey: config.apiKey,
            accountIds: accountIds,
            // Legacy fallback
            accountId: accountIds[0],
          }),
        }),
      ]);

      if (!metricsResponse.ok || !zonesResponse.ok) {
        const metricsError = !metricsResponse.ok ? await metricsResponse.json() : null;
        const zonesError = !zonesResponse.ok ? await zonesResponse.json() : null;
        const errorMsg = metricsError?.error || zonesError?.error || 'Failed to fetch data from Cloudflare API';
        throw new Error(errorMsg);
      }

      const metricsData = await metricsResponse.json();
      const zonesData = await zonesResponse.json();

      setMetrics(metricsData);
      setZones(zonesData);
      setLastChecked(new Date());

      // Always check thresholds if Slack webhook is configured (toggle just controls UI indicator)
      if (config.slackWebhook && alertsEnabled) {
        checkThresholds(metricsData, zonesData);
      }
    } catch (err) {
      console.error('Error fetching data:', err);
    } finally {
      setLoading(false);
    }
  };

  const checkThresholds = async (metricsData, zonesData, forceTest = false) => {
    const accountIds = config?.accountIds || (config?.accountId ? [config.accountId] : []);
    
    try {
      const response = await fetch('/api/webhook/check', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          metrics: {
            zones: zonesData?.enterprise || 0,
            requests: metricsData?.current?.cleanRequests || 0,
            bandwidth: metricsData?.current?.bytes || 0,
          },
          thresholds: {
            zones: config.thresholdZones,
            requests: config.thresholdRequests,
            bandwidth: config.thresholdBandwidth,
          },
          slackWebhook: config.slackWebhook,
          accountIds: accountIds,
          // Legacy support
          accountId: accountIds[0],
          forceTest,
        }),
      });

      const result = await response.json();
      setLastChecked(new Date());
      
      // Only show notification when:
      // 1. Manual test via "Test Now" button
      // 2. Slack notification was actually sent (not skipped)
      if (forceTest) {
        alert(`✅ Test notification sent!\n\n${result.message || 'Slack webhook test completed.'}`);
      } else if (result.slackSent) {
        alert(`🚨 Alert sent!\n\nSlack notification sent for ${result.alerts?.length || 0} threshold breach(es).`);
      }
      // Silent otherwise (no alerts triggered or already sent this month)
    } catch (error) {
      console.error('Error checking thresholds:', error);
      alert('❌ Failed to check thresholds. Please try again.');
    }
  };  

  const toggleAlerts = async () => {
    const newState = !alertsEnabled;
    setAlertsEnabled(newState);
    
    // Save alerts state to config
    try {
      await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          userId: 'default',
          config: {
            ...config,
            alertsEnabled: newState,
          },
        }),
      });
    } catch (err) {
      console.error('Failed to save alerts state:', err);
    }
  };

  // Get filtered data based on selected account
  const getFilteredData = () => {
    if (!metrics) return { metrics: null, zones: null };
    
    // If "all accounts" selected, return aggregated data
    if (selectedAccount === 'all') {
      return { 
        metrics: {
          current: metrics.current,
          previous: metrics.previous,
          timeSeries: metrics.timeSeries,
          zoneBreakdown: metrics.zoneBreakdown,
          previousMonthZoneBreakdown: metrics.previousMonthZoneBreakdown,
        }, 
        zones 
      };
    }
    
    // Find data for selected account
    const accountData = metrics.perAccountData?.find(acc => acc.accountId === selectedAccount);
    if (!accountData) {
      return { metrics: null, zones: null };
    }
    
    // Filter zones to only those from this account
    const accountZones = zones?.zones?.filter(zone => {
      const zoneMetric = accountData.zoneBreakdown.zones.find(z => z.zoneTag === zone.id);
      return !!zoneMetric;
    });
    
    return {
      metrics: accountData,
      zones: accountZones ? { ...zones, zones: accountZones, enterprise: accountZones.length } : zones
    };
  };

  const filteredData = getFilteredData();
  const displayMetrics = filteredData.metrics;
  const displayZones = filteredData.zones;

  const calculatePercentage = (current, threshold) => {
    if (!threshold || threshold === 0) return 0;
    return Math.min((current / threshold) * 100, 100);
  };

  if (loading && !metrics) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="text-center">
          <RefreshCw className="w-12 h-12 text-blue-500 animate-spin mx-auto mb-4" />
          <p className="text-gray-600">Loading your usage data...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <div className="flex items-start space-x-3">
          <AlertCircle className="w-6 h-6 text-red-600 flex-shrink-0 mt-0.5" />
          <div>
            <h3 className="font-semibold text-red-900 mb-1">Error Loading Data</h3>
            <p className="text-red-700 text-sm">{error}</p>
            <p className="text-red-600 text-xs mt-2">
              Please check your API key and Account ID in settings.
            </p>
          </div>
        </div>
      </div>
    );
  }

  // Get account list for dropdown with names
  const accountIds = config?.accountIds || [];
  const accountsWithNames = metrics?.perAccountData?.map(acc => ({
    id: acc.accountId,
    name: acc.accountName || `${acc.accountId.substring(0, 8)}...${acc.accountId.substring(acc.accountId.length - 4)}`
  })) || accountIds.map(id => ({
    id,
    name: `${id.substring(0, 8)}...${id.substring(id.length - 4)}`
  }));
  const showAccountFilter = accountIds.length > 1;

  return (
    <div className="space-y-6">
      {/* Account Filter & Alert Toggle */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Account Filter */}
        {showAccountFilter && (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
            <div className="flex items-center space-x-3">
              <Filter className="w-5 h-5 text-slate-600" />
              <div className="flex-1">
                <h3 className="font-semibold text-gray-900 mb-2">Account Filter</h3>
                <select
                  value={selectedAccount}
                  onChange={(e) => setSelectedAccount(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  <option value="all">All Accounts (Aggregated)</option>
                  {accountsWithNames.map((account) => (
                    <option key={account.id} value={account.id}>
                      {account.name}
                    </option>
                  ))}
                </select>
                <p className="text-xs text-gray-500 mt-1">
                  {selectedAccount === 'all' ? 'Showing combined data from all accounts' : 'Showing data for selected account only'}
                </p>
              </div>
            </div>
          </div>
        )}
        
        {/* Alert Toggle */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-start gap-4 flex-1">
              {alertsEnabled ? (
                <Bell className="w-5 h-5 text-blue-600 mt-0.5 flex-shrink-0" />
              ) : (
                <BellOff className="w-5 h-5 text-gray-400 mt-0.5 flex-shrink-0" />
              )}
              <div className="flex-1 min-w-0">
                <h3 className="font-semibold text-gray-900 mb-1">Threshold Alerts</h3>
                <p className="text-sm text-gray-600 leading-relaxed">
                  Get notified when usage reaches 90% of contracted limits
                </p>
              </div>
            </div>
            <div className="flex items-center gap-3 flex-shrink-0">
              {alertsEnabled && metrics && zones && (
                <button
                  onClick={() => checkThresholds(metrics, zones, true)}
                  className="px-4 py-2 text-sm font-medium bg-blue-100 text-blue-700 rounded-lg hover:bg-blue-200 transition-colors whitespace-nowrap"
                >
                  Send Now
                </button>
              )}
              <button
                onClick={toggleAlerts}
                className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                  alertsEnabled ? 'bg-blue-600' : 'bg-gray-300'
                }`}
              >
              <span
                className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                  alertsEnabled ? 'translate-x-6' : 'translate-x-1'
                }`}
              />
            </button>
          </div>
          </div>
          {lastChecked && (
            <p className="text-xs text-gray-500 mt-4 ml-9">
              Last checked: {lastChecked.toLocaleTimeString()}
            </p>
          )}
        </div>
      </div>

      {/* Action Bar */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Usage Overview</h2>
          <p className="text-sm text-gray-600 mt-1">
            Monitor your Cloudflare Enterprise consumption
          </p>
        </div>
        <button
          onClick={fetchData}
          disabled={loading}
          className="flex items-center space-x-2 px-5 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-sm font-medium"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          <span>Refresh</span>
        </button>
      </div>

      {/* Usage Metrics Section */}
      <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-lg">
        <div className="bg-gradient-to-r from-slate-700 to-slate-600 rounded-lg px-6 py-4 mb-6">
          <div className="flex items-center justify-between">
            <h3 className="text-2xl font-semibold text-white tracking-tight">Usage Metrics</h3>
            
            {/* Toggle */}
            <div className="flex items-center bg-white rounded-lg p-1 shadow-sm">
              <button
                onClick={() => setUsageViewMode('current')}
                className={`flex items-center space-x-2 px-4 py-2 rounded-md transition-all font-medium text-sm ${
                  usageViewMode === 'current'
                    ? 'bg-blue-600 text-white shadow-sm'
                    : 'text-gray-600 hover:bg-gray-50'
                }`}
              >
                <Calendar className="w-4 h-4" />
                <span>Current</span>
              </button>
              <button
                onClick={() => setUsageViewMode('previous')}
                className={`flex items-center space-x-2 px-4 py-2 rounded-md transition-all font-medium text-sm ${
                  usageViewMode === 'previous'
                    ? 'bg-blue-600 text-white shadow-sm'
                    : 'text-gray-600 hover:bg-gray-50'
                }`}
              >
                <Calendar className="w-4 h-4" />
                <span>Last Month</span>
              </button>
            </div>
          </div>
        </div>
        
        {/* Primary Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
          <MetricCard
            title="Enterprise Zones"
            value={displayZones?.enterprise || 0}
            formatted={formatNumber(displayZones?.enterprise || 0)}
            threshold={config.thresholdZones}
            percentage={calculatePercentage(displayZones?.enterprise || 0, config.thresholdZones)}
            icon="zones"
            unit="zones"
            zoneBreakdown={displayMetrics?.previousMonthZoneBreakdown}
            primaryZones={config.primaryZones}
            secondaryZones={config.secondaryZones}
          />
          
          <MetricCard
            title="Data Transfer"
            value={usageViewMode === 'current' ? displayMetrics?.current.bytes || 0 : displayMetrics?.previous.bytes || 0}
            formatted={formatBandwidthTB(usageViewMode === 'current' ? displayMetrics?.current.bytes || 0 : displayMetrics?.previous.bytes || 0)}
            threshold={config.thresholdBandwidth}
            percentage={calculatePercentage(usageViewMode === 'current' ? displayMetrics?.current.bytes || 0 : displayMetrics?.previous.bytes || 0, config.thresholdBandwidth)}
            icon="bandwidth"
            unit="TB"
          />

          <MetricCard
            title="DNS Queries"
            value={usageViewMode === 'current' ? displayMetrics?.current.dnsQueries || 0 : displayMetrics?.previous.dnsQueries || 0}
            formatted={formatRequests(usageViewMode === 'current' ? displayMetrics?.current.dnsQueries || 0 : displayMetrics?.previous.dnsQueries || 0)}
            threshold={config.thresholdDnsQueries}
            percentage={calculatePercentage(usageViewMode === 'current' ? displayMetrics?.current.dnsQueries || 0 : displayMetrics?.previous.dnsQueries || 0, config.thresholdDnsQueries)}
            icon="dns"
            unit="M"
          />
        </div>

        {/* Request Breakdown */}
        <div className="bg-gray-50 border-l-4 border-l-blue-500 rounded-lg p-5">
          <h4 className="text-base font-semibold text-gray-900 mb-4">HTTP Requests Breakdown</h4>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <MetricCard
              title="Total Requests"
              value={usageViewMode === 'current' 
                ? displayMetrics?.current.totalRequests || 0 
                : displayMetrics?.previous.totalRequests || displayMetrics?.previous.requests || 0}
              formatted={formatRequests(usageViewMode === 'current' 
                ? displayMetrics?.current.totalRequests || 0 
                : displayMetrics?.previous.totalRequests || displayMetrics?.previous.requests || 0)}
              icon="requests"
              unit="M"
              compact
            />
            
            <MetricCard
              title="Blocked Requests"
              subtitle="(Security actions)"
              value={usageViewMode === 'current' 
                ? displayMetrics?.current.blockedRequests || 0 
                : displayMetrics?.previous.blockedRequests || 0}
              formatted={formatRequests(usageViewMode === 'current' 
                ? displayMetrics?.current.blockedRequests || 0 
                : displayMetrics?.previous.blockedRequests || 0)}
              icon="shield"
              unit="M"
              compact
            />
            
            <MetricCard
              title="Clean Traffic (Billable)"
              subtitle={usageViewMode === 'current' ? 'Used for threshold alerts' : 'Previous period'}
              value={usageViewMode === 'current' 
                ? displayMetrics?.current.cleanRequests || 0 
                : displayMetrics?.previous.cleanRequests || displayMetrics?.previous.requests || 0}
              formatted={formatRequests(usageViewMode === 'current' 
                ? displayMetrics?.current.cleanRequests || 0 
                : displayMetrics?.previous.cleanRequests || displayMetrics?.previous.requests || 0)}
              threshold={config.thresholdRequests}
              percentage={calculatePercentage(usageViewMode === 'current' 
                ? displayMetrics?.current.cleanRequests || 0 
                : displayMetrics?.previous.cleanRequests || displayMetrics?.previous.requests || 0, config.thresholdRequests)}
              icon="check"
              unit="M"
              compact
            />
          </div>
        </div>
      </div>

      {/* Breakdown by Zones Section */}
      {displayZones?.zones && displayZones.zones.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-lg">
          <div className="bg-gradient-to-r from-slate-700 to-slate-600 rounded-lg px-6 py-4 mb-6">
            <h3 className="text-2xl font-semibold text-white tracking-tight">Breakdown by Zones</h3>
            <p className="text-slate-200 text-sm mt-1">
              View detailed metrics for each enterprise zone
            </p>
            
            {/* Zone View Toggle */}
            <div className="mt-4 flex space-x-2">
              <button
                onClick={() => setZonesViewMode('current')}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  zonesViewMode === 'current'
                    ? 'bg-white text-slate-700'
                    : 'bg-slate-600 text-slate-200 hover:bg-slate-500'
                }`}
              >
                <Calendar className="w-4 h-4 inline mr-1" />
                <span>This Month</span>
              </button>
              <button
                onClick={() => setZonesViewMode('previous')}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  zonesViewMode === 'previous'
                    ? 'bg-white text-slate-700'
                    : 'bg-slate-600 text-slate-200 hover:bg-slate-500'
                }`}
              >
                <Calendar className="w-4 h-4 inline mr-1" />
                <span>Last Month</span>
              </button>
            </div>
          </div>
          
          {zonesViewMode === 'current' && (
            <div className="mt-4 mb-4 bg-blue-50 border-l-4 border-l-blue-500 rounded-lg p-4">
              <p className="text-sm text-gray-700">
                <span className="font-semibold">Note:</span> Primary/secondary classifications are based on previous month's usage (zones with ≥50GB are Primary).
              </p>
            </div>
          )}
          
          <ZonesList 
            zones={displayZones.zones} 
            zoneMetrics={zonesViewMode === 'current' 
              ? displayMetrics?.zoneBreakdown?.zones 
              : displayMetrics?.previousMonthZoneBreakdown?.zones
            }
            usePreviousClassification={zonesViewMode === 'current'}
            previousMonthMetrics={displayMetrics?.previousMonthZoneBreakdown?.zones}
          />
        </div>
      )}

      {/* Usage Charts */}
      {displayMetrics?.timeSeries && displayMetrics.timeSeries.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-lg">
          <div className="bg-gradient-to-r from-slate-700 to-slate-600 rounded-lg px-6 py-4 mb-6">
            <h3 className="text-2xl font-semibold text-white tracking-tight">Monthly Usage Trends</h3>
          </div>
          <p className="text-sm text-gray-600 mb-6">
            Historical monthly aggregated data for Enterprise zones only
          </p>
          
          <div className="space-y-6">
            <UsageChart
              data={displayMetrics.timeSeries}
              dataKey="requests"
              title="HTTP Requests by Month"
              color="#2563eb"
            />
            
            <UsageChart
              data={displayMetrics.timeSeries}
              dataKey="bytes"
              title="Data Transfer by Month"
              color="#2563eb"
              formatter={formatBytes}
            />

            <UsageChart
              data={displayMetrics.timeSeries}
              dataKey="dnsQueries"
              title="DNS Queries by Month"
              color="#2563eb"
            />
          </div>
        </div>
      )}
    </div>
  );
}

export default Dashboard;

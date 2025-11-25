import React, { useState, useEffect } from 'react';
import MetricCard from './MetricCard';
import UsageChart from './UsageChart';
import ZonesList from './ZonesList';
import { RefreshCw, Calendar, AlertCircle, Bell, BellOff, Filter } from 'lucide-react';
import { formatNumber, formatRequests, formatBandwidthTB, formatBytes } from '../utils/formatters';
import { SERVICE_CATEGORIES, SERVICE_METADATA } from '../constants/services';

function Dashboard({ config, zones, setZones, refreshTrigger }) {
  const [loading, setLoading] = useState(true);
  const [loadingPhase, setLoadingPhase] = useState(null); // null, 1, 2, 3, or 'cached'
  const [error, setError] = useState(null);
  const [metrics, setMetrics] = useState(null);
  const [alertsEnabled, setAlertsEnabled] = useState(config?.alertsEnabled || false);
  const [lastChecked, setLastChecked] = useState(null);
  const [usageViewMode, setUsageViewMode] = useState('current'); // 'current' or 'previous'
  const [zonesViewMode, setZonesViewMode] = useState('current'); // 'current' or 'previous'
  const [selectedAccount, setSelectedAccount] = useState('all'); // 'all' or specific accountId
  const [cacheAge, setCacheAge] = useState(null); // Cache age in seconds
  const [activeServiceTab, setActiveServiceTab] = useState(SERVICE_CATEGORIES.APPLICATION_SERVICES); // Active service tab
  const [zoneBreakdownSKU, setZoneBreakdownSKU] = useState('appServices'); // 'appServices' or 'botManagement'
  const [prewarming, setPrewarming] = useState(false); // Pre-warming cache state
  const [isInitialSetup, setIsInitialSetup] = useState(false); // Track first-time setup in progress

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

  // Handle refreshTrigger changes (from config save)
  useEffect(() => {
    if (refreshTrigger > 0) {
      // Trigger cache prewarm after config save
      prewarmCache();
    }
  }, [refreshTrigger]);

  const fetchData = async () => {
    // Support both old accountId and new accountIds format
    const accountIds = config?.accountIds || (config?.accountId ? [config.accountId] : []);
    
    // Don't fetch if config is missing or incomplete
    if (!config || accountIds.length === 0) {
      setError('Account IDs not configured. Please configure them in Settings.');
      setLoading(false);
      setLoadingPhase(null);
      return;
    }

    setLoading(true);
    setError(null);
    setCacheAge(null);
    setLoadingPhase(1);

    const startTime = Date.now();

    try {
      // Progressive Loading: Phase 1, 2, 3
      
      // Phase 1: Fast - Get zone count + check cache
      const phase1Response = await fetch('/api/metrics/progressive', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          phase: 1,
          accountIds: accountIds,
          accountId: accountIds[0], // Legacy fallback
        }),
      });

      if (!phase1Response.ok) {
        throw new Error('Failed to fetch Phase 1 data');
      }

      const phase1Data = await phase1Response.json();
      
      // Check if we got cached data (instant!)
      if (phase1Data.phase === 'cached') {
        setCacheAge(Math.floor(phase1Data.cacheAge / 1000)); // Convert to seconds
        setMetrics(phase1Data);
        setLoadingPhase('cached');
        
        // Use cached zones if available, otherwise fetch
        let zonesData;
        if (phase1Data.zones) {
          zonesData = phase1Data.zones;
          setZones(zonesData);
        } else {
          const zonesResponse = await fetch('/api/zones', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ accountIds, accountId: accountIds[0] }),
          });
          zonesData = await zonesResponse.json();
          setZones(zonesData);
        }
        setLastChecked(new Date());
        
        // Check thresholds if configured
        if (config.slackWebhook && alertsEnabled) {
          checkThresholds(phase1Data, zonesData);
        }
        
        setLoading(false);
        setLoadingPhase(null);
        return;
      }

      // Cache miss - continue with progressive loading
      
      // Update UI with Phase 1 data (zone count)
      setMetrics(phase1Data);
      setLoadingPhase(2);
      
      // Fetch zones in parallel with Phase 2
      const zonesPromise = fetch('/api/zones', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ accountIds, accountId: accountIds[0] }),
      });
      
      // Phase 2: Current month metrics + zone breakdown
      const phase2Response = await fetch('/api/metrics/progressive', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          phase: 2,
          accountIds: accountIds,
          accountId: accountIds[0],
        }),
      });

      if (!phase2Response.ok) {
        throw new Error('Failed to fetch Phase 2 data');
      }

      const phase2Data = await phase2Response.json();
      
      // Update UI with Phase 2 data
      setMetrics(phase2Data);
      setLoadingPhase(3);

      // Get zones data
      const zonesResponse = await zonesPromise;
      const zonesData = await zonesResponse.json();
      setZones(zonesData);
      
      // Phase 3: Historical data (time series)
      const phase3Response = await fetch('/api/metrics/progressive', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          phase: 3,
          accountIds: accountIds,
          accountId: accountIds[0],
        }),
      });

      if (!phase3Response.ok) {
        throw new Error('Failed to fetch Phase 3 data');
      }

      const phase3Data = await phase3Response.json();
      
      // Update UI with final complete data
      setMetrics(phase3Data);
      setLastChecked(new Date());

      // Check thresholds if configured
      if (config.slackWebhook && alertsEnabled) {
        checkThresholds(phase3Data, zonesData);
      }
    } catch (err) {
      console.error('Error fetching data:', err);
      setError(err.message || 'Failed to fetch data from Cloudflare API');
    } finally {
      setLoading(false);
      setLoadingPhase(null);
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
            requests: metricsData?.current?.requests || 0,
            bandwidth: metricsData?.current?.bytes || 0,
            botManagement: metricsData?.botManagement?.current?.likelyHuman || 0,
            apiShield: metricsData?.apiShield?.current?.requests || 0,
            pageShield: metricsData?.pageShield?.current?.requests || 0,
            advancedRateLimiting: metricsData?.advancedRateLimiting?.current?.requests || 0,
          },
          thresholds: {
            zones: config.thresholdZones,
            requests: config.thresholdRequests,
            bandwidth: config.thresholdBandwidth,
            botManagement: config?.applicationServices?.botManagement?.threshold || null,
            apiShield: config?.applicationServices?.apiShield?.threshold || null,
            pageShield: config?.applicationServices?.pageShield?.threshold || null,
            advancedRateLimiting: config?.applicationServices?.advancedRateLimiting?.threshold || null,
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

  const prewarmCache = async () => {
    setPrewarming(true);
    setLoading(true);
    setError(null);
    
    // For first-time setup: show progress phases during prewarm
    const isFirstTime = !metrics;
    if (isFirstTime) {
      setIsInitialSetup(true);  // Mark as initial setup
      setLoadingPhase(1);
      
      // Simulate phase progression during backend prewarm
      setTimeout(() => setLoadingPhase(2), 2000);  // Phase 2 after 2s
      setTimeout(() => setLoadingPhase(3), 8000);  // Phase 3 after 8s
    }
    
    try {
      const response = await fetch('/api/cache/prewarm', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      
      const result = await response.json();
      
      if (result.success) {
        // Refetch data to show updated metrics (including removed SKUs)
        await fetchData();
      } else {
        console.error(`❌ Refresh failed: ${result.error}`);
        alert(`❌ Refresh failed: ${result.error}`);
        setLoading(false);
        setLoadingPhase(null);
        setIsInitialSetup(false);
      }
    } catch (error) {
      console.error('Refresh error:', error);
      alert('❌ Failed to refresh data. Please try again.');
      setLoading(false);
      setLoadingPhase(null);
      setIsInitialSetup(false);
    } finally {
      setPrewarming(false);
      setIsInitialSetup(false);  // Always clear flag when done
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
          botManagement: metrics.botManagement ? {
            ...metrics.botManagement,
            threshold: config?.applicationServices?.botManagement?.threshold || metrics.botManagement.threshold,
          } : null,
          apiShield: metrics.apiShield ? {
            ...metrics.apiShield,
            threshold: config?.applicationServices?.apiShield?.threshold || metrics.apiShield.threshold,
          } : null,
          pageShield: metrics.pageShield ? {
            ...metrics.pageShield,
            threshold: config?.applicationServices?.pageShield?.threshold || metrics.pageShield.threshold,
          } : null,
          advancedRateLimiting: metrics.advancedRateLimiting ? {
            ...metrics.advancedRateLimiting,
            threshold: config?.applicationServices?.advancedRateLimiting?.threshold || metrics.advancedRateLimiting.threshold,
          } : null,
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
    
    // Filter Bot Management data for selected account
    let filteredBotManagement = null;
    if (metrics.botManagement && metrics.botManagement.enabled) {
      const accountBotData = metrics.botManagement.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      
      if (accountBotData) {
        filteredBotManagement = {
          enabled: true,
          threshold: config?.applicationServices?.botManagement?.threshold || metrics.botManagement.threshold,
          current: accountBotData.current,
          previous: accountBotData.previous,
          timeSeries: accountBotData.timeSeries,
        };
      }
      // If no data for this account, set to null (product not contracted)
    }
    
    // Filter API Shield data for selected account
    let filteredApiShield = null;
    if (metrics.apiShield && metrics.apiShield.enabled) {
      const accountApiShieldData = metrics.apiShield.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      
      if (accountApiShieldData) {
        filteredApiShield = {
          enabled: true,
          threshold: config?.applicationServices?.apiShield?.threshold || metrics.apiShield.threshold,
          current: accountApiShieldData.current,
          previous: accountApiShieldData.previous,
          timeSeries: accountApiShieldData.timeSeries,
        };
      }
      // If no data for this account, set to null (product not contracted)
    }
    
    // Filter Page Shield data for selected account
    let filteredPageShield = null;
    if (metrics.pageShield && metrics.pageShield.enabled) {
      const accountPageShieldData = metrics.pageShield.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      
      if (accountPageShieldData) {
        filteredPageShield = {
          enabled: true,
          threshold: config?.applicationServices?.pageShield?.threshold || metrics.pageShield.threshold,
          current: accountPageShieldData.current,
          previous: accountPageShieldData.previous,
          timeSeries: accountPageShieldData.timeSeries,
        };
      }
      // If no data for this account, set to null (product not contracted)
    }
    
    // Filter Advanced Rate Limiting data for selected account
    let filteredAdvancedRateLimiting = null;
    if (metrics.advancedRateLimiting && metrics.advancedRateLimiting.enabled) {
      const accountRateLimitingData = metrics.advancedRateLimiting.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      
      if (accountRateLimitingData) {
        filteredAdvancedRateLimiting = {
          enabled: true,
          threshold: config?.applicationServices?.advancedRateLimiting?.threshold || metrics.advancedRateLimiting.threshold,
          current: accountRateLimitingData.current,
          previous: accountRateLimitingData.previous,
          timeSeries: accountRateLimitingData.timeSeries,
        };
      }
      // If no data for this account, set to null (product not contracted)
    }
    
    return {
      metrics: {
        ...accountData,
        botManagement: filteredBotManagement,
        apiShield: filteredApiShield,
        pageShield: filteredPageShield,
        advancedRateLimiting: filteredAdvancedRateLimiting,
      },
      zones: accountZones ? { ...zones, zones: accountZones, enterprise: accountZones.length } : zones
    };
  };

  const filteredData = getFilteredData();
  const displayMetrics = filteredData.metrics;
  const displayZones = filteredData.zones;

  const calculatePercentage = (current, threshold) => {
    if (!threshold || threshold === 0) return 0;
    return (current / threshold) * 100;
  };

  // Show progress screen during initial setup OR when no metrics yet
  if (isInitialSetup || (loading && !metrics)) {
    // Show enhanced loading for initial setup
    const showProgress = (isInitialSetup || !cacheAge) && loadingPhase;
    
    return (
      <div className="flex items-center justify-center py-20">
        <div className="text-center max-w-md">
          <RefreshCw className="w-12 h-12 text-blue-500 animate-spin mx-auto mb-4" />
          
          {showProgress ? (
            <>
              <p className="text-gray-900 font-semibold text-lg mb-2">🚀 Setting up your dashboard...</p>
              <p className="text-gray-600 mb-4">Hold tight! We're fetching your account data from Cloudflare.</p>
              
              {/* Progress indicator */}
              <div className="bg-gray-100 rounded-lg p-4 text-left space-y-2">
                <div className="flex items-center space-x-3">
                  <div className={`w-2 h-2 rounded-full ${
                    loadingPhase >= 1 ? 'bg-green-500' : 'bg-gray-300'
                  }`} />
                  <span className="text-sm text-gray-700">Counting zones</span>
                </div>
                <div className="flex items-center space-x-3">
                  <div className={`w-2 h-2 rounded-full ${
                    loadingPhase >= 2 ? 'bg-green-500 animate-pulse' : 'bg-gray-300'
                  }`} />
                  <span className="text-sm text-gray-700">Fetching HTTP requests & data transfer</span>
                </div>
                <div className="flex items-center space-x-3">
                  <div className={`w-2 h-2 rounded-full ${
                    loadingPhase >= 3 ? 'bg-green-500 animate-pulse' : 'bg-gray-300'
                  }`} />
                  <span className="text-sm text-gray-700">Loading DNS queries & add-ons</span>
                </div>
              </div>
              
              <p className="text-xs text-gray-500 mt-4">This usually takes 20-30 seconds on first setup</p>
            </>
          ) : (
            <>
              <p className="text-gray-600 font-medium">Loading your usage data...</p>
              <p className="text-sm text-gray-500 mt-2">Fetching latest metrics from Cloudflare</p>
            </>
          )}
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
    <div className="space-y-6 relative">
      {/* Loading overlay for refresh/prewarm (NOT during initial setup) */}
      {loading && metrics && !isInitialSetup && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl shadow-2xl p-8 max-w-md">
            <RefreshCw className="w-12 h-12 text-blue-500 animate-spin mx-auto mb-4" />
            <p className="text-gray-900 font-semibold text-lg text-center mb-2">
              Refreshing your data...
            </p>
            <p className="text-gray-600 text-center text-sm">
              Fetching the latest metrics from Cloudflare
            </p>
          </div>
        </div>
      )}

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
          <div className="flex items-center gap-3 mt-1">
            <p className="text-sm text-gray-600">
              Monitor your Cloudflare Enterprise consumption
            </p>
            {cacheAge !== null && (
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                Data last refreshed: {
                  cacheAge < 60 
                    ? `${cacheAge}s ago` 
                    : cacheAge < 3600 
                      ? `${Math.floor(cacheAge / 60)}m ago`
                      : `${Math.floor(cacheAge / 3600)}h ago`
                }
              </span>
            )}
          </div>
        </div>
        <button
          onClick={prewarmCache}
          disabled={loading || prewarming}
          className="flex items-center space-x-2 px-5 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-sm font-medium"
          title="Fetch fresh data and cache for instant future loads"
        >
          <RefreshCw className={`w-4 h-4 ${prewarming ? 'animate-spin' : ''}`} />
          <span>{prewarming ? 'Refreshing...' : 'Refresh Data'}</span>
        </button>
      </div>

      {/* Service Tabs */}
      <div className="bg-white border border-gray-200 rounded-xl shadow-lg overflow-hidden">
        <div className="border-b border-gray-200 px-6 bg-gray-50">
          <nav className="-mb-px flex space-x-8">
            {Object.keys(SERVICE_METADATA).map(serviceKey => {
              const service = SERVICE_METADATA[serviceKey];
              const isActive = activeServiceTab === service.id;
              
              return (
                <button
                  key={service.id}
                  type="button"
                  onClick={() => setActiveServiceTab(service.id)}
                  className={`
                    py-4 px-1 border-b-2 font-medium text-sm transition-colors
                    ${isActive 
                      ? 'border-blue-500 text-blue-600' 
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                    }
                  `}
                >
                  <span className="mr-2">{service.icon}</span>
                  {service.name}
                </button>
              );
            })}
          </nav>
        </div>

        {/* Service Content */}
        <div className="p-6 bg-gray-50">
          {activeServiceTab === SERVICE_CATEGORIES.APPLICATION_SERVICES && renderApplicationServices()}
          {activeServiceTab === SERVICE_CATEGORIES.ZERO_TRUST && renderPlaceholderService('Zero Trust Services')}
          {activeServiceTab === SERVICE_CATEGORIES.NETWORK_SERVICES && renderPlaceholderService('Network Services')}
          {activeServiceTab === SERVICE_CATEGORIES.DEVELOPER_SERVICES && renderPlaceholderService('Developer Services')}
        </div>
      </div>
    </div>
  );

  // Render Application Services Tab Content
  function renderApplicationServices() {
    return (
      <>
      {/* Usage Metrics Section */}
      <div className="bg-white border border-gray-200 rounded-xl p-6 mb-10">
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
                <span>Current Month</span>
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
        
        {/* App Services Core Metrics - Only show if enabled */}
        {config?.applicationServices?.core?.enabled !== false && metrics?.current && (
          <>
            <h4 className="text-lg font-semibold text-gray-900 mb-4 mt-6">App Services Core</h4>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
              <MetricCard
                title="Enterprise Zones"
                value={displayZones?.enterprise || 0}
                formatted={formatNumber(displayZones?.enterprise || 0)}
                threshold={config?.applicationServices?.core?.thresholdZones || config.thresholdZones}
                percentage={calculatePercentage(displayZones?.enterprise || 0, config?.applicationServices?.core?.thresholdZones || config.thresholdZones)}
                icon="zones"
                unit="zones"
                zoneBreakdown={displayMetrics?.previousMonthZoneBreakdown}
                primaryZones={config?.applicationServices?.core?.primaryZones || config.primaryZones}
                secondaryZones={config?.applicationServices?.core?.secondaryZones || config.secondaryZones}
              />

              <MetricCard
                title="HTTP Requests"
                value={usageViewMode === 'current' 
                  ? displayMetrics?.current.requests || 0 
                  : displayMetrics?.previous.requests || 0}
                formatted={formatRequests(usageViewMode === 'current' 
                  ? displayMetrics?.current.requests || 0 
                  : displayMetrics?.previous.requests || 0)}
                threshold={config?.applicationServices?.core?.thresholdRequests || config.thresholdRequests}
                percentage={calculatePercentage(usageViewMode === 'current' 
                  ? displayMetrics?.current.requests || 0 
                  : displayMetrics?.previous.requests || 0, config?.applicationServices?.core?.thresholdRequests || config.thresholdRequests)}
                icon="requests"
                unit="M"
                confidence={usageViewMode === 'current' ? displayMetrics?.current?.confidence?.requests : null}
              />
              
              <MetricCard
                title="Data Transfer"
                value={usageViewMode === 'current' ? displayMetrics?.current.bytes || 0 : displayMetrics?.previous.bytes || 0}
                formatted={formatBandwidthTB(usageViewMode === 'current' ? displayMetrics?.current.bytes || 0 : displayMetrics?.previous.bytes || 0)}
                threshold={config?.applicationServices?.core?.thresholdBandwidth || config.thresholdBandwidth}
                percentage={calculatePercentage(usageViewMode === 'current' ? displayMetrics?.current.bytes || 0 : displayMetrics?.previous.bytes || 0, config?.applicationServices?.core?.thresholdBandwidth || config.thresholdBandwidth)}
                icon="bandwidth"
                unit="TB"
                confidence={usageViewMode === 'current' ? displayMetrics?.current?.confidence?.bytes : null}
                confidenceMetricType="HTTP Requests (measuring bytes)"
              />

              <MetricCard
                title="DNS Queries"
                value={usageViewMode === 'current' ? displayMetrics?.current.dnsQueries || 0 : displayMetrics?.previous.dnsQueries || 0}
                formatted={formatRequests(usageViewMode === 'current' ? displayMetrics?.current.dnsQueries || 0 : displayMetrics?.previous.dnsQueries || 0)}
                threshold={config?.applicationServices?.core?.thresholdDnsQueries || config.thresholdDnsQueries}
                percentage={calculatePercentage(usageViewMode === 'current' ? displayMetrics?.current.dnsQueries || 0 : displayMetrics?.previous.dnsQueries || 0, config?.applicationServices?.core?.thresholdDnsQueries || config.thresholdDnsQueries)}
                icon="dns"
                unit="M"
                confidence={usageViewMode === 'current' ? displayMetrics?.current?.confidence?.dnsQueries : null}
                confidenceMetricType="DNS Queries"
              />
            </div>
          </>
        )}

        {/* Add-ons Section - Integrated into Usage Metrics */}
        {(displayMetrics?.botManagement?.enabled || 
          displayMetrics?.apiShield?.enabled || 
          displayMetrics?.pageShield?.enabled || 
          displayMetrics?.advancedRateLimiting?.enabled) && (
          <>
            <h4 className="text-lg font-semibold text-gray-900 mb-4 mt-6">Add-ons</h4>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              {/* Bot Management */}
              {displayMetrics?.botManagement && displayMetrics.botManagement.enabled && (
                <MetricCard
                  title="Bot Management"
                  subtitle="Likely Human Requests"
                  value={usageViewMode === 'current' 
                    ? displayMetrics.botManagement.current?.likelyHuman || 0
                    : displayMetrics.botManagement.previous?.likelyHuman || 0}
                  formatted={formatRequests(usageViewMode === 'current' 
                    ? displayMetrics.botManagement.current?.likelyHuman || 0
                    : displayMetrics.botManagement.previous?.likelyHuman || 0)}
                  threshold={displayMetrics.botManagement.threshold}
                  percentage={calculatePercentage(
                    usageViewMode === 'current' 
                      ? displayMetrics.botManagement.current?.likelyHuman || 0
                      : displayMetrics.botManagement.previous?.likelyHuman || 0,
                    displayMetrics.botManagement.threshold
                  )}
                  icon="traffic"
                  unit="M"
                  confidence={usageViewMode === 'current' ? displayMetrics.botManagement.current?.confidence : null}
                  confidenceMetricType="Likely Human Requests"
                />
              )}

              {/* API Shield */}
              {displayMetrics?.apiShield && displayMetrics.apiShield.enabled && (
                <MetricCard
                  title="API Shield"
                  subtitle="HTTP Requests"
                  value={usageViewMode === 'current' 
                    ? displayMetrics.apiShield.current?.requests || 0
                    : displayMetrics.apiShield.previous?.requests || 0}
                  formatted={formatRequests(usageViewMode === 'current' 
                    ? displayMetrics.apiShield.current?.requests || 0
                    : displayMetrics.apiShield.previous?.requests || 0)}
                  threshold={displayMetrics.apiShield.threshold}
                  percentage={calculatePercentage(
                    usageViewMode === 'current' 
                      ? displayMetrics.apiShield.current?.requests || 0
                      : displayMetrics.apiShield.previous?.requests || 0,
                    displayMetrics.apiShield.threshold
                  )}
                  icon="requests"
                  unit="M"
                  confidence={usageViewMode === 'current' ? displayMetrics.apiShield.current?.confidence : null}
                  isZoneFiltered={true}
                />
              )}

              {/* Page Shield */}
              {displayMetrics?.pageShield && displayMetrics.pageShield.enabled && (
                <MetricCard
                  title="Page Shield"
                  subtitle="HTTP Requests"
                  value={usageViewMode === 'current' 
                    ? displayMetrics.pageShield.current?.requests || 0
                    : displayMetrics.pageShield.previous?.requests || 0}
                  formatted={formatRequests(usageViewMode === 'current' 
                    ? displayMetrics.pageShield.current?.requests || 0
                    : displayMetrics.pageShield.previous?.requests || 0)}
                  threshold={displayMetrics.pageShield.threshold}
                  percentage={calculatePercentage(
                    usageViewMode === 'current' 
                      ? displayMetrics.pageShield.current?.requests || 0
                      : displayMetrics.pageShield.previous?.requests || 0,
                    displayMetrics.pageShield.threshold
                  )}
                  icon="requests"
                  unit="M"
                  confidence={usageViewMode === 'current' ? displayMetrics.pageShield.current?.confidence : null}
                  isZoneFiltered={true}
                />
              )}

              {/* Advanced Rate Limiting */}
              {displayMetrics?.advancedRateLimiting && displayMetrics.advancedRateLimiting.enabled && (
                <MetricCard
                  title="Advanced Rate Limiting"
                  subtitle="HTTP Requests"
                  value={usageViewMode === 'current' 
                    ? displayMetrics.advancedRateLimiting.current?.requests || 0
                    : displayMetrics.advancedRateLimiting.previous?.requests || 0}
                  formatted={formatRequests(usageViewMode === 'current' 
                    ? displayMetrics.advancedRateLimiting.current?.requests || 0
                    : displayMetrics.advancedRateLimiting.previous?.requests || 0)}
                  threshold={displayMetrics.advancedRateLimiting.threshold}
                  percentage={calculatePercentage(
                    usageViewMode === 'current' 
                      ? displayMetrics.advancedRateLimiting.current?.requests || 0
                      : displayMetrics.advancedRateLimiting.previous?.requests || 0,
                    displayMetrics.advancedRateLimiting.threshold
                  )}
                  icon="requests"
                  unit="M"
                  confidence={usageViewMode === 'current' ? displayMetrics.advancedRateLimiting.current?.confidence : null}
                  isZoneFiltered={true}
                />
              )}
            </div>
          </>
        )}
      </div>

      {/* Breakdown by Zones Section - Show if any zone-level SKU is enabled */}
      {((config?.applicationServices?.core?.enabled !== false && displayZones?.zones && displayZones.zones.length > 0) || 
        (displayMetrics?.botManagement && displayMetrics.botManagement.enabled) ||
        (displayMetrics?.apiShield && displayMetrics.apiShield.enabled) ||
        (displayMetrics?.pageShield && displayMetrics.pageShield.enabled) ||
        (displayMetrics?.advancedRateLimiting && displayMetrics.advancedRateLimiting.enabled)) && (
        <div className="bg-white border border-gray-200 rounded-xl p-6 mb-10">
          <div className="bg-gradient-to-r from-slate-700 to-slate-600 rounded-lg px-6 py-4 mb-6">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-2xl font-semibold text-white tracking-tight">Breakdown by Zones</h3>
                <p className="text-slate-200 text-sm mt-1">
                  View detailed metrics for each enterprise zone
                </p>
              </div>
              
              {/* Zone View Toggle */}
              <div className="flex items-center bg-white rounded-lg p-1 shadow-sm">
                <button
                  onClick={() => setZonesViewMode('current')}
                  className={`flex items-center space-x-2 px-4 py-2 rounded-md transition-all font-medium text-sm ${
                    zonesViewMode === 'current'
                      ? 'bg-blue-600 text-white shadow-sm'
                      : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <Calendar className="w-4 h-4" />
                  <span>Current Month</span>
                </button>
                <button
                  onClick={() => setZonesViewMode('previous')}
                  className={`flex items-center space-x-2 px-4 py-2 rounded-md transition-all font-medium text-sm ${
                    zonesViewMode === 'previous'
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

          {/* SKU Selector Dropdown */}
          <div className="mb-6">
            <select
              value={zoneBreakdownSKU}
              onChange={(e) => setZoneBreakdownSKU(e.target.value)}
              className="w-full md:w-64 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-white"
            >
              {config?.applicationServices?.core?.enabled !== false && (
                <option value="appServices">App Services</option>
              )}
              {displayMetrics?.botManagement && displayMetrics.botManagement.enabled && (
                <option value="botManagement">Bot Management</option>
              )}
              {displayMetrics?.apiShield && displayMetrics.apiShield.enabled && (
                <option value="apiShield">API Shield</option>
              )}
              {displayMetrics?.pageShield && displayMetrics.pageShield.enabled && (
                <option value="pageShield">Page Shield</option>
              )}
              {displayMetrics?.advancedRateLimiting && displayMetrics.advancedRateLimiting.enabled && (
                <option value="advancedRateLimiting">Advanced Rate Limiting</option>
              )}
            </select>
          </div>
          
          {/* App Services Breakdown */}
          {zoneBreakdownSKU === 'appServices' && (
            <>
              {!displayZones?.zones || displayZones.zones.length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
                  <p className="text-sm text-yellow-800">
                    No App Services zone data available.
                  </p>
                </div>
              ) : (
                <>
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
                </>
              )}
            </>
          )}

          {/* Bot Management Breakdown */}
          {zoneBreakdownSKU === 'botManagement' && (
            <>
              {(() => {
                const botZones = zonesViewMode === 'current' 
                  ? displayMetrics?.botManagement?.current?.zones 
                  : displayMetrics?.botManagement?.previous?.zones;
                
                if (!botZones || botZones.length === 0) {
                  return (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
                      <p className="text-sm text-yellow-800">
                        No Bot Management zone data available for {zonesViewMode === 'current' ? 'current' : 'previous'} month.
                      </p>
                    </div>
                  );
                }
                
                // Deduplicate zones by zoneId
                const uniqueZones = botZones.reduce((acc, zone) => {
                  const id = zone.zoneId;
                  if (!acc[id]) {
                    acc[id] = zone;
                  }
                  return acc;
                }, {});
                const deduplicatedZones = Object.values(uniqueZones);
                
                return (
                  <div className="bg-gray-50 border border-gray-200 rounded-lg overflow-hidden">
                    <div className="max-h-96 overflow-y-auto">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-100 sticky top-0 z-10">
                          <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Zone
                            </th>
                            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Likely Human Requests
                            </th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {deduplicatedZones.map((zone, index) => (
                            <tr key={zone.zoneId || index} className="hover:bg-gray-50">
                              <td className="px-6 py-4 whitespace-nowrap">
                                <div className="text-sm font-medium text-gray-900">
                                  {zone.zoneName || zone.zoneId || 'Unknown Zone'}
                                </div>
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 font-medium">
                                {formatRequests(zone.likelyHuman || 0)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                );
              })()}
            </>
          )}

          {/* API Shield Breakdown */}
          {zoneBreakdownSKU === 'apiShield' && (
            <>
              {(() => {
                const addonZones = zonesViewMode === 'current' 
                  ? displayMetrics?.apiShield?.current?.zones 
                  : displayMetrics?.apiShield?.previous?.zones;
                
                if (!addonZones || addonZones.length === 0) {
                  return (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
                      <p className="text-sm text-yellow-800">
                        No API Shield zone data available for {zonesViewMode === 'current' ? 'current' : 'previous'} month.
                      </p>
                    </div>
                  );
                }
                
                return (
                  <div className="bg-gray-50 border border-gray-200 rounded-lg overflow-hidden">
                    <div className="max-h-96 overflow-y-auto">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-100 sticky top-0 z-10">
                          <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Zone
                            </th>
                            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                              HTTP Requests
                            </th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {addonZones.map((zone, index) => (
                            <tr key={zone.zoneId || index} className="hover:bg-gray-50">
                              <td className="px-6 py-4 whitespace-nowrap">
                                <div className="text-sm font-medium text-gray-900">
                                  {zone.zoneName || zone.zoneId || 'Unknown Zone'}
                                </div>
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 font-medium">
                                {formatRequests(zone.requests || 0)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                );
              })()}
            </>
          )}

          {/* Page Shield Breakdown */}
          {zoneBreakdownSKU === 'pageShield' && (
            <>
              {(() => {
                const addonZones = zonesViewMode === 'current' 
                  ? displayMetrics?.pageShield?.current?.zones 
                  : displayMetrics?.pageShield?.previous?.zones;
                
                if (!addonZones || addonZones.length === 0) {
                  return (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
                      <p className="text-sm text-yellow-800">
                        No Page Shield zone data available for {zonesViewMode === 'current' ? 'current' : 'previous'} month.
                      </p>
                    </div>
                  );
                }
                
                return (
                  <div className="bg-gray-50 border border-gray-200 rounded-lg overflow-hidden">
                    <div className="max-h-96 overflow-y-auto">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-100 sticky top-0 z-10">
                          <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Zone
                            </th>
                            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                              HTTP Requests
                            </th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {addonZones.map((zone, index) => (
                            <tr key={zone.zoneId || index} className="hover:bg-gray-50">
                              <td className="px-6 py-4 whitespace-nowrap">
                                <div className="text-sm font-medium text-gray-900">
                                  {zone.zoneName || zone.zoneId || 'Unknown Zone'}
                                </div>
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 font-medium">
                                {formatRequests(zone.requests || 0)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                );
              })()}
            </>
          )}

          {/* Advanced Rate Limiting Breakdown */}
          {zoneBreakdownSKU === 'advancedRateLimiting' && (
            <>
              {(() => {
                const addonZones = zonesViewMode === 'current' 
                  ? displayMetrics?.advancedRateLimiting?.current?.zones 
                  : displayMetrics?.advancedRateLimiting?.previous?.zones;
                
                if (!addonZones || addonZones.length === 0) {
                  return (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
                      <p className="text-sm text-yellow-800">
                        No Advanced Rate Limiting zone data available for {zonesViewMode === 'current' ? 'current' : 'previous'} month.
                      </p>
                    </div>
                  );
                }
                
                return (
                  <div className="bg-gray-50 border border-gray-200 rounded-lg overflow-hidden">
                    <div className="max-h-96 overflow-y-auto">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-100 sticky top-0 z-10">
                          <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Zone
                            </th>
                            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                              HTTP Requests
                            </th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {addonZones.map((zone, index) => (
                            <tr key={zone.zoneId || index} className="hover:bg-gray-50">
                              <td className="px-6 py-4 whitespace-nowrap">
                                <div className="text-sm font-medium text-gray-900">
                                  {zone.zoneName || zone.zoneId || 'Unknown Zone'}
                                </div>
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 font-medium">
                                {formatRequests(zone.requests || 0)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                );
              })()}
            </>
          )}
        </div>
      )}

      {/* Usage Charts - 2 Column Layout */}
      {displayMetrics?.timeSeries && displayMetrics.timeSeries.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-xl p-6 mb-10">
          <div className="bg-gradient-to-r from-slate-700 to-slate-600 rounded-lg px-6 py-4 mb-6">
            <h3 className="text-2xl font-semibold text-white tracking-tight">Monthly Usage Trends</h3>
            <p className="text-slate-200 text-sm mt-1">
              Historical monthly aggregated data for Enterprise zones only
            </p>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <UsageChart
              data={displayMetrics.timeSeries}
              dataKey="requests"
              title="HTTP Requests"
              color="#2563eb"
              threshold={config?.applicationServices?.core?.thresholdRequests || config?.thresholdRequests}
              yAxisLabel="HTTP Requests"
            />
            
            <UsageChart
              data={displayMetrics.timeSeries}
              dataKey="bytes"
              title="Data Transfer"
              color="#10b981"
              formatter={formatBytes}
              threshold={config?.applicationServices?.core?.thresholdBandwidth || config?.thresholdBandwidth}
              yAxisLabel="Data Transfer"
            />

            <UsageChart
              data={displayMetrics.timeSeries}
              dataKey="dnsQueries"
              title="DNS Queries"
              color="#f59e0b"
              threshold={config?.applicationServices?.core?.thresholdDnsQueries || config?.thresholdDnsQueries}
              yAxisLabel="DNS Queries"
            />

            {/* Bot Management Chart */}
            {displayMetrics?.botManagement?.enabled && 
             displayMetrics.botManagement.timeSeries && 
             displayMetrics.botManagement.timeSeries.length > 0 && (
              <UsageChart
                data={displayMetrics.botManagement.timeSeries}
                dataKey="likelyHuman"
                title="Bot Management: Likely Human Requests"
                color="#9333ea"
                threshold={displayMetrics.botManagement.threshold}
                yAxisLabel="Likely Human Requests"
              />
            )}

            {/* API Shield Chart */}
            {displayMetrics?.apiShield?.enabled && 
             displayMetrics.apiShield.timeSeries && 
             displayMetrics.apiShield.timeSeries.length > 0 && (
              <UsageChart
                data={displayMetrics.apiShield.timeSeries}
                dataKey="requests"
                title="API Shield: HTTP Requests"
                color="#06b6d4"
                threshold={displayMetrics.apiShield.threshold}
                yAxisLabel="HTTP Requests"
              />
            )}

            {/* Page Shield Chart */}
            {displayMetrics?.pageShield?.enabled && 
             displayMetrics.pageShield.timeSeries && 
             displayMetrics.pageShield.timeSeries.length > 0 && (
              <UsageChart
                data={displayMetrics.pageShield.timeSeries}
                dataKey="requests"
                title="Page Shield: HTTP Requests"
                color="#ec4899"
                threshold={displayMetrics.pageShield.threshold}
                yAxisLabel="HTTP Requests"
              />
            )}

            {/* Advanced Rate Limiting Chart */}
            {displayMetrics?.advancedRateLimiting?.enabled && 
             displayMetrics.advancedRateLimiting.timeSeries && 
             displayMetrics.advancedRateLimiting.timeSeries.length > 0 && (
              <UsageChart
                data={displayMetrics.advancedRateLimiting.timeSeries}
                dataKey="requests"
                title="Advanced Rate Limiting: HTTP Requests"
                color="#f97316"
                threshold={displayMetrics.advancedRateLimiting.threshold}
                yAxisLabel="HTTP Requests"
              />
            )}
          </div>
        </div>
      )}
      </>
    );
  }

  // Render Placeholder for Future Services
  function renderPlaceholderService(serviceName) {
    return (
      <div className="text-center py-20">
        <div className="text-gray-400 mb-4">
          <AlertCircle className="w-16 h-16 mx-auto" />
        </div>
        <h3 className="text-xl font-semibold text-gray-700 mb-2">
          {serviceName} Coming Soon
        </h3>
        <p className="text-sm text-gray-500 max-w-md mx-auto">
          SKUs and metrics for {serviceName} will be added here. This section will display usage data once configured.
        </p>
      </div>
    );
  }
}

export default Dashboard;

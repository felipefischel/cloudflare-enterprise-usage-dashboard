import React, { useState, useEffect } from 'react';
import ConsolidatedCard from './ConsolidatedCard';
import ZonesList from './ZonesList';
import { RefreshCw, AlertCircle, Bell, BellOff, Filter, ChevronRight, Info } from 'lucide-react';
import { formatNumber, formatRequests, formatBandwidthTB, formatBytes } from '../utils/formatters';
import { SERVICE_CATEGORIES, SERVICE_METADATA } from '../constants/services';

function Dashboard({ config, zones, setZones, refreshTrigger }) {
  const [loading, setLoading] = useState(true);
  const [loadingPhase, setLoadingPhase] = useState(null); // null, 1, 2, 3, or 'cached'
  const [error, setError] = useState(null);
  const [metrics, setMetrics] = useState(null);
  const [alertsEnabled, setAlertsEnabled] = useState(config?.alertsEnabled || false);
  const [lastChecked, setLastChecked] = useState(null);
  const [selectedAccount, setSelectedAccount] = useState('all');
  const [cacheAge, setCacheAge] = useState(null);
  const [activeServiceTab, setActiveServiceTab] = useState(SERVICE_CATEGORIES.APPLICATION_SERVICES);
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [zonesViewMode, setZonesViewMode] = useState('current');
  const [prewarming, setPrewarming] = useState(false);
  const [isInitialSetup, setIsInitialSetup] = useState(false);

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
    setError(null);
    
    // For first-time setup: show progress phases during prewarm
    const isFirstTime = !metrics;
    if (isFirstTime) {
      setLoading(true);
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
          zeroTrustSeats: metrics.zeroTrustSeats ? {
            ...metrics.zeroTrustSeats,
            threshold: config?.zeroTrust?.seats?.threshold || metrics.zeroTrustSeats.threshold,
          } : null,
          workersPages: metrics.workersPages ? {
            ...metrics.workersPages,
            requestsThreshold: config?.developerServices?.workersPages?.requestsThreshold || metrics.workersPages.requestsThreshold,
            cpuTimeThreshold: config?.developerServices?.workersPages?.cpuTimeThreshold || metrics.workersPages.cpuTimeThreshold,
          } : null,
          r2Storage: metrics.r2Storage ? {
            ...metrics.r2Storage,
            classAOpsThreshold: config?.developerServices?.r2Storage?.classAOpsThreshold || metrics.r2Storage.classAOpsThreshold,
            classBOpsThreshold: config?.developerServices?.r2Storage?.classBOpsThreshold || metrics.r2Storage.classBOpsThreshold,
            storageThreshold: config?.developerServices?.r2Storage?.storageThreshold || metrics.r2Storage.storageThreshold,
          } : null,
          magicTransit: metrics.magicTransit ? {
            ...metrics.magicTransit,
            egressEnabled: config?.networkServices?.magicTransit?.egressEnabled || false,
            threshold: config?.networkServices?.magicTransit?.threshold || metrics.magicTransit.threshold,
            egressThreshold: config?.networkServices?.magicTransit?.egressThreshold || null,
          } : null,
          magicWan: metrics.magicWan ? {
            ...metrics.magicWan,
            threshold: config?.networkServices?.magicWan?.threshold || metrics.magicWan.threshold,
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
    
    // Filter Zero Trust Seats data for selected account
    let filteredZeroTrustSeats = null;
    if (metrics.zeroTrustSeats && metrics.zeroTrustSeats.enabled) {
      const accountSeatsData = metrics.zeroTrustSeats.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      
      if (accountSeatsData) {
        filteredZeroTrustSeats = {
          enabled: true,
          threshold: config?.zeroTrust?.seats?.threshold || metrics.zeroTrustSeats.threshold,
          current: accountSeatsData.current,
          previous: accountSeatsData.previous,
          timeSeries: accountSeatsData.timeSeries,
        };
      }
    }
    
    // Filter Workers & Pages data for selected account
    let filteredWorkersPages = null;
    if (metrics.workersPages && metrics.workersPages.enabled) {
      const accountWpData = metrics.workersPages.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      
      if (accountWpData) {
        filteredWorkersPages = {
          enabled: true,
          requestsThreshold: config?.developerServices?.workersPages?.requestsThreshold || metrics.workersPages.requestsThreshold,
          cpuTimeThreshold: config?.developerServices?.workersPages?.cpuTimeThreshold || metrics.workersPages.cpuTimeThreshold,
          current: accountWpData.current,
          previous: accountWpData.previous,
          timeSeries: accountWpData.timeSeries,
        };
      }
    }
    
    // Filter R2 Storage data for selected account
    let filteredR2Storage = null;
    if (metrics.r2Storage && metrics.r2Storage.enabled) {
      const accountR2Data = metrics.r2Storage.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      
      if (accountR2Data) {
        filteredR2Storage = {
          enabled: true,
          classAOpsThreshold: config?.developerServices?.r2Storage?.classAOpsThreshold || metrics.r2Storage.classAOpsThreshold,
          classBOpsThreshold: config?.developerServices?.r2Storage?.classBOpsThreshold || metrics.r2Storage.classBOpsThreshold,
          storageThreshold: config?.developerServices?.r2Storage?.storageThreshold || metrics.r2Storage.storageThreshold,
          current: accountR2Data.current,
          previous: accountR2Data.previous,
          timeSeries: accountR2Data.timeSeries,
        };
      }
    }
    
    // Filter Magic Transit data for selected account
    let filteredMagicTransit = null;
    if (metrics.magicTransit && metrics.magicTransit.enabled) {
      const accountMtData = metrics.magicTransit.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      
      if (accountMtData) {
        filteredMagicTransit = {
          enabled: true,
          egressEnabled: config?.networkServices?.magicTransit?.egressEnabled || false,
          threshold: config?.networkServices?.magicTransit?.threshold || metrics.magicTransit.threshold,
          egressThreshold: config?.networkServices?.magicTransit?.egressThreshold || null,
          current: accountMtData.current,
          previous: accountMtData.previous,
          timeSeries: accountMtData.timeSeries,
        };
      }
    }
    
    // Filter Magic WAN data for selected account
    let filteredMagicWan = null;
    if (metrics.magicWan && metrics.magicWan.enabled) {
      const accountMwData = metrics.magicWan.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      
      if (accountMwData) {
        filteredMagicWan = {
          enabled: true,
          threshold: config?.networkServices?.magicWan?.threshold || metrics.magicWan.threshold,
          current: accountMwData.current,
          previous: accountMwData.previous,
          timeSeries: accountMwData.timeSeries,
        };
      }
    }
    
    return {
      metrics: {
        ...accountData,
        botManagement: filteredBotManagement,
        apiShield: filteredApiShield,
        pageShield: filteredPageShield,
        advancedRateLimiting: filteredAdvancedRateLimiting,
        zeroTrustSeats: filteredZeroTrustSeats,
        workersPages: filteredWorkersPages,
        r2Storage: filteredR2Storage,
        magicTransit: filteredMagicTransit,
        magicWan: filteredMagicWan,
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
                  onClick={() => { setActiveServiceTab(service.id); setSelectedProduct(null); }}
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
        <div className="bg-gray-50">
          {activeServiceTab === SERVICE_CATEGORIES.APPLICATION_SERVICES && renderApplicationServices()}
          {activeServiceTab === SERVICE_CATEGORIES.CLOUDFLARE_ONE && renderCloudflareOne()}
          {activeServiceTab === SERVICE_CATEGORIES.DEVELOPER_PLATFORM && renderDeveloperPlatform()}
        </div>
      </div>
    </div>
  );

  function renderSidebarLayout(sidebarItems, renderContent) {
    const activeProduct = selectedProduct || (sidebarItems.length > 0 ? sidebarItems[0].id : null);

    return (
      <div className="flex min-h-[500px]">
        <div className="w-64 border-r border-gray-200 bg-white flex-shrink-0 pt-6">
          <nav className="py-2">
            {sidebarItems.map((item) => {
              const isActive = activeProduct === item.id;
              return (
                <button
                  key={item.id}
                  onClick={() => setSelectedProduct(item.id)}
                  className={`w-full flex items-center justify-between px-4 py-3 text-sm transition-colors ${
                    isActive
                      ? 'bg-blue-50 text-blue-700 border-r-2 border-blue-600 font-medium'
                      : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'
                  }`}
                >
                  <span>{item.label}</span>
                  <ChevronRight className={`w-4 h-4 ${isActive ? 'text-blue-600' : 'text-gray-400'}`} />
                </button>
              );
            })}
          </nav>
        </div>
        <div className="flex-1 p-6 overflow-auto">
          {renderContent(activeProduct)}
        </div>
      </div>
    );
  }

  function renderApplicationServices() {
    const sidebarItems = [];

    if (config?.applicationServices?.core?.enabled !== false && metrics?.current) {
      sidebarItems.push({ id: 'enterpriseZones', label: 'Enterprise Zones' });
      sidebarItems.push({ id: 'appServices', label: 'Enterprise Core' });
      sidebarItems.push({ id: 'dns', label: 'DNS' });
    }
    if (displayMetrics?.magicTransit?.enabled) {
      sidebarItems.push({ id: 'magicTransit', label: 'Magic Transit' });
    }
    if (displayMetrics?.botManagement?.enabled) {
      sidebarItems.push({ id: 'botManagement', label: 'Bot Management' });
    }
    if (displayMetrics?.apiShield?.enabled) {
      sidebarItems.push({ id: 'apiShield', label: 'API Shield' });
    }
    if (displayMetrics?.pageShield?.enabled) {
      sidebarItems.push({ id: 'pageShield', label: 'Page Shield' });
    }
    if (displayMetrics?.advancedRateLimiting?.enabled) {
      sidebarItems.push({ id: 'advancedRateLimiting', label: 'Advanced Rate Limiting' });
    }

    if (sidebarItems.length === 0) {
      return (
        <div className="text-center py-20">
          <AlertCircle className="w-16 h-16 mx-auto text-gray-400 mb-4" />
          <h3 className="text-xl font-semibold text-gray-700 mb-2">Application Services</h3>
          <p className="text-sm text-gray-500">No Application Services configured. Go to Settings to enable them.</p>
        </div>
      );
    }

    return renderSidebarLayout(sidebarItems, (activeProduct) => {
      switch (activeProduct) {
        case 'enterpriseZones':
          return renderEnterpriseZones();
        case 'appServices':
          return renderAppServicesCore();
        case 'dns':
          return renderDNS();
        case 'magicTransit':
          return renderMagicTransit();
        case 'botManagement':
          return renderAddonProduct('botManagement', 'Bot Management', 'Likely Human Requests', 'likelyHuman', 'traffic', '#f59e0b');
        case 'apiShield':
          return renderAddonProduct('apiShield', 'API Shield', 'HTTP Requests', 'requests', 'requests', '#8b5cf6');
        case 'pageShield':
          return renderAddonProduct('pageShield', 'Page Shield', 'HTTP Requests', 'requests', 'requests', '#ec4899');
        case 'advancedRateLimiting':
          return renderAddonProduct('advancedRateLimiting', 'Advanced Rate Limiting', 'HTTP Requests', 'requests', 'requests', '#14b8a6');
        default:
          return null;
      }
    });
  }

  function renderEnterpriseZones() {
    const zonesThreshold = config?.applicationServices?.core?.thresholdZones || config.thresholdZones;
    const zonesCount = displayZones?.enterprise || 0;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Enterprise Zones"
          subtitle="Active enterprise zones across accounts"
          value={zonesCount}
          formatted={formatNumber(zonesCount)}
          threshold={zonesThreshold}
          percentage={calculatePercentage(zonesCount, zonesThreshold)}
          icon="zones"
          unit="zones"
          color="#3b82f6"
          timeSeries={displayZones?.zonesTimeSeries}
          dataKey="zones"
          chartFormatter={formatNumber}
          yAxisLabel="Zones"
          zoneBreakdown={displayMetrics?.previousMonthZoneBreakdown}
          primaryZones={config?.applicationServices?.core?.primaryZones || config.primaryZones}
          secondaryZones={config?.applicationServices?.core?.secondaryZones || config.secondaryZones}
        />
      </div>
    );
  }

  function renderAppServicesCore() {
    const reqThreshold = config?.applicationServices?.core?.thresholdRequests || config.thresholdRequests;
    const bwThreshold = config?.applicationServices?.core?.thresholdBandwidth || config.thresholdBandwidth;
    const currentRequests = displayMetrics?.current?.requests || 0;
    const currentBytes = displayMetrics?.current?.bytes || 0;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="HTTP Requests"
          subtitle="Billable HTTP requests (excluding blocked)"
          value={currentRequests}
          formatted={formatRequests(currentRequests)}
          threshold={reqThreshold}
          percentage={calculatePercentage(currentRequests, reqThreshold)}
          icon="requests"
          unit="M"
          color="#3b82f6"
          timeSeries={displayMetrics?.timeSeries}
          dataKey="requests"
          chartFormatter={formatRequests}
          yAxisLabel="Requests"
          confidence={displayMetrics?.current?.confidence?.requests}
          summaryBadge={displayMetrics?.current?.totalRequests != null ? (() => {
            const http = displayMetrics.current;
            return (
              <div className="bg-gray-50 border border-gray-200 rounded-lg px-3 py-2 text-xs">
                <div className="flex items-center justify-between space-x-2 mb-1">
                  <span className="text-gray-600">Total:</span>
                  <span className="font-semibold text-gray-900">{formatRequests(http.totalRequests)}</span>
                </div>
                <div className="flex items-center justify-between space-x-2">
                  <span className="text-gray-600">Blocked:</span>
                  <span className="font-semibold text-gray-900">{formatRequests(http.blockedRequests || 0)}</span>
                </div>
              </div>
            );
          })() : null}
        />
        <ConsolidatedCard
          title="Data Transfer"
          subtitle="Billable bandwidth served"
          value={currentBytes}
          formatted={formatBandwidthTB(currentBytes)}
          threshold={bwThreshold}
          percentage={calculatePercentage(currentBytes, bwThreshold)}
          icon="bandwidth"
          unit="TB"
          color="#6366f1"
          timeSeries={displayMetrics?.timeSeries}
          dataKey="bytes"
          chartFormatter={formatBandwidthTB}
          yAxisLabel="Bandwidth"
          confidence={displayMetrics?.current?.confidence?.bytes}
          confidenceMetricType="HTTP Requests (measuring bytes)"
          summaryBadge={displayMetrics?.current?.totalBytes != null ? (() => {
            const httpBytes = displayMetrics.current;
            return (
              <div className="bg-gray-50 border border-gray-200 rounded-lg px-3 py-2 text-xs">
                <div className="flex items-center justify-between space-x-2 mb-1">
                  <span className="text-gray-600">Total:</span>
                  <span className="font-semibold text-gray-900">{formatBandwidthTB(httpBytes.totalBytes)}</span>
                </div>
                <div className="flex items-center justify-between space-x-2">
                  <span className="text-gray-600">Blocked:</span>
                  <span className="font-semibold text-gray-900">{formatBandwidthTB(httpBytes.blockedBytes || 0)}</span>
                </div>
              </div>
            );
          })() : null}
        />
        {renderZoneBreakdown('appServices')}
      </div>
    );
  }

  function renderDNS() {
    const dnsThreshold = config?.applicationServices?.core?.thresholdDnsQueries || config.thresholdDnsQueries;
    const currentDns = displayMetrics?.current?.dnsQueries || 0;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="DNS Queries"
          subtitle="Authoritative DNS query volume"
          value={currentDns}
          formatted={formatRequests(currentDns)}
          threshold={dnsThreshold}
          percentage={calculatePercentage(currentDns, dnsThreshold)}
          icon="dns"
          unit="M"
          color="#0ea5e9"
          timeSeries={displayMetrics?.timeSeries}
          dataKey="dnsQueries"
          chartFormatter={formatRequests}
          yAxisLabel="Queries"
          confidence={displayMetrics?.current?.confidence?.dnsQueries}
          confidenceMetricType="DNS Queries"
        />
      </div>
    );
  }

  function renderMagicTransit() {
    const formatBandwidth = (mbps) => {
      if (mbps >= 1000) return `${(mbps / 1000).toFixed(2)} Gbps`;
      if (mbps >= 1) return `${mbps.toFixed(2)} Mbps`;
      if (mbps >= 0.001) return `${(mbps * 1000).toFixed(2)} Kbps`;
      if (mbps > 0) return `${(mbps * 1000000).toFixed(2)} bps`;
      return '0 Mbps';
    };

    const mt = displayMetrics?.magicTransit;
    if (!mt?.enabled) return null;

    const showEgress = mt.egressEnabled;

    return (
      <div className="space-y-6">
        {showEgress ? (
          <>
            <ConsolidatedCard
              title="Magic Transit (Ingress)"
              subtitle="P95th Bandwidth"
              value={mt.current?.ingressP95Mbps || 0}
              formatted={formatBandwidth(mt.current?.ingressP95Mbps || 0)}
              threshold={mt.threshold}
              percentage={calculatePercentage(mt.current?.ingressP95Mbps || 0, mt.threshold)}
              icon="bandwidth"
              unit="Mbps"
              color="#0ea5e9"
              timeSeries={mt.timeSeries}
              dataKey="ingressP95Mbps"
              yAxisLabel="Mbps"
            />
            <ConsolidatedCard
              title="Magic Transit (Egress)"
              subtitle="P95th Bandwidth"
              value={mt.current?.egressP95Mbps || 0}
              formatted={formatBandwidth(mt.current?.egressP95Mbps || 0)}
              threshold={mt.egressThreshold}
              percentage={calculatePercentage(mt.current?.egressP95Mbps || 0, mt.egressThreshold)}
              icon="bandwidth"
              unit="Mbps"
              color="#06b6d4"
              timeSeries={mt.timeSeries}
              dataKey="egressP95Mbps"
              yAxisLabel="Mbps"
            />
          </>
        ) : (
          <ConsolidatedCard
            title="Magic Transit"
            subtitle="P95th Bandwidth"
            value={mt.current?.p95Mbps || 0}
            formatted={formatBandwidth(mt.current?.p95Mbps || 0)}
            threshold={mt.threshold}
            percentage={calculatePercentage(mt.current?.p95Mbps || 0, mt.threshold)}
            icon="bandwidth"
            unit="Mbps"
            color="#0ea5e9"
            timeSeries={mt.timeSeries}
            dataKey="p95Mbps"
            yAxisLabel="Mbps"
          />
        )}
      </div>
    );
  }

  function renderAddonProduct(productKey, title, subtitle, dataField, iconType, color) {
    const product = displayMetrics?.[productKey];
    if (!product?.enabled) return null;

    const currentVal = product.current?.[dataField] || 0;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title={title}
          subtitle={subtitle}
          value={currentVal}
          formatted={formatRequests(currentVal)}
          threshold={product.threshold}
          percentage={calculatePercentage(currentVal, product.threshold)}
          icon={iconType}
          unit="M"
          color={color}
          timeSeries={product.timeSeries}
          dataKey={dataField}
          chartFormatter={formatRequests}
          yAxisLabel={subtitle}
          confidence={product.current?.confidence}
          confidenceMetricType={subtitle}
          isZoneFiltered={true}
        />
        {renderAddonZoneBreakdown(productKey, title, dataField)}
      </div>
    );
  }

  function renderZoneBreakdown(type) {
    if (!displayZones?.zones || displayZones.zones.length === 0) return null;

    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">Breakdown by Zone</h3>
            <p className="text-sm text-gray-500 mt-1">Usage per enterprise zone</p>
          </div>
          <div className="flex items-center bg-gray-100 rounded-lg p-1">
            <button
              onClick={() => setZonesViewMode('current')}
              className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                zonesViewMode === 'current'
                  ? 'bg-blue-600 text-white shadow-sm'
                  : 'text-gray-600 hover:bg-gray-200'
              }`}
            >
              Current Month
            </button>
            <button
              onClick={() => setZonesViewMode('previous')}
              className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                zonesViewMode === 'previous'
                  ? 'bg-blue-600 text-white shadow-sm'
                  : 'text-gray-600 hover:bg-gray-200'
              }`}
            >
              Previous Month
            </button>
          </div>
        </div>
        <div className="p-6">
          {zonesViewMode === 'current' && (
            <div className="mb-4 flex items-center space-x-1.5 text-xs text-gray-500">
              <div className="relative group">
                <Info className="w-4 h-4 text-gray-400 cursor-help" />
                <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50 pointer-events-none">
                  Primary/secondary classifications are based on previous month's usage (zones with ≥50GB are Primary).
                  <div className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-900"></div>
                </div>
              </div>
              <span>Classifications based on previous month</span>
            </div>
          )}
          <ZonesList
            zones={displayZones.zones}
            zoneMetrics={zonesViewMode === 'current'
              ? displayMetrics?.zoneBreakdown?.zones
              : displayMetrics?.previousMonthZoneBreakdown?.zones}
            usePreviousClassification={zonesViewMode === 'current'}
            previousMonthMetrics={displayMetrics?.previousMonthZoneBreakdown?.zones}
          />
        </div>
      </div>
    );
  }

  function renderAddonZoneBreakdown(productKey, title, dataField) {
    const product = displayMetrics?.[productKey];
    const zoneData = zonesViewMode === 'current' ? product?.current?.zones : product?.previous?.zones;
    if (!zoneData || zoneData.length === 0) return null;

    const uniqueZones = zoneData.reduce((acc, zone) => {
      const id = zone.zoneId;
      if (!acc[id]) acc[id] = zone;
      return acc;
    }, {});
    const deduplicatedZones = Object.values(uniqueZones);

    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">{title} - Breakdown by Zone</h3>
            <p className="text-sm text-gray-500 mt-1">Usage per zone</p>
          </div>
          <div className="flex items-center bg-gray-100 rounded-lg p-1">
            <button
              onClick={() => setZonesViewMode('current')}
              className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                zonesViewMode === 'current'
                  ? 'bg-blue-600 text-white shadow-sm'
                  : 'text-gray-600 hover:bg-gray-200'
              }`}
            >
              Current Month
            </button>
            <button
              onClick={() => setZonesViewMode('previous')}
              className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                zonesViewMode === 'previous'
                  ? 'bg-blue-600 text-white shadow-sm'
                  : 'text-gray-600 hover:bg-gray-200'
              }`}
            >
              Previous Month
            </button>
          </div>
        </div>
        <div className="p-6">
          <div className="bg-gray-50 border border-gray-200 rounded-lg overflow-hidden">
            <div className="max-h-96 overflow-y-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-100 sticky top-0 z-10">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Zone</th>
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">{title === 'Bot Management' ? 'Likely Human Requests' : 'Requests'}</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {deduplicatedZones.map((zone, index) => (
                    <tr key={zone.zoneId || index} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-sm font-medium text-gray-900">{zone.zoneName || zone.zoneId || 'Unknown Zone'}</div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <span className="text-sm font-semibold text-gray-900">
                          {formatRequests(zone[dataField] || zone.requests || zone.likelyHuman || 0)}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    );
  }

  function renderCloudflareOne() {
    const sidebarItems = [];

    if (displayMetrics?.zeroTrustSeats?.enabled) {
      sidebarItems.push({ id: 'zeroTrustSeats', label: 'Zero Trust Seats' });
    }
    if (displayMetrics?.magicWan?.enabled) {
      sidebarItems.push({ id: 'wan', label: 'WAN' });
    }

    if (sidebarItems.length === 0) {
      return (
        <div className="text-center py-20">
          <AlertCircle className="w-16 h-16 mx-auto text-gray-400 mb-4" />
          <h3 className="text-xl font-semibold text-gray-700 mb-2">Cloudflare One</h3>
          <p className="text-sm text-gray-500">No Cloudflare One services configured. Go to Settings to enable Zero Trust Seats or WAN.</p>
        </div>
      );
    }

    return renderSidebarLayout(sidebarItems, (activeProduct) => {
      switch (activeProduct) {
        case 'zeroTrustSeats':
          return renderZeroTrustSeats();
        case 'wan':
          return renderWAN();
        default:
          return null;
      }
    });
  }

  function renderZeroTrustSeats() {
    const zt = displayMetrics?.zeroTrustSeats;
    if (!zt?.enabled) return null;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Zero Trust Seats"
          subtitle="Active users consuming Access or Gateway seats"
          value={zt.current?.seats || 0}
          formatted={formatNumber(zt.current?.seats || 0)}
          threshold={zt.threshold}
          percentage={calculatePercentage(zt.current?.seats || 0, zt.threshold)}
          icon="users"
          unit=""
          color="#8b5cf6"
          timeSeries={zt.timeSeries}
          dataKey="seats"
          yAxisLabel="Seats"
        />
      </div>
    );
  }

  function renderWAN() {
    const formatBandwidth = (mbps) => {
      if (mbps >= 1000) return `${(mbps / 1000).toFixed(2)} Gbps`;
      if (mbps >= 1) return `${mbps.toFixed(2)} Mbps`;
      if (mbps >= 0.001) return `${(mbps * 1000).toFixed(2)} Kbps`;
      if (mbps > 0) return `${(mbps * 1000000).toFixed(2)} bps`;
      return '0 Mbps';
    };

    const wan = displayMetrics?.magicWan;
    if (!wan?.enabled) return null;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="WAN"
          subtitle="P95th Bandwidth"
          value={wan.current?.p95Mbps || 0}
          formatted={formatBandwidth(wan.current?.p95Mbps || 0)}
          threshold={wan.threshold}
          percentage={calculatePercentage(wan.current?.p95Mbps || 0, wan.threshold)}
          icon="bandwidth"
          unit="Mbps"
          color="#14b8a6"
          timeSeries={wan.timeSeries}
          dataKey="p95Mbps"
          yAxisLabel="Mbps"
        />
      </div>
    );
  }

  function renderDeveloperPlatform() {
    const sidebarItems = [];

    if (displayMetrics?.workersPages?.enabled) {
      sidebarItems.push({ id: 'workersPages', label: 'Workers & Pages' });
    }
    if (displayMetrics?.r2Storage?.enabled) {
      sidebarItems.push({ id: 'r2Storage', label: 'R2 Storage' });
    }

    if (sidebarItems.length === 0) {
      return (
        <div className="text-center py-20">
          <AlertCircle className="w-16 h-16 mx-auto text-gray-400 mb-4" />
          <h3 className="text-xl font-semibold text-gray-700 mb-2">Developer Platform</h3>
          <p className="text-sm text-gray-500">No Developer Platform services configured. Go to Settings to enable Workers & Pages or R2 Storage.</p>
        </div>
      );
    }

    return renderSidebarLayout(sidebarItems, (activeProduct) => {
      switch (activeProduct) {
        case 'workersPages':
          return renderWorkersPages();
        case 'r2Storage':
          return renderR2Storage();
        default:
          return null;
      }
    });
  }

  function renderWorkersPages() {
    const wp = displayMetrics?.workersPages;
    if (!wp?.enabled) return null;

    const formatReqs = (val) => {
      if (val >= 1000000000) return `${(val / 1000000000).toFixed(2)}B`;
      if (val >= 1000000) return `${(val / 1000000).toFixed(2)}M`;
      if (val >= 1000) return `${(val / 1000).toFixed(1)}K`;
      return val.toLocaleString();
    };

    const formatCpuTime = (ms) => {
      if (ms >= 1000000000) return `${(ms / 1000000000).toFixed(2)}B ms`;
      if (ms >= 1000000) return `${(ms / 1000000).toFixed(2)}M ms`;
      if (ms >= 1000) return `${(ms / 1000).toFixed(1)}K ms`;
      return `${ms.toLocaleString()} ms`;
    };

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Workers & Pages Requests"
          subtitle="Total invocations"
          value={wp.current?.requests || 0}
          formatted={formatReqs(wp.current?.requests || 0)}
          threshold={wp.requestsThreshold ? wp.requestsThreshold * 1000000 : null}
          percentage={calculatePercentage(wp.current?.requests || 0, wp.requestsThreshold ? wp.requestsThreshold * 1000000 : null)}
          icon="activity"
          unit=""
          color="#3b82f6"
          timeSeries={wp.timeSeries}
          dataKey="requests"
          chartFormatter={formatReqs}
          yAxisLabel="Requests"
        />
        <ConsolidatedCard
          title="CPU Time"
          subtitle="Total compute time"
          value={wp.current?.cpuTimeMs || 0}
          formatted={formatCpuTime(wp.current?.cpuTimeMs || 0)}
          threshold={wp.cpuTimeThreshold ? wp.cpuTimeThreshold * 1000000 : null}
          percentage={calculatePercentage(wp.current?.cpuTimeMs || 0, wp.cpuTimeThreshold ? wp.cpuTimeThreshold * 1000000 : null)}
          icon="cpu"
          unit=""
          color="#6366f1"
          timeSeries={wp.timeSeries}
          dataKey="cpuTimeMs"
          chartFormatter={formatCpuTime}
          yAxisLabel="CPU Time (ms)"
        />
      </div>
    );
  }

  function renderR2Storage() {
    const r2 = displayMetrics?.r2Storage;
    if (!r2?.enabled) return null;

    const formatReqs = (val) => {
      if (val >= 1000000000) return `${(val / 1000000000).toFixed(2)}B`;
      if (val >= 1000000) return `${(val / 1000000).toFixed(2)}M`;
      if (val >= 1000) return `${(val / 1000).toFixed(1)}K`;
      return val.toLocaleString();
    };

    const formatStorage = (gb) => {
      if (gb >= 1000) return `${(gb / 1000).toFixed(2)} TB`;
      if (gb >= 1) return `${gb.toFixed(2)} GB`;
      if (gb >= 0.001) return `${(gb * 1000).toFixed(2)} MB`;
      return `${(gb * 1000000).toFixed(2)} KB`;
    };

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Class A Operations"
          subtitle="Write/List/Delete"
          value={r2.current?.classAOps || 0}
          formatted={formatReqs(r2.current?.classAOps || 0)}
          threshold={r2.classAOpsThreshold ? r2.classAOpsThreshold * 1000000 : null}
          percentage={calculatePercentage(r2.current?.classAOps || 0, r2.classAOpsThreshold ? r2.classAOpsThreshold * 1000000 : null)}
          icon="upload"
          unit=""
          color="#3b82f6"
          timeSeries={r2.timeSeries}
          dataKey="classAOps"
          chartFormatter={formatReqs}
          yAxisLabel="Operations"
        />
        <ConsolidatedCard
          title="Class B Operations"
          subtitle="Read"
          value={r2.current?.classBOps || 0}
          formatted={formatReqs(r2.current?.classBOps || 0)}
          threshold={r2.classBOpsThreshold ? r2.classBOpsThreshold * 1000000 : null}
          percentage={calculatePercentage(r2.current?.classBOps || 0, r2.classBOpsThreshold ? r2.classBOpsThreshold * 1000000 : null)}
          icon="download"
          unit=""
          color="#6366f1"
          timeSeries={r2.timeSeries}
          dataKey="classBOps"
          chartFormatter={formatReqs}
          yAxisLabel="Operations"
        />
        <ConsolidatedCard
          title="Total Storage"
          subtitle="Capacity used"
          value={r2.current?.storageGB || 0}
          formatted={formatStorage(r2.current?.storageGB || 0)}
          threshold={r2.storageThreshold || null}
          percentage={calculatePercentage(r2.current?.storageGB || 0, r2.storageThreshold || null)}
          icon="database"
          unit=""
          color="#10b981"
          timeSeries={r2.timeSeries}
          dataKey="storageGB"
          chartFormatter={formatStorage}
          yAxisLabel="Storage (GB)"
        />
      </div>
    );
  }
}

export default Dashboard;


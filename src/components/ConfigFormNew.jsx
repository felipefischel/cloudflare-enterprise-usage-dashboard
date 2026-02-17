import React, { useState, useEffect } from 'react';
import { Save, X, TrendingUp, Key, AlertTriangle, Plus, Trash2, RefreshCw, CheckCircle, ChevronRight } from 'lucide-react';
import { MessageSquare } from 'lucide-react';
import { SERVICE_CATEGORIES, SERVICE_METADATA, APPLICATION_SERVICES_SKUS } from '../constants/services';

function ConfigFormNew({ onSave, initialConfig, onCancel, cachedZones }) {
  // Configuration step: 1 = Account IDs, 2 = Service Thresholds
  const [configStep, setConfigStep] = useState(initialConfig?.accountIds?.length > 0 ? 2 : 1);
  
  // Active service tab
  const [activeServiceTab, setActiveServiceTab] = useState(SERVICE_CATEGORIES.APPLICATION_SERVICES);
  const [selectedConfigProduct, setSelectedConfigProduct] = useState(null);
  
  // Loaded zones from accounts (use cached zones if available)
  const [availableZones, setAvailableZones] = useState(cachedZones?.zones || []);
  const [accountNamesMap, setAccountNamesMap] = useState(cachedZones?.accounts || {});
  const [loadingZones, setLoadingZones] = useState(false);
  const [zonesLoaded, setZonesLoaded] = useState(!!cachedZones?.zones);

  // Update availableZones when cachedZones changes (from Dashboard fetching)
  useEffect(() => {
    if (cachedZones?.zones && cachedZones.zones.length > 0) {
      console.log('🔄 Using cached zones from Dashboard:', cachedZones.zones.length);
      setAvailableZones(cachedZones.zones);
      setZonesLoaded(true);
    }
    if (cachedZones?.accounts) {
      setAccountNamesMap(cachedZones.accounts);
    }
  }, [cachedZones]);

  // Get account name - first from accounts map, then from zones, then fallback
  const getAccountName = (accountId) => {
    // First try the accounts map (works for accounts without zones)
    if (accountNamesMap[accountId]) {
      return accountNamesMap[accountId];
    }
    // Try to find account name from zones data
    const zoneWithAccount = availableZones.find(z => z.account?.id === accountId);
    if (zoneWithAccount?.account?.name) {
      return zoneWithAccount.account.name;
    }
    // Fallback to truncated ID
    return `Account ${accountId.substring(0, 8)}...`;
  };

  // Migrate old single accountId to accountIds array
  const getInitialAccountIds = () => {
    if (initialConfig?.accountIds && Array.isArray(initialConfig.accountIds)) {
      return initialConfig.accountIds;
    }
    if (initialConfig?.accountId) {
      return [initialConfig.accountId];
    }
    return [''];
  };

  const [formData, setFormData] = useState({
    accountIds: getInitialAccountIds(),
    
    // Application Services thresholds
    applicationServices: {
      // Core SKUs
      core: {
        enabled: initialConfig?.applicationServices?.core?.enabled !== undefined 
          ? initialConfig.applicationServices.core.enabled 
          : true, // Default enabled for backward compatibility
        thresholdZones: initialConfig?.thresholdZones || initialConfig?.applicationServices?.core?.thresholdZones || '',
        primaryZones: initialConfig?.primaryZones || initialConfig?.applicationServices?.core?.primaryZones || '',
        secondaryZones: initialConfig?.secondaryZones || initialConfig?.applicationServices?.core?.secondaryZones || '',
        thresholdRequests: initialConfig?.thresholdRequests 
          ? (initialConfig.thresholdRequests / 1e6) 
          : (initialConfig?.applicationServices?.core?.thresholdRequests ? (initialConfig.applicationServices.core.thresholdRequests / 1e6) : ''),
        thresholdBandwidth: initialConfig?.thresholdBandwidth 
          ? parseFloat((initialConfig.thresholdBandwidth / (1000 ** 4)).toFixed(6))
          : (initialConfig?.applicationServices?.core?.thresholdBandwidth ? parseFloat((initialConfig.applicationServices.core.thresholdBandwidth / (1000 ** 4)).toFixed(6)) : ''),
        thresholdDnsQueries: initialConfig?.thresholdDnsQueries 
          ? (initialConfig.thresholdDnsQueries / 1e6) 
          : (initialConfig?.applicationServices?.core?.thresholdDnsQueries ? (initialConfig.applicationServices.core.thresholdDnsQueries / 1e6) : ''),
      },
      
      // Add-on SKUs
      botManagement: {
        enabled: initialConfig?.applicationServices?.botManagement?.enabled || false,
        threshold: initialConfig?.applicationServices?.botManagement?.threshold ? (initialConfig.applicationServices.botManagement.threshold / 1e6) : '',
        zones: initialConfig?.applicationServices?.botManagement?.zones || [],
      },
      apiShield: {
        enabled: initialConfig?.applicationServices?.apiShield?.enabled || false,
        threshold: initialConfig?.applicationServices?.apiShield?.threshold ? (initialConfig.applicationServices.apiShield.threshold / 1e6) : '',
        zones: initialConfig?.applicationServices?.apiShield?.zones || [],
      },
      pageShield: {
        enabled: initialConfig?.applicationServices?.pageShield?.enabled || false,
        threshold: initialConfig?.applicationServices?.pageShield?.threshold ? (initialConfig.applicationServices.pageShield.threshold / 1e6) : '',
        zones: initialConfig?.applicationServices?.pageShield?.zones || [],
      },
      advancedRateLimiting: {
        enabled: initialConfig?.applicationServices?.advancedRateLimiting?.enabled || false,
        threshold: initialConfig?.applicationServices?.advancedRateLimiting?.threshold ? (initialConfig.applicationServices.advancedRateLimiting.threshold / 1e6) : '',
        zones: initialConfig?.applicationServices?.advancedRateLimiting?.zones || [],
      },
    },
    
    // Zero Trust thresholds
    zeroTrust: {
      seats: {
        enabled: initialConfig?.zeroTrust?.seats?.enabled || false,
        threshold: initialConfig?.zeroTrust?.seats?.threshold || '',
        accountIds: initialConfig?.zeroTrust?.seats?.accountIds || [],
      },
    },
    
    // Network Services thresholds
    networkServices: {
      magicTransit: {
        enabled: initialConfig?.networkServices?.magicTransit?.enabled || false,
        egressEnabled: initialConfig?.networkServices?.magicTransit?.egressEnabled || false,
        threshold: initialConfig?.networkServices?.magicTransit?.threshold || '',
        egressThreshold: initialConfig?.networkServices?.magicTransit?.egressThreshold || '',
        accountIds: initialConfig?.networkServices?.magicTransit?.accountIds || [],
      },
      magicWan: {
        enabled: initialConfig?.networkServices?.magicWan?.enabled || false,
        threshold: initialConfig?.networkServices?.magicWan?.threshold || '',
        accountIds: initialConfig?.networkServices?.magicWan?.accountIds || [],
      },
    },
    
    // Developer Services thresholds
    developerServices: {
      workersPages: {
        enabled: initialConfig?.developerServices?.workersPages?.enabled || false,
        requestsThreshold: initialConfig?.developerServices?.workersPages?.requestsThreshold || '',
        cpuTimeThreshold: initialConfig?.developerServices?.workersPages?.cpuTimeThreshold || '',
        accountIds: initialConfig?.developerServices?.workersPages?.accountIds || [],
      },
      r2Storage: {
        enabled: initialConfig?.developerServices?.r2Storage?.enabled || false,
        classAOpsThreshold: initialConfig?.developerServices?.r2Storage?.classAOpsThreshold || '',
        classBOpsThreshold: initialConfig?.developerServices?.r2Storage?.classBOpsThreshold || '',
        storageThreshold: initialConfig?.developerServices?.r2Storage?.storageThreshold || '',
        accountIds: initialConfig?.developerServices?.r2Storage?.accountIds || [],
      },
    },
    
    slackWebhook: initialConfig?.slackWebhook || '',
  });

  const [errors, setErrors] = useState({});

  // Load zones on mount if account IDs are already configured (only if not cached)
  useEffect(() => {
    const loadZonesOnMount = async () => {
      const validAccountIds = formData.accountIds.filter(id => id.trim());
      
      // If we have account IDs and haven't loaded zones yet (and no cached zones), load them
      if (validAccountIds.length > 0 && availableZones.length === 0 && !loadingZones && !cachedZones) {
        console.log('Auto-loading zones for configured accounts (no cache)...');
        setLoadingZones(true);
        
        try {
          const response = await fetch('/api/zones', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              accountIds: validAccountIds,
              accountId: validAccountIds[0],
            }),
          });

          if (response.ok) {
            const zonesData = await response.json();
            setAvailableZones(zonesData.zones || []);
            setAccountNamesMap(zonesData.accounts || {});
            setZonesLoaded(true);
            console.log(`Loaded ${zonesData.zones?.length || 0} zones, ${Object.keys(zonesData.accounts || {}).length} accounts`);
          }
        } catch (error) {
          console.error('Failed to auto-load zones:', error);
        } finally {
          setLoadingZones(false);
        }
      }
    };

    loadZonesOnMount();
  }, []); // Run once on mount

  const handleChange = (service, field, value) => {
    setFormData(prev => ({
      ...prev,
      [service]: {
        ...prev[service],
        [field]: value
      }
    }));
    
    // Clear errors
    if (errors[`${service}.${field}`]) {
      setErrors(prev => ({ ...prev, [`${service}.${field}`]: null }));
    }
  };

  const addAccountId = () => {
    setFormData(prev => ({
      ...prev,
      accountIds: [...prev.accountIds, '']
    }));
  };

  const removeAccountId = (index) => {
    setFormData(prev => ({
      ...prev,
      accountIds: prev.accountIds.filter((_, i) => i !== index)
    }));
  };

  const updateAccountId = (index, value) => {
    setFormData(prev => ({
      ...prev,
      accountIds: prev.accountIds.map((id, i) => i === index ? value : id)
    }));
  };

  // Handle Bot Management zone selection
  const toggleBotManagementZone = (zoneId) => {
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        botManagement: {
          ...prev.applicationServices.botManagement,
          zones: prev.applicationServices.botManagement.zones.includes(zoneId)
            ? prev.applicationServices.botManagement.zones.filter(id => id !== zoneId)
            : [...prev.applicationServices.botManagement.zones, zoneId]
        }
      }
    }));
  };

  const toggleAllBotManagementZones = () => {
    const currentZones = formData.applicationServices.botManagement.zones;
    const allZoneIds = availableZones.map(z => z.id);
    
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        botManagement: {
          ...prev.applicationServices.botManagement,
          zones: currentZones.length === allZoneIds.length ? [] : allZoneIds
        }
      }
    }));
  };

  // Handle API Shield zone selection
  const toggleApiShieldZone = (zoneId) => {
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        apiShield: {
          ...prev.applicationServices.apiShield,
          zones: prev.applicationServices.apiShield.zones.includes(zoneId)
            ? prev.applicationServices.apiShield.zones.filter(id => id !== zoneId)
            : [...prev.applicationServices.apiShield.zones, zoneId]
        }
      }
    }));
  };

  const toggleAllApiShieldZones = () => {
    const currentZones = formData.applicationServices.apiShield.zones;
    const allZoneIds = availableZones.map(z => z.id);
    
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        apiShield: {
          ...prev.applicationServices.apiShield,
          zones: currentZones.length === allZoneIds.length ? [] : allZoneIds
        }
      }
    }));
  };

  // Handle Page Shield zone selection
  const togglePageShieldZone = (zoneId) => {
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        pageShield: {
          ...prev.applicationServices.pageShield,
          zones: prev.applicationServices.pageShield.zones.includes(zoneId)
            ? prev.applicationServices.pageShield.zones.filter(id => id !== zoneId)
            : [...prev.applicationServices.pageShield.zones, zoneId]
        }
      }
    }));
  };

  const toggleAllPageShieldZones = () => {
    const currentZones = formData.applicationServices.pageShield.zones;
    const allZoneIds = availableZones.map(z => z.id);
    
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        pageShield: {
          ...prev.applicationServices.pageShield,
          zones: currentZones.length === allZoneIds.length ? [] : allZoneIds
        }
      }
    }));
  };

  // Handle Advanced Rate Limiting zone selection
  const toggleAdvancedRateLimitingZone = (zoneId) => {
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        advancedRateLimiting: {
          ...prev.applicationServices.advancedRateLimiting,
          zones: prev.applicationServices.advancedRateLimiting.zones.includes(zoneId)
            ? prev.applicationServices.advancedRateLimiting.zones.filter(id => id !== zoneId)
            : [...prev.applicationServices.advancedRateLimiting.zones, zoneId]
        }
      }
    }));
  };

  const toggleAllAdvancedRateLimitingZones = () => {
    const currentZones = formData.applicationServices.advancedRateLimiting.zones;
    const allZoneIds = availableZones.map(z => z.id);
    
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        advancedRateLimiting: {
          ...prev.applicationServices.advancedRateLimiting,
          zones: currentZones.length === allZoneIds.length ? [] : allZoneIds
        }
      }
    }));
  };

  // Load zones from accounts
  const handleLoadZones = async () => {
    const validAccountIds = formData.accountIds.filter(id => id.trim());
    
    if (validAccountIds.length === 0) {
      setErrors({ accountIds: 'At least one Account ID is required' });
      return;
    }

    setLoadingZones(true);
    setErrors({});

    try {
      const response = await fetch('/api/zones', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          accountIds: validAccountIds,
          accountId: validAccountIds[0], // Legacy fallback
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to load zones');
      }

      const zonesData = await response.json();
      setAvailableZones(zonesData.zones || []);
      setAccountNamesMap(zonesData.accounts || {});
      setZonesLoaded(true);
      setConfigStep(2); // Move to thresholds step
    } catch (error) {
      console.error('Error loading zones:', error);
      setErrors({ accountIds: error.message || 'Failed to load zones. Check your Account IDs and API token.' });
    } finally {
      setLoadingZones(false);
    }
  };

  const validate = () => {
    const newErrors = {};

    // Validate account IDs
    const validAccountIds = formData.accountIds.filter(id => id.trim());
    if (validAccountIds.length === 0) {
      newErrors.accountIds = 'At least one Account ID is required';
    }

    // Validate Application Services zone breakdown
    const appServices = formData.applicationServices;
    const core = appServices.core;
    const totalZones = core.thresholdZones ? parseInt(core.thresholdZones, 10) : 0;
    const primaryZones = core.primaryZones ? parseInt(core.primaryZones, 10) : 0;
    const secondaryZones = core.secondaryZones ? parseInt(core.secondaryZones, 10) : 0;
    
    if (totalZones > 0 && (primaryZones > 0 || secondaryZones > 0)) {
      const sum = primaryZones + secondaryZones;
      if (sum !== totalZones) {
        newErrors.zoneBreakdown = `Primary zones (${primaryZones}) + Secondary zones (${secondaryZones}) = ${sum}, but Total zones is ${totalZones}. They must be equal.`;
      }
    }

    // Validate Slack webhook
    if (formData.slackWebhook && !formData.slackWebhook.startsWith('https://hooks.slack.com/')) {
      newErrors.slackWebhook = 'Invalid Slack webhook URL';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    
    if (validate()) {
      const validAccountIds = formData.accountIds.filter(id => id.trim());
      
      const config = {
        accountIds: validAccountIds,
        
        // Application Services (new structured format)
        applicationServices: {
          core: {
            enabled: formData.applicationServices.core.enabled,
            thresholdZones: formData.applicationServices.core.thresholdZones ? parseInt(formData.applicationServices.core.thresholdZones, 10) : null,
            primaryZones: formData.applicationServices.core.primaryZones ? parseInt(formData.applicationServices.core.primaryZones, 10) : null,
            secondaryZones: formData.applicationServices.core.secondaryZones ? parseInt(formData.applicationServices.core.secondaryZones, 10) : null,
            thresholdRequests: formData.applicationServices.core.thresholdRequests ? Math.round(parseFloat(formData.applicationServices.core.thresholdRequests) * 1e6) : null,
            thresholdBandwidth: formData.applicationServices.core.thresholdBandwidth ? Math.round(parseFloat(formData.applicationServices.core.thresholdBandwidth) * (1000 ** 4)) : null,
            thresholdDnsQueries: formData.applicationServices.core.thresholdDnsQueries ? Math.round(parseFloat(formData.applicationServices.core.thresholdDnsQueries) * 1e6) : null,
          },
          botManagement: {
            enabled: formData.applicationServices.botManagement.enabled,
            threshold: formData.applicationServices.botManagement.threshold ? Math.round(parseFloat(formData.applicationServices.botManagement.threshold) * 1e6) : null,
            zones: formData.applicationServices.botManagement.zones,
          },
          apiShield: {
            enabled: formData.applicationServices.apiShield.enabled,
            threshold: formData.applicationServices.apiShield.threshold ? Math.round(parseFloat(formData.applicationServices.apiShield.threshold) * 1e6) : null,
            zones: formData.applicationServices.apiShield.zones,
          },
          pageShield: {
            enabled: formData.applicationServices.pageShield.enabled,
            threshold: formData.applicationServices.pageShield.threshold ? Math.round(parseFloat(formData.applicationServices.pageShield.threshold) * 1e6) : null,
            zones: formData.applicationServices.pageShield.zones,
          },
          advancedRateLimiting: {
            enabled: formData.applicationServices.advancedRateLimiting.enabled,
            threshold: formData.applicationServices.advancedRateLimiting.threshold ? Math.round(parseFloat(formData.applicationServices.advancedRateLimiting.threshold) * 1e6) : null,
            zones: formData.applicationServices.advancedRateLimiting.zones,
          },
        },
        
        // Zero Trust
        zeroTrust: {
          seats: {
            enabled: formData.zeroTrust.seats.enabled,
            threshold: formData.zeroTrust.seats.threshold ? parseInt(formData.zeroTrust.seats.threshold, 10) : null,
            accountIds: formData.zeroTrust.seats.accountIds || [],
          },
        },
        // Network Services
        networkServices: {
          magicTransit: {
            enabled: formData.networkServices.magicTransit.enabled,
            egressEnabled: formData.networkServices.magicTransit.egressEnabled || false,
            threshold: formData.networkServices.magicTransit.threshold ? parseInt(formData.networkServices.magicTransit.threshold, 10) : null,
            egressThreshold: formData.networkServices.magicTransit.egressThreshold ? parseInt(formData.networkServices.magicTransit.egressThreshold, 10) : null,
            accountIds: formData.networkServices.magicTransit.accountIds || [],
          },
          magicWan: {
            enabled: formData.networkServices.magicWan.enabled,
            threshold: formData.networkServices.magicWan.threshold ? parseInt(formData.networkServices.magicWan.threshold, 10) : null,
            accountIds: formData.networkServices.magicWan.accountIds || [],
          },
        },
        developerServices: {
          workersPages: {
            enabled: formData.developerServices.workersPages.enabled,
            requestsThreshold: formData.developerServices.workersPages.requestsThreshold ? Number(formData.developerServices.workersPages.requestsThreshold) : null,
            cpuTimeThreshold: formData.developerServices.workersPages.cpuTimeThreshold ? Number(formData.developerServices.workersPages.cpuTimeThreshold) : null,
            accountIds: formData.developerServices.workersPages.accountIds || [],
          },
          r2Storage: {
            enabled: formData.developerServices.r2Storage.enabled,
            classAOpsThreshold: formData.developerServices.r2Storage.classAOpsThreshold ? Number(formData.developerServices.r2Storage.classAOpsThreshold) : null,
            classBOpsThreshold: formData.developerServices.r2Storage.classBOpsThreshold ? Number(formData.developerServices.r2Storage.classBOpsThreshold) : null,
            storageThreshold: formData.developerServices.r2Storage.storageThreshold ? Number(formData.developerServices.r2Storage.storageThreshold) : null,
            accountIds: formData.developerServices.r2Storage.accountIds || [],
          },
        },
        
        slackWebhook: formData.slackWebhook || '',
        alertsEnabled: initialConfig?.alertsEnabled !== undefined ? initialConfig.alertsEnabled : false,
      };
      
      onSave(config);
    }
  };

  // Render Account IDs Step
  const renderAccountIdsStep = () => (
    <div className="p-6 space-y-6">
      {/* API Token Notice */}
      <div className="bg-blue-50 border-l-4 border-blue-400 p-4">
        <div className="flex items-start">
          <Key className="w-5 h-5 text-blue-400 mt-0.5 mr-3 flex-shrink-0" />
          <div>
            <h3 className="text-sm font-semibold text-blue-800">Cloudflare API Token Required</h3>
            <p className="text-xs text-blue-700 mt-1">
              If you haven't already created an API token as part of the configuration, you can create one at{' '}
              <a 
                href="https://dash.cloudflare.com/profile/api-tokens" 
                target="_blank" 
                rel="noopener noreferrer"
                className="underline font-medium hover:text-blue-900"
              >
                Cloudflare Dashboard
              </a>
              {' '}(use the 'Read all resources' template).
            </p>
            <p className="text-xs text-blue-700 mt-2">
              Then add it as a secret by going to: <strong>Workers and Pages</strong> → <strong>enterprise-usage-dashboard</strong> → <strong>Settings</strong> → <strong>Variables and Secrets</strong> → <strong>Add Secret</strong>
            </p>
            <p className="text-xs text-blue-700 mt-2">
              Secret name: <code className="bg-blue-100 text-blue-900 px-1.5 py-0.5 rounded font-mono">CLOUDFLARE_API_TOKEN</code>
            </p>
          </div>
        </div>
      </div>

      {/* Account IDs */}
      <div className="space-y-4">
        <h3 className="text-lg font-semibold text-gray-900 flex items-center space-x-2">
          <Key className="w-5 h-5" />
          <span>Cloudflare Accounts</span>
        </h3>
        <p className="text-sm text-gray-600">
          Configure which Cloudflare accounts to monitor. We'll load your Enterprise zones after saving.
        </p>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Account IDs *
          </label>
          
          <div className="space-y-2">
            {formData.accountIds.map((accountId, index) => (
              <div key={index} className="flex items-center space-x-2">
                <input
                  type="text"
                  value={accountId}
                  onChange={(e) => updateAccountId(index, e.target.value)}
                  className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="Account ID"
                />
                {formData.accountIds.length > 1 && (
                  <button
                    type="button"
                    onClick={() => removeAccountId(index)}
                    className="p-2 text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                    title="Remove account"
                  >
                    <Trash2 className="w-5 h-5" />
                  </button>
                )}
              </div>
            ))}
          </div>

          <button
            type="button"
            onClick={addAccountId}
            className="mt-3 flex items-center space-x-2 px-4 py-2 text-blue-600 border border-blue-600 rounded-lg hover:bg-blue-50 transition-colors"
          >
            <Plus className="w-4 h-4" />
            <span>Add Another Account</span>
          </button>

          {errors.accountIds && (
            <p className="text-red-600 text-sm mt-2">{errors.accountIds}</p>
          )}
          
          <div className="mt-2 space-y-1">
            <p className="text-gray-500 text-xs">
              Find in your Cloudflare dashboard URL or account settings
            </p>
            <p className="text-blue-600 text-xs font-medium">
              💡 You can add multiple accounts. Your API token must have access to all accounts.
            </p>
          </div>
        </div>
      </div>

      {/* Slack Notifications */}
      <div className="border-t border-gray-200 pt-6 space-y-4">
        <h3 className="text-lg font-semibold text-gray-900 flex items-center space-x-2">
          <MessageSquare className="w-5 h-5" />
          <span>Slack Notifications</span>
        </h3>
        <p className="text-sm text-gray-600">
          Add a Slack webhook URL to receive alerts when usage reaches 90% of thresholds
        </p>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            <MessageSquare className="inline w-4 h-4 mr-1" />
            Slack Webhook URL (optional)
          </label>
          <input
            type="text"
            value={formData.slackWebhook}
            onChange={(e) => setFormData(prev => ({ ...prev, slackWebhook: e.target.value }))}
            className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
              errors.slackWebhook ? 'border-red-500' : 'border-gray-300'
            }`}
            placeholder="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
          />
          {errors.slackWebhook && (
            <p className="text-red-600 text-sm mt-1">{errors.slackWebhook}</p>
          )}
          <p className="text-xs text-gray-500 mt-1">
            Get your webhook URL from Slack: Workspace Settings → Apps → Incoming Webhooks
          </p>
        </div>
      </div>

      {/* Action Buttons */}
      <div className="flex items-center justify-between pt-6 pb-4 border-t border-gray-200">
        <div>
          {zonesLoaded && (
            <div className="flex items-center space-x-2 text-green-600 text-sm">
              <CheckCircle className="w-4 h-4" />
              <span>{availableZones.length} zones loaded</span>
            </div>
          )}
        </div>
        <div className="flex items-center space-x-3">
          {onCancel && (
            <button
              type="button"
              onClick={onCancel}
              className="px-6 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors flex items-center space-x-2"
            >
              <X className="w-4 h-4" />
              <span>Cancel</span>
            </button>
          )}
          <button
            type="button"
            onClick={handleLoadZones}
            disabled={loadingZones}
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center space-x-2 shadow-sm disabled:opacity-50"
          >
            <RefreshCw className={`w-4 h-4 ${loadingZones ? 'animate-spin' : ''}`} />
            <span>{loadingZones ? 'Loading Zones...' : 'Save & Load Zones'}</span>
          </button>
        </div>
      </div>
    </div>
  );

  // Render Service Thresholds Step
  const renderServiceThresholdsStep = () => {
    const serviceKeys = Object.keys(SERVICE_METADATA);
    
    return (
      <div className="space-y-6">
        {/* Service Tabs */}
        <div className="border-b border-gray-200 px-6">
          <nav className="-mb-px flex space-x-8">
            {serviceKeys.map(serviceKey => {
              const service = SERVICE_METADATA[serviceKey];
              const isActive = activeServiceTab === service.id;
              
              return (
                <button
                  key={service.id}
                  type="button"
                  onClick={() => { setActiveServiceTab(service.id); setSelectedConfigProduct(null); }}
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

        {/* Service Content with Sidebar */}
        <div className="pb-6">
          {activeServiceTab === SERVICE_CATEGORIES.APPLICATION_SERVICES && renderAppServicesWithSidebar()}
          {activeServiceTab === SERVICE_CATEGORIES.CLOUDFLARE_ONE && renderCloudflareOneWithSidebar()}
          {activeServiceTab === SERVICE_CATEGORIES.DEVELOPER_PLATFORM && renderDevPlatformWithSidebar()}
        </div>

        {/* Action Buttons */}
        <div className="flex items-center justify-between px-6 pt-6 pb-4 border-t border-gray-200">
          <button
            type="button"
            onClick={() => setConfigStep(1)}
            className="px-6 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors"
          >
            ← Back to Settings
          </button>
          <div className="flex items-center space-x-3">
            {onCancel && (
              <button
                type="button"
                onClick={onCancel}
                className="px-6 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors flex items-center space-x-2"
              >
                <X className="w-4 h-4" />
                <span>Cancel</span>
              </button>
            )}
            <button
              type="submit"
              className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center space-x-2 shadow-sm"
            >
              <Save className="w-4 h-4" />
              <span>Save Configuration</span>
            </button>
          </div>
        </div>
      </div>
    );
  };

  const renderConfigSidebar = (sidebarItems, renderContent) => {
    const activeProduct = selectedConfigProduct || (sidebarItems.length > 0 ? sidebarItems[0].id : null);

    return (
      <div className="flex min-h-[400px]">
        <div className="w-56 border-r border-gray-200 bg-white flex-shrink-0">
          <nav className="py-2">
            {sidebarItems.map((item) => {
              const isActive = activeProduct === item.id;
              return (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => setSelectedConfigProduct(item.id)}
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
  };

  const renderAppServicesWithSidebar = () => {
    const items = [
      { id: 'enterpriseZones', label: 'Enterprise Zones' },
      { id: 'core', label: 'Enterprise Core' },
      { id: 'botManagement', label: 'Bot Management' },
      { id: 'apiShield', label: 'API Shield' },
      { id: 'pageShield', label: 'Page Shield' },
      { id: 'advancedRateLimiting', label: 'Adv. Rate Limiting' },
      { id: 'magicTransit', label: 'Magic Transit' },
    ];
    return renderConfigSidebar(items, (active) => {
      switch (active) {
        case 'enterpriseZones': return renderEnterpriseZonesConfig();
        case 'core': return renderCoreConfig();
        case 'botManagement': return renderBotManagementConfig();
        case 'apiShield': return renderApiShieldConfig();
        case 'pageShield': return renderPageShieldConfig();
        case 'advancedRateLimiting': return renderAdvancedRateLimitingConfig();
        case 'magicTransit': return renderMagicTransitConfig();
        default: return null;
      }
    });
  };

  const renderCloudflareOneWithSidebar = () => {
    const items = [
      { id: 'zeroTrustSeats', label: 'Zero Trust Seats' },
      { id: 'wan', label: 'WAN' },
    ];
    return renderConfigSidebar(items, (active) => {
      switch (active) {
        case 'zeroTrustSeats': return renderZeroTrustSeatsConfig();
        case 'wan': return renderWanConfig();
        default: return null;
      }
    });
  };

  const renderDevPlatformWithSidebar = () => {
    const items = [
      { id: 'workersPages', label: 'Workers & Pages' },
      { id: 'r2Storage', label: 'R2 Storage' },
    ];
    return renderConfigSidebar(items, (active) => {
      switch (active) {
        case 'workersPages': return renderWorkersPagesConfig();
        case 'r2Storage': return renderR2StorageConfig();
        default: return null;
      }
    });
  };

  const renderEnterpriseZonesConfig = () => {
    const appServices = formData.applicationServices;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Enterprise Zones</h4>
            <p className="text-sm text-gray-600 mt-1">Contracted zone counts and classification</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={appServices.core.enabled}
              onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, core: { ...prev.applicationServices.core, enabled: e.target.checked } } }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {appServices.core.enabled && (
          <div className="space-y-6 mt-4 pt-4 border-t border-gray-300">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Enterprise Zones (Total)</label>
                <input type="number" value={appServices.core.thresholdZones}
                  onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, core: { ...prev.applicationServices.core, thresholdZones: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 100" min="0" />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Primary Zones</label>
                <input type="number" value={appServices.core.primaryZones}
                  onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, core: { ...prev.applicationServices.core, primaryZones: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 80" min="0" />
                <p className="text-xs text-gray-500 mt-1">Zones with ≥50GB bandwidth/month</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Secondary Zones</label>
                <input type="number" value={appServices.core.secondaryZones}
                  onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, core: { ...prev.applicationServices.core, secondaryZones: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 20" min="0" />
                <p className="text-xs text-gray-500 mt-1">Zones with &lt;50GB bandwidth/month</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderCoreConfig = () => {
    const appServices = formData.applicationServices;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Enterprise Core</h4>
            <p className="text-sm text-gray-600 mt-1">HTTP Requests, Data Transfer, DNS Queries</p>
          </div>
        </div>
        {appServices.core.enabled ? (
          <div className="space-y-6 mt-4 pt-4 border-t border-gray-300">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">HTTP Requests (Millions)</label>
                <input type="number" value={appServices.core.thresholdRequests}
                  onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, core: { ...prev.applicationServices.core, thresholdRequests: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1000" min="0" step="0.01" />
                <p className="text-xs text-gray-500 mt-1">In millions (M)</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Data Transfer (TB)</label>
                <input type="number" value={appServices.core.thresholdBandwidth}
                  onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, core: { ...prev.applicationServices.core, thresholdBandwidth: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1.0" min="0" step="0.01" />
                <p className="text-xs text-gray-500 mt-1">In terabytes (TB)</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">DNS Queries (Millions)</label>
                <input type="number" value={appServices.core.thresholdDnsQueries}
                  onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, core: { ...prev.applicationServices.core, thresholdDnsQueries: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 500" min="0" step="0.01" />
                <p className="text-xs text-gray-500 mt-1">In millions (M)</p>
              </div>
            </div>
          </div>
        ) : (
          <div className="mt-4 pt-4 border-t border-gray-300">
            <p className="text-sm text-gray-500">Enable Application Services in Enterprise Zones first.</p>
          </div>
        )}
      </div>
    );
  };

  const renderAddonZoneConfig = (addonKey, title, thresholdLabel, toggleZoneFn, toggleAllFn) => {
    const appServices = formData.applicationServices;
    const addon = appServices[addonKey];
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div><h4 className="text-lg font-semibold text-gray-900">{title}</h4></div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={addon.enabled}
              onChange={(e) => handleChange('applicationServices', addonKey, { ...addon, enabled: e.target.checked })}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {addon.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">{thresholdLabel}</label>
              <input type="number" value={addon.threshold}
                onChange={(e) => handleChange('applicationServices', addonKey, { ...addon, threshold: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="e.g., 1000" min="0" step="0.01" />
              <p className="text-xs text-gray-500 mt-1">Total contracted across all selected zones</p>
            </div>
            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="block text-sm font-medium text-gray-700">Select Zones with {title}</label>
                {availableZones.length > 0 && (
                  <button type="button" onClick={toggleAllFn} className="text-sm text-blue-600 hover:text-blue-700 font-medium">
                    {addon.zones.length === availableZones.length ? 'Deselect All' : 'Select All'}
                  </button>
                )}
              </div>
              {loadingZones ? (
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-center">
                  <RefreshCw className="w-5 h-5 text-blue-600 animate-spin mx-auto mb-2" />
                  <p className="text-sm text-blue-800">Loading zones...</p>
                </div>
              ) : availableZones.length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                  <p className="text-sm text-yellow-800">Please save and load zones in Step 1 first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-36 overflow-y-auto">
                  {availableZones.map((zone) => (
                    <label key={zone.id} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={addon.zones.includes(zone.id)} onChange={() => toggleZoneFn(zone.id)}
                        className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
                      <span className="text-sm text-gray-900 font-medium">{zone.name}</span>
                    </label>
                  ))}
                </div>
              )}
              <p className="text-xs text-gray-500 mt-2">{addon.zones.length} zone{addon.zones.length !== 1 ? 's' : ''} selected</p>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderBotManagementConfig = () => renderAddonZoneConfig('botManagement', 'Bot Management', 'Contracted Likely Human Requests (Millions)', toggleBotManagementZone, toggleAllBotManagementZones);
  const renderApiShieldConfig = () => renderAddonZoneConfig('apiShield', 'API Shield', 'Contracted HTTP Requests (Millions)', toggleApiShieldZone, toggleAllApiShieldZones);
  const renderPageShieldConfig = () => renderAddonZoneConfig('pageShield', 'Page Shield', 'Contracted HTTP Requests (Millions)', togglePageShieldZone, toggleAllPageShieldZones);
  const renderAdvancedRateLimitingConfig = () => renderAddonZoneConfig('advancedRateLimiting', 'Advanced Rate Limiting', 'Contracted HTTP Requests (Millions)', toggleAdvancedRateLimitingZone, toggleAllAdvancedRateLimitingZones);

  const renderMagicTransitConfig = () => {
    const mt = formData.networkServices.magicTransit;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Magic Transit</h4>
            <p className="text-sm text-gray-600 mt-1">P95th bandwidth for Magic Transit tunnels</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={mt.enabled}
              onChange={(e) => setFormData(prev => ({ ...prev, networkServices: { ...prev.networkServices, magicTransit: { ...prev.networkServices.magicTransit, enabled: e.target.checked } } }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {mt.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Magic Transit</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Magic Transit contracted</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer">
                      <input type="checkbox" checked={mt.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...mt.accountIds, accountId] : mt.accountIds.filter(id => id !== accountId);
                          setFormData(prev => ({ ...prev, networkServices: { ...prev.networkServices, magicTransit: { ...prev.networkServices.magicTransit, accountIds: newIds } } }));
                        }}
                        className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
                      <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
            <div className="max-w-md">
              <label className="block text-sm font-medium text-gray-700 mb-2">Contracted Bandwidth (Mbps)</label>
              <input type="number" value={mt.threshold}
                onChange={(e) => setFormData(prev => ({ ...prev, networkServices: { ...prev.networkServices, magicTransit: { ...prev.networkServices.magicTransit, threshold: e.target.value } } }))}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1000" min="0" />
              <p className="text-xs text-gray-500 mt-1">P95th ingress bandwidth threshold in Mbps</p>
            </div>
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
              <label className="flex items-center space-x-3 cursor-pointer">
                <input type="checkbox" checked={mt.egressEnabled || false}
                  onChange={(e) => setFormData(prev => ({ ...prev, networkServices: { ...prev.networkServices, magicTransit: { ...prev.networkServices.magicTransit, egressEnabled: e.target.checked } } }))}
                  className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
                <div>
                  <span className="text-sm font-medium text-gray-900">Egress Enabled</span>
                  <p className="text-xs text-gray-600 mt-0.5">Check if your contract includes egress bandwidth billing</p>
                </div>
              </label>
              {mt.egressEnabled && (
                <div className="mt-4 pt-4 border-t border-blue-200">
                  <label className="block text-sm font-medium text-gray-700 mb-2">Egress Contracted Bandwidth (Mbps)</label>
                  <input type="number" value={mt.egressThreshold || ''}
                    onChange={(e) => setFormData(prev => ({ ...prev, networkServices: { ...prev.networkServices, magicTransit: { ...prev.networkServices.magicTransit, egressThreshold: e.target.value } } }))}
                    className="w-full max-w-md px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 500" min="0" />
                  <p className="text-xs text-gray-500 mt-1">P95th egress bandwidth threshold in Mbps</p>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderZeroTrustSeatsConfig = () => {
    const zt = formData.zeroTrust.seats;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Zero Trust Seats</h4>
            <p className="text-sm text-gray-600 mt-1">Active users consuming Access or Gateway seats</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={zt.enabled}
              onChange={(e) => setFormData(prev => ({ ...prev, zeroTrust: { ...prev.zeroTrust, seats: { ...prev.zeroTrust.seats, enabled: e.target.checked } } }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {zt.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Zero Trust Seats</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Enterprise Zero Trust seats contracted</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer">
                      <input type="checkbox" checked={zt.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...zt.accountIds, accountId] : zt.accountIds.filter(id => id !== accountId);
                          setFormData(prev => ({ ...prev, zeroTrust: { ...prev.zeroTrust, seats: { ...prev.zeroTrust.seats, accountIds: newIds } } }));
                        }}
                        className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
                      <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
            <div className="max-w-md">
              <label className="block text-sm font-medium text-gray-700 mb-2">Contracted Seats</label>
              <input type="number" value={zt.threshold}
                onChange={(e) => setFormData(prev => ({ ...prev, zeroTrust: { ...prev.zeroTrust, seats: { ...prev.zeroTrust.seats, threshold: e.target.value } } }))}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 100" min="0" />
              <p className="text-xs text-gray-500 mt-1">Total contracted seats across selected accounts</p>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderWanConfig = () => {
    const wan = formData.networkServices.magicWan;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">WAN</h4>
            <p className="text-sm text-gray-600 mt-1">P95th bandwidth for WAN tunnels</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={wan.enabled}
              onChange={(e) => setFormData(prev => ({ ...prev, networkServices: { ...prev.networkServices, magicWan: { ...prev.networkServices.magicWan, enabled: e.target.checked } } }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {wan.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with WAN</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have WAN contracted</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer">
                      <input type="checkbox" checked={wan.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...wan.accountIds, accountId] : wan.accountIds.filter(id => id !== accountId);
                          setFormData(prev => ({ ...prev, networkServices: { ...prev.networkServices, magicWan: { ...prev.networkServices.magicWan, accountIds: newIds } } }));
                        }}
                        className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
                      <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
            <div className="max-w-md">
              <label className="block text-sm font-medium text-gray-700 mb-2">Contracted Bandwidth (Mbps)</label>
              <input type="number" value={wan.threshold}
                onChange={(e) => setFormData(prev => ({ ...prev, networkServices: { ...prev.networkServices, magicWan: { ...prev.networkServices.magicWan, threshold: e.target.value } } }))}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1000" min="0" />
              <p className="text-xs text-gray-500 mt-1">P95th bandwidth threshold in Mbps</p>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderWorkersPagesConfig = () => {
    const wp = formData.developerServices.workersPages;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Workers & Pages</h4>
            <p className="text-sm text-gray-600 mt-1">Serverless compute requests and CPU time</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={wp.enabled}
              onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, workersPages: { ...prev.developerServices.workersPages, enabled: e.target.checked } } }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {wp.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Workers & Pages</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Workers & Pages usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer">
                      <input type="checkbox" checked={wp.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...wp.accountIds, accountId] : wp.accountIds.filter(id => id !== accountId);
                          setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, workersPages: { ...prev.developerServices.workersPages, accountIds: newIds } } }));
                        }}
                        className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
                      <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
            <div className="max-w-md">
              <label className="block text-sm font-medium text-gray-700 mb-2">Contracted Requests (millions)</label>
              <input type="number" value={wp.requestsThreshold}
                onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, workersPages: { ...prev.developerServices.workersPages, requestsThreshold: e.target.value } } }))}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 50" min="0" />
              <p className="text-xs text-gray-500 mt-1">Monthly request threshold in millions</p>
            </div>
            <div className="max-w-md">
              <label className="block text-sm font-medium text-gray-700 mb-2">Contracted CPU Time (million ms)</label>
              <input type="number" value={wp.cpuTimeThreshold}
                onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, workersPages: { ...prev.developerServices.workersPages, cpuTimeThreshold: e.target.value } } }))}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 100" min="0" />
              <p className="text-xs text-gray-500 mt-1">Monthly CPU time threshold in million milliseconds</p>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderR2StorageConfig = () => {
    const r2 = formData.developerServices.r2Storage;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">R2 Storage</h4>
            <p className="text-sm text-gray-600 mt-1">Object storage operations and capacity</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={r2.enabled}
              onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, r2Storage: { ...prev.developerServices.r2Storage, enabled: e.target.checked } } }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {r2.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with R2 Storage</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have R2 Storage usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer">
                      <input type="checkbox" checked={r2.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...r2.accountIds, accountId] : r2.accountIds.filter(id => id !== accountId);
                          setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, r2Storage: { ...prev.developerServices.r2Storage, accountIds: newIds } } }));
                        }}
                        className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
                      <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Class A Ops (millions)</label>
                <input type="number" value={r2.classAOpsThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, r2Storage: { ...prev.developerServices.r2Storage, classAOpsThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 10" min="0" />
                <p className="text-xs text-gray-500 mt-1">Write/List/Delete</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Class B Ops (millions)</label>
                <input type="number" value={r2.classBOpsThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, r2Storage: { ...prev.developerServices.r2Storage, classBOpsThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 100" min="0" />
                <p className="text-xs text-gray-500 mt-1">Read operations</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Storage (GB)</label>
                <input type="number" value={r2.storageThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, r2Storage: { ...prev.developerServices.r2Storage, storageThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 100" min="0" />
                <p className="text-xs text-gray-500 mt-1">Total storage capacity</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  // --- Legacy monolithic render functions (no longer called directly) ---
  // Application Services Configuration
  const renderApplicationServicesConfig = () => {
    const appServices = formData.applicationServices;
    
    return (
      <div className="space-y-8">
        {/* Core Section */}
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h4 className="text-lg font-semibold text-gray-900">App Services Core</h4>
              <p className="text-sm text-gray-600 mt-1">
                Zones, HTTP Requests, Data Transfer, DNS Queries
              </p>
            </div>
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                checked={appServices.core.enabled}
                onChange={(e) => setFormData(prev => ({
                  ...prev,
                  applicationServices: {
                    ...prev.applicationServices,
                    core: {
                      ...prev.applicationServices.core,
                      enabled: e.target.checked
                    }
                  }
                }))}
                className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
              />
              <span className="text-sm font-medium text-gray-700">Enable</span>
            </label>
          </div>

          {appServices.core.enabled && (
            <div className="space-y-6 mt-4 pt-4 border-t border-gray-300">
              {/* Zones */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Enterprise Zones (Total)
                  </label>
                  <input
                    type="number"
                    value={appServices.core.thresholdZones}
                    onChange={(e) => setFormData(prev => ({
                      ...prev,
                      applicationServices: {
                        ...prev.applicationServices,
                        core: { ...prev.applicationServices.core, thresholdZones: e.target.value }
                      }
                    }))}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="e.g., 100"
                    min="0"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Primary Zones
                  </label>
                  <input
                    type="number"
                    value={appServices.core.primaryZones}
                    onChange={(e) => setFormData(prev => ({
                      ...prev,
                      applicationServices: {
                        ...prev.applicationServices,
                        core: { ...prev.applicationServices.core, primaryZones: e.target.value }
                      }
                    }))}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="e.g., 80"
                    min="0"
                  />
                  <p className="text-xs text-gray-500 mt-1">Zones with ≥50GB bandwidth/month</p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Secondary Zones
                  </label>
                  <input
                    type="number"
                    value={appServices.core.secondaryZones}
                    onChange={(e) => setFormData(prev => ({
                      ...prev,
                      applicationServices: {
                        ...prev.applicationServices,
                        core: { ...prev.applicationServices.core, secondaryZones: e.target.value }
                      }
                    }))}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="e.g., 20"
                    min="0"
                  />
                  <p className="text-xs text-gray-500 mt-1">Zones with &lt;50GB bandwidth/month</p>
                </div>
              </div>

              {/* Zone Breakdown Error */}
              {errors.zoneBreakdown && (
                <div className="bg-red-50 border-l-4 border-red-400 p-4">
                  <div className="flex items-start">
                    <AlertTriangle className="w-5 h-5 text-red-400 mt-0.5 mr-3 flex-shrink-0" />
                    <p className="text-sm text-red-700">{errors.zoneBreakdown}</p>
                  </div>
                </div>
              )}

              {/* Other SKUs */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    HTTP Requests (Millions)
                  </label>
                  <input
                    type="number"
                    value={appServices.core.thresholdRequests}
                    onChange={(e) => setFormData(prev => ({
                      ...prev,
                      applicationServices: {
                        ...prev.applicationServices,
                        core: { ...prev.applicationServices.core, thresholdRequests: e.target.value }
                      }
                    }))}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="e.g., 1000"
                    min="0"
                    step="0.01"
                  />
                  <p className="text-xs text-gray-500 mt-1">In millions (M)</p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Data Transfer (TB)
                  </label>
                  <input
                    type="number"
                    value={appServices.core.thresholdBandwidth}
                    onChange={(e) => setFormData(prev => ({
                      ...prev,
                      applicationServices: {
                        ...prev.applicationServices,
                        core: { ...prev.applicationServices.core, thresholdBandwidth: e.target.value }
                      }
                    }))}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="e.g., 1.0"
                    min="0"
                    step="0.01"
                  />
                  <p className="text-xs text-gray-500 mt-1">In terabytes (TB)</p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    DNS Queries (Millions)
                  </label>
                  <input
                    type="number"
                    value={appServices.core.thresholdDnsQueries}
                    onChange={(e) => setFormData(prev => ({
                      ...prev,
                      applicationServices: {
                        ...prev.applicationServices,
                        core: { ...prev.applicationServices.core, thresholdDnsQueries: e.target.value }
                      }
                    }))}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="e.g., 500"
                    min="0"
                    step="0.01"
                  />
                  <p className="text-xs text-gray-500 mt-1">In millions (M)</p>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Add-ons Section */}
        <div>
          <h4 className="text-lg font-semibold text-gray-900 mb-4">Add-ons</h4>

          {/* Add-ons Grid - 2 columns */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {/* Bot Management */}
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
            <div className="flex items-start justify-between mb-4">
              <div>
                <h5 className="text-md font-semibold text-gray-900">Bot Management</h5>
              </div>
              <label className="flex items-center space-x-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={appServices.botManagement.enabled}
                  onChange={(e) => handleChange('applicationServices', 'botManagement', {
                    ...appServices.botManagement,
                    enabled: e.target.checked
                  })}
                  className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                />
                <span className="text-sm font-medium text-gray-700">Enable</span>
              </label>
            </div>

            {appServices.botManagement.enabled && (
              <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
                {/* Threshold Input */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Contracted Likely Human Requests (Millions)
                  </label>
                  <input
                    type="number"
                    value={appServices.botManagement.threshold}
                    onChange={(e) => handleChange('applicationServices', 'botManagement', {
                      ...appServices.botManagement,
                      threshold: e.target.value
                    })}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="e.g., 1000"
                    min="0"
                    step="0.01"
                  />
                  <p className="text-xs text-gray-500 mt-1">Total contracted Likely Human requests across all selected zones</p>
                </div>

                {/* Zone Selection */}
                <div>
                  <div className="flex items-center justify-between mb-2">
                    <label className="block text-sm font-medium text-gray-700">
                      Select Zones with Bot Management
                    </label>
                    {availableZones.length > 0 && (
                      <button
                        type="button"
                        onClick={toggleAllBotManagementZones}
                        className="text-sm text-blue-600 hover:text-blue-700 font-medium"
                      >
                        {appServices.botManagement.zones.length === availableZones.length ? 'Deselect All' : 'Select All'}
                      </button>
                    )}
                  </div>
                  
                  {loadingZones ? (
                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-center">
                      <RefreshCw className="w-5 h-5 text-blue-600 animate-spin mx-auto mb-2" />
                      <p className="text-sm text-blue-800">
                        Loading zones from your account...
                      </p>
                    </div>
                  ) : availableZones.length === 0 ? (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                      <p className="text-sm text-yellow-800">
                        Please save and load zones in Step 1 first to select zones for Bot Management.
                      </p>
                    </div>
                  ) : (
                    <div className="bg-white border border-gray-300 rounded-lg max-h-36 overflow-y-auto">
                      {availableZones.map((zone) => (
                        <label
                          key={zone.id}
                          className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0"
                        >
                          <input
                            type="checkbox"
                            checked={appServices.botManagement.zones.includes(zone.id)}
                            onChange={() => toggleBotManagementZone(zone.id)}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-900 font-medium">{zone.name}</span>
                        </label>
                      ))}
                    </div>
                  )}
                  <p className="text-xs text-gray-500 mt-2">
                    {appServices.botManagement.zones.length} zone{appServices.botManagement.zones.length !== 1 ? 's' : ''} selected
                  </p>
                </div>
              </div>
            )}
          </div>

            {/* API Shield */}
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
            <div className="flex items-start justify-between mb-4">
              <div>
                <h5 className="text-md font-semibold text-gray-900">API Shield</h5>
              </div>
              <label className="flex items-center space-x-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={appServices.apiShield.enabled}
                  onChange={(e) => handleChange('applicationServices', 'apiShield', {
                    ...appServices.apiShield,
                    enabled: e.target.checked
                  })}
                  className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                />
                <span className="text-sm font-medium text-gray-700">Enable</span>
              </label>
            </div>

            {appServices.apiShield.enabled && (
              <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Contracted HTTP Requests (Millions)
                  </label>
                  <input
                    type="number"
                    value={appServices.apiShield.threshold}
                    onChange={(e) => handleChange('applicationServices', 'apiShield', {
                      ...appServices.apiShield,
                      threshold: e.target.value
                    })}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="e.g., 1000"
                    min="0"
                    step="0.01"
                  />
                  <p className="text-xs text-gray-500 mt-1">Total contracted requests across all selected zones</p>
                </div>

                <div>
                  <div className="flex items-center justify-between mb-2">
                    <label className="block text-sm font-medium text-gray-700">
                      Select Zones with API Shield
                    </label>
                    {availableZones.length > 0 && (
                      <button
                        type="button"
                        onClick={toggleAllApiShieldZones}
                        className="text-sm text-blue-600 hover:text-blue-700 font-medium"
                      >
                        {appServices.apiShield.zones.length === availableZones.length ? 'Deselect All' : 'Select All'}
                      </button>
                    )}
                  </div>
                  
                  {loadingZones ? (
                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-center">
                      <RefreshCw className="w-5 h-5 text-blue-600 animate-spin mx-auto mb-2" />
                      <p className="text-sm text-blue-800">Loading zones...</p>
                    </div>
                  ) : availableZones.length === 0 ? (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                      <p className="text-sm text-yellow-800">
                        Please save and load zones in Step 1 first.
                      </p>
                    </div>
                  ) : (
                    <div className="bg-white border border-gray-300 rounded-lg max-h-36 overflow-y-auto">
                      {availableZones.map((zone) => (
                        <label
                          key={zone.id}
                          className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0"
                        >
                          <input
                            type="checkbox"
                            checked={appServices.apiShield.zones.includes(zone.id)}
                            onChange={() => toggleApiShieldZone(zone.id)}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-900 font-medium">{zone.name}</span>
                        </label>
                      ))}
                    </div>
                  )}
                  <p className="text-xs text-gray-500 mt-2">
                    {appServices.apiShield.zones.length} zone{appServices.apiShield.zones.length !== 1 ? 's' : ''} selected
                  </p>
                </div>
              </div>
            )}
          </div>

            {/* Page Shield */}
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
            <div className="flex items-start justify-between mb-4">
              <div>
                <h5 className="text-md font-semibold text-gray-900">Page Shield</h5>
              </div>
              <label className="flex items-center space-x-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={appServices.pageShield.enabled}
                  onChange={(e) => handleChange('applicationServices', 'pageShield', {
                    ...appServices.pageShield,
                    enabled: e.target.checked
                  })}
                  className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                />
                <span className="text-sm font-medium text-gray-700">Enable</span>
              </label>
            </div>

            {appServices.pageShield.enabled && (
              <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Contracted HTTP Requests (Millions)
                  </label>
                  <input
                    type="number"
                    value={appServices.pageShield.threshold}
                    onChange={(e) => handleChange('applicationServices', 'pageShield', {
                      ...appServices.pageShield,
                      threshold: e.target.value
                    })}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="e.g., 1000"
                    min="0"
                    step="0.01"
                  />
                  <p className="text-xs text-gray-500 mt-1">Total contracted requests across all selected zones</p>
                </div>

                <div>
                  <div className="flex items-center justify-between mb-2">
                    <label className="block text-sm font-medium text-gray-700">
                      Select Zones with Page Shield
                    </label>
                    {availableZones.length > 0 && (
                      <button
                        type="button"
                        onClick={toggleAllPageShieldZones}
                        className="text-sm text-blue-600 hover:text-blue-700 font-medium"
                      >
                        {appServices.pageShield.zones.length === availableZones.length ? 'Deselect All' : 'Select All'}
                      </button>
                    )}
                  </div>
                  
                  {loadingZones ? (
                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-center">
                      <RefreshCw className="w-5 h-5 text-blue-600 animate-spin mx-auto mb-2" />
                      <p className="text-sm text-blue-800">Loading zones...</p>
                    </div>
                  ) : availableZones.length === 0 ? (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                      <p className="text-sm text-yellow-800">
                        Please save and load zones in Step 1 first.
                      </p>
                    </div>
                  ) : (
                    <div className="bg-white border border-gray-300 rounded-lg max-h-36 overflow-y-auto">
                      {availableZones.map((zone) => (
                        <label
                          key={zone.id}
                          className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0"
                        >
                          <input
                            type="checkbox"
                            checked={appServices.pageShield.zones.includes(zone.id)}
                            onChange={() => togglePageShieldZone(zone.id)}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-900 font-medium">{zone.name}</span>
                        </label>
                      ))}
                    </div>
                  )}
                  <p className="text-xs text-gray-500 mt-2">
                    {appServices.pageShield.zones.length} zone{appServices.pageShield.zones.length !== 1 ? 's' : ''} selected
                  </p>
                </div>
              </div>
            )}
          </div>

            {/* Advanced Rate Limiting */}
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
            <div className="flex items-start justify-between mb-4">
              <div>
                <h5 className="text-md font-semibold text-gray-900">Advanced Rate Limiting</h5>
              </div>
              <label className="flex items-center space-x-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={appServices.advancedRateLimiting.enabled}
                  onChange={(e) => handleChange('applicationServices', 'advancedRateLimiting', {
                    ...appServices.advancedRateLimiting,
                    enabled: e.target.checked
                  })}
                  className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                />
                <span className="text-sm font-medium text-gray-700">Enable</span>
              </label>
            </div>

            {appServices.advancedRateLimiting.enabled && (
              <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Contracted HTTP Requests (Millions)
                  </label>
                  <input
                    type="number"
                    value={appServices.advancedRateLimiting.threshold}
                    onChange={(e) => handleChange('applicationServices', 'advancedRateLimiting', {
                      ...appServices.advancedRateLimiting,
                      threshold: e.target.value
                    })}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="e.g., 1000"
                    min="0"
                    step="0.01"
                  />
                  <p className="text-xs text-gray-500 mt-1">Total contracted requests across all selected zones</p>
                </div>

                <div>
                  <div className="flex items-center justify-between mb-2">
                    <label className="block text-sm font-medium text-gray-700">
                      Select Zones with Advanced Rate Limiting
                    </label>
                    {availableZones.length > 0 && (
                      <button
                        type="button"
                        onClick={toggleAllAdvancedRateLimitingZones}
                        className="text-sm text-blue-600 hover:text-blue-700 font-medium"
                      >
                        {appServices.advancedRateLimiting.zones.length === availableZones.length ? 'Deselect All' : 'Select All'}
                      </button>
                    )}
                  </div>
                  
                  {loadingZones ? (
                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-center">
                      <RefreshCw className="w-5 h-5 text-blue-600 animate-spin mx-auto mb-2" />
                      <p className="text-sm text-blue-800">Loading zones...</p>
                    </div>
                  ) : availableZones.length === 0 ? (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                      <p className="text-sm text-yellow-800">
                        Please save and load zones in Step 1 first.
                      </p>
                    </div>
                  ) : (
                    <div className="bg-white border border-gray-300 rounded-lg max-h-36 overflow-y-auto">
                      {availableZones.map((zone) => (
                        <label
                          key={zone.id}
                          className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0"
                        >
                          <input
                            type="checkbox"
                            checked={appServices.advancedRateLimiting.zones.includes(zone.id)}
                            onChange={() => toggleAdvancedRateLimitingZone(zone.id)}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-900 font-medium">{zone.name}</span>
                        </label>
                      ))}
                    </div>
                  )}
                  <p className="text-xs text-gray-500 mt-2">
                    {appServices.advancedRateLimiting.zones.length} zone{appServices.advancedRateLimiting.zones.length !== 1 ? 's' : ''} selected
                  </p>
                </div>
              </div>
            )}
          </div>
          </div>
        </div>

        {/* Magic Transit Section */}
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h4 className="text-lg font-semibold text-gray-900">Magic Transit</h4>
              <p className="text-sm text-gray-600 mt-1">
                P95th bandwidth for Magic Transit tunnels
              </p>
            </div>
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                checked={formData.networkServices.magicTransit.enabled}
                onChange={(e) => setFormData(prev => ({
                  ...prev,
                  networkServices: {
                    ...prev.networkServices,
                    magicTransit: {
                      ...prev.networkServices.magicTransit,
                      enabled: e.target.checked
                    }
                  }
                }))}
                className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
              />
              <span className="text-sm font-medium text-gray-700">Enable</span>
            </label>
          </div>

          {formData.networkServices.magicTransit.enabled && (
            <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Select Accounts with Magic Transit
                </label>
                <p className="text-xs text-gray-500 mb-3">
                  Choose which accounts have Magic Transit contracted
                </p>
                {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                  <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                    <p className="text-sm text-yellow-800">
                      No accounts configured. Please add account IDs first.
                    </p>
                  </div>
                ) : (
                  <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = formData.networkServices.magicTransit.accountIds.includes(accountId);
                      return (
                        <label key={accountId} className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer">
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={(e) => {
                              const newAccountIds = e.target.checked
                                ? [...formData.networkServices.magicTransit.accountIds, accountId]
                                : formData.networkServices.magicTransit.accountIds.filter(id => id !== accountId);
                              setFormData(prev => ({
                                ...prev,
                                networkServices: {
                                  ...prev.networkServices,
                                  magicTransit: {
                                    ...prev.networkServices.magicTransit,
                                    accountIds: newAccountIds
                                  }
                                }
                              }));
                            }}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                        </label>
                      );
                    })}
                  </div>
                )}
              </div>

              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                <label className="flex items-center space-x-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={formData.networkServices.magicTransit.egressEnabled || false}
                    onChange={(e) => setFormData(prev => ({
                      ...prev,
                      networkServices: {
                        ...prev.networkServices,
                        magicTransit: {
                          ...prev.networkServices.magicTransit,
                          egressEnabled: e.target.checked
                        }
                      }
                    }))}
                    className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                  />
                  <div>
                    <span className="text-sm font-medium text-gray-900">Egress Enabled</span>
                    <p className="text-xs text-gray-600 mt-0.5">
                      Check if your contract includes egress bandwidth billing
                    </p>
                  </div>
                </label>
                {formData.networkServices.magicTransit.egressEnabled && (
                  <div className="mt-4 pt-4 border-t border-blue-200">
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Egress Contracted Bandwidth (Mbps)
                    </label>
                    <input
                      type="number"
                      value={formData.networkServices.magicTransit.egressThreshold || ''}
                      onChange={(e) => setFormData(prev => ({
                        ...prev,
                        networkServices: {
                          ...prev.networkServices,
                          magicTransit: {
                            ...prev.networkServices.magicTransit,
                            egressThreshold: e.target.value
                          }
                        }
                      }))}
                      className="w-full max-w-md px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                      placeholder="e.g., 500"
                      min="0"
                    />
                    <p className="text-xs text-gray-500 mt-1">P95th egress bandwidth threshold in Mbps</p>
                  </div>
                )}
              </div>

              <div className="max-w-md">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Contracted Bandwidth (Mbps)
                </label>
                <input
                  type="number"
                  value={formData.networkServices.magicTransit.threshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    networkServices: {
                      ...prev.networkServices,
                      magicTransit: {
                        ...prev.networkServices.magicTransit,
                        threshold: e.target.value
                      }
                    }
                  }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., 1000"
                  min="0"
                />
                <p className="text-xs text-gray-500 mt-1">P95th bandwidth threshold in Mbps</p>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };

  // Cloudflare One Configuration (Zero Trust Seats + WAN)
  const renderCloudflareOneConfig = () => {
    const ztServices = formData.zeroTrust;
    const netServices = formData.networkServices;

    return (
      <div className="space-y-8">
        {/* Zero Trust Seats */}
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h4 className="text-lg font-semibold text-gray-900">Zero Trust Seats</h4>
              <p className="text-sm text-gray-600 mt-1">
                Active users consuming Access or Gateway seats
              </p>
            </div>
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                checked={ztServices.seats.enabled}
                onChange={(e) => setFormData(prev => ({
                  ...prev,
                  zeroTrust: {
                    ...prev.zeroTrust,
                    seats: {
                      ...prev.zeroTrust.seats,
                      enabled: e.target.checked
                    }
                  }
                }))}
                className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
              />
              <span className="text-sm font-medium text-gray-700">Enable</span>
            </label>
          </div>

          {ztServices.seats.enabled && (
            <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Select Accounts with Zero Trust Seats
                </label>
                <p className="text-xs text-gray-500 mb-3">
                  Choose which accounts have Enterprise Zero Trust seats contracted
                </p>
                {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                  <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                    <p className="text-sm text-yellow-800">
                      No accounts configured. Please add account IDs first.
                    </p>
                  </div>
                ) : (
                  <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = ztServices.seats.accountIds.includes(accountId);
                      return (
                        <label key={accountId} className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer">
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={(e) => {
                              const newAccountIds = e.target.checked
                                ? [...ztServices.seats.accountIds, accountId]
                                : ztServices.seats.accountIds.filter(id => id !== accountId);
                              setFormData(prev => ({
                                ...prev,
                                zeroTrust: {
                                  ...prev.zeroTrust,
                                  seats: {
                                    ...prev.zeroTrust.seats,
                                    accountIds: newAccountIds
                                  }
                                }
                              }));
                            }}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                        </label>
                      );
                    })}
                  </div>
                )}
              </div>

              <div className="max-w-md">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Contracted Seats
                </label>
                <input
                  type="number"
                  value={ztServices.seats.threshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    zeroTrust: {
                      ...prev.zeroTrust,
                      seats: {
                        ...prev.zeroTrust.seats,
                        threshold: e.target.value
                      }
                    }
                  }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., 100"
                  min="0"
                />
                <p className="text-xs text-gray-500 mt-1">Total contracted seats across selected accounts</p>
              </div>
            </div>
          )}
        </div>

        {/* WAN */}
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h4 className="text-lg font-semibold text-gray-900">WAN</h4>
              <p className="text-sm text-gray-600 mt-1">
                P95th bandwidth for WAN tunnels
              </p>
            </div>
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                checked={netServices.magicWan.enabled}
                onChange={(e) => setFormData(prev => ({
                  ...prev,
                  networkServices: {
                    ...prev.networkServices,
                    magicWan: {
                      ...prev.networkServices.magicWan,
                      enabled: e.target.checked
                    }
                  }
                }))}
                className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
              />
              <span className="text-sm font-medium text-gray-700">Enable</span>
            </label>
          </div>

          {netServices.magicWan.enabled && (
            <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Select Accounts with WAN
                </label>
                <p className="text-xs text-gray-500 mb-3">
                  Choose which accounts have WAN contracted
                </p>
                {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                  <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                    <p className="text-sm text-yellow-800">
                      No accounts configured. Please add account IDs first.
                    </p>
                  </div>
                ) : (
                  <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = netServices.magicWan.accountIds.includes(accountId);
                      return (
                        <label key={accountId} className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer">
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={(e) => {
                              const newAccountIds = e.target.checked
                                ? [...netServices.magicWan.accountIds, accountId]
                                : netServices.magicWan.accountIds.filter(id => id !== accountId);
                              setFormData(prev => ({
                                ...prev,
                                networkServices: {
                                  ...prev.networkServices,
                                  magicWan: {
                                    ...prev.networkServices.magicWan,
                                    accountIds: newAccountIds
                                  }
                                }
                              }));
                            }}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                        </label>
                      );
                    })}
                  </div>
                )}
              </div>

              <div className="max-w-md">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Contracted Bandwidth (Mbps)
                </label>
                <input
                  type="number"
                  value={netServices.magicWan.threshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    networkServices: {
                      ...prev.networkServices,
                      magicWan: {
                        ...prev.networkServices.magicWan,
                        threshold: e.target.value
                      }
                    }
                  }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., 1000"
                  min="0"
                />
                <p className="text-xs text-gray-500 mt-1">P95th bandwidth threshold in Mbps</p>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };

  // Old Zero Trust Config - replaced by renderCloudflareOneConfig
  const renderZeroTrustConfig = () => {
    const ztServices = formData.zeroTrust;
    
    return (
      <div className="space-y-8">
        {/* Seats Section */}
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h4 className="text-lg font-semibold text-gray-900">Zero Trust Seats</h4>
              <p className="text-sm text-gray-600 mt-1">
                Active users consuming Access or Gateway seats
              </p>
            </div>
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                checked={ztServices.seats.enabled}
                onChange={(e) => setFormData(prev => ({
                  ...prev,
                  zeroTrust: {
                    ...prev.zeroTrust,
                    seats: {
                      ...prev.zeroTrust.seats,
                      enabled: e.target.checked
                    }
                  }
                }))}
                className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
              />
              <span className="text-sm font-medium text-gray-700">Enable</span>
            </label>
          </div>

          {ztServices.seats.enabled && (
            <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
              {/* Account Selection */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Select Accounts with Zero Trust Seats
                </label>
                <p className="text-xs text-gray-500 mb-3">
                  Choose which accounts have Enterprise Zero Trust seats contracted
                </p>
                
                {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                  <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                    <p className="text-sm text-yellow-800">
                      No accounts configured. Please add account IDs in the Account IDs step first.
                    </p>
                  </div>
                ) : (
                  <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId, index) => {
                      const isSelected = ztServices.seats.accountIds.includes(accountId);
                      
                      return (
                        <label 
                          key={accountId} 
                          className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer"
                        >
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={(e) => {
                              const newAccountIds = e.target.checked
                                ? [...ztServices.seats.accountIds, accountId]
                                : ztServices.seats.accountIds.filter(id => id !== accountId);
                              
                              setFormData(prev => ({
                                ...prev,
                                zeroTrust: {
                                  ...prev.zeroTrust,
                                  seats: {
                                    ...prev.zeroTrust.seats,
                                    accountIds: newAccountIds
                                  }
                                }
                              }));
                            }}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                        </label>
                      );
                    })}
                  </div>
                )}
              </div>

              {/* Threshold */}
              <div className="max-w-md">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Contracted Seats
                </label>
                <input
                  type="number"
                  value={ztServices.seats.threshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    zeroTrust: {
                      ...prev.zeroTrust,
                      seats: {
                        ...prev.zeroTrust.seats,
                        threshold: e.target.value
                      }
                    }
                  }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., 100"
                  min="0"
                />
                <p className="text-xs text-gray-500 mt-1">Total contracted seats across selected accounts</p>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };

  // Network Services Configuration
  const renderNetworkServicesConfig = () => {
    const netServices = formData.networkServices;
    
    const renderAccountSelector = (serviceKey, serviceName) => {
      const service = netServices[serviceKey];
      
      return (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h4 className="text-lg font-semibold text-gray-900">{serviceName}</h4>
              <p className="text-sm text-gray-600 mt-1">
                P95th bandwidth for {serviceName} tunnels
              </p>
            </div>
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                checked={service.enabled}
                onChange={(e) => setFormData(prev => ({
                  ...prev,
                  networkServices: {
                    ...prev.networkServices,
                    [serviceKey]: {
                      ...prev.networkServices[serviceKey],
                      enabled: e.target.checked
                    }
                  }
                }))}
                className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
              />
              <span className="text-sm font-medium text-gray-700">Enable</span>
            </label>
          </div>

          {service.enabled && (
            <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
              {/* Account Selection */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Select Accounts with {serviceName}
                </label>
                <p className="text-xs text-gray-500 mb-3">
                  Choose which accounts have {serviceName} contracted
                </p>
                
                {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                  <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                    <p className="text-sm text-yellow-800">
                      No accounts configured. Please add account IDs in the Account IDs step first.
                    </p>
                  </div>
                ) : (
                  <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = service.accountIds.includes(accountId);
                      
                      return (
                        <label 
                          key={accountId} 
                          className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer"
                        >
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={(e) => {
                              const newAccountIds = e.target.checked
                                ? [...service.accountIds, accountId]
                                : service.accountIds.filter(id => id !== accountId);
                              
                              setFormData(prev => ({
                                ...prev,
                                networkServices: {
                                  ...prev.networkServices,
                                  [serviceKey]: {
                                    ...prev.networkServices[serviceKey],
                                    accountIds: newAccountIds
                                  }
                                }
                              }));
                            }}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                        </label>
                      );
                    })}
                  </div>
                )}
              </div>

              {/* Egress Option (Magic Transit only) */}
              {serviceKey === 'magicTransit' && (
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                  <label className="flex items-center space-x-3 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={service.egressEnabled || false}
                      onChange={(e) => setFormData(prev => ({
                        ...prev,
                        networkServices: {
                          ...prev.networkServices,
                          [serviceKey]: {
                            ...prev.networkServices[serviceKey],
                            egressEnabled: e.target.checked
                          }
                        }
                      }))}
                      className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                    />
                    <div>
                      <span className="text-sm font-medium text-gray-900">Egress Enabled</span>
                      <p className="text-xs text-gray-600 mt-0.5">
                        Check this if your Magic Transit contract includes egress bandwidth billing
                      </p>
                    </div>
                  </label>
                  
                  {/* Egress Threshold - only show when egress is enabled */}
                  {service.egressEnabled && (
                    <div className="mt-4 pt-4 border-t border-blue-200">
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Egress Contracted Bandwidth (Mbps)
                      </label>
                      <input
                        type="number"
                        value={service.egressThreshold || ''}
                        onChange={(e) => setFormData(prev => ({
                          ...prev,
                          networkServices: {
                            ...prev.networkServices,
                            [serviceKey]: {
                              ...prev.networkServices[serviceKey],
                              egressThreshold: e.target.value
                            }
                          }
                        }))}
                        className="w-full max-w-md px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                        placeholder="e.g., 500"
                        min="0"
                      />
                      <p className="text-xs text-gray-500 mt-1">P95th egress bandwidth threshold in Mbps</p>
                    </div>
                  )}
                </div>
              )}

              {/* Threshold */}
              <div className="max-w-md">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Contracted Bandwidth (Mbps)
                </label>
                <input
                  type="number"
                  value={service.threshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    networkServices: {
                      ...prev.networkServices,
                      [serviceKey]: {
                        ...prev.networkServices[serviceKey],
                        threshold: e.target.value
                      }
                    }
                  }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., 1000"
                  min="0"
                />
                <p className="text-xs text-gray-500 mt-1">P95th bandwidth threshold in Mbps</p>
              </div>
            </div>
          )}
        </div>
      );
    };
    
    return (
      <div className="space-y-8">
        {renderAccountSelector('magicTransit', 'Magic Transit')}
        {renderAccountSelector('magicWan', 'Magic WAN')}
      </div>
    );
  };

  // Developer Platform Configuration
  const renderDeveloperPlatformConfig = () => {
    const devServices = formData.developerServices;
    
    return (
      <div className="space-y-8">
        {/* Workers & Pages Section */}
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h4 className="text-lg font-semibold text-gray-900">Workers & Pages</h4>
              <p className="text-sm text-gray-600 mt-1">
                Serverless compute requests and CPU time
              </p>
            </div>
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                checked={devServices.workersPages.enabled}
                onChange={(e) => setFormData(prev => ({
                  ...prev,
                  developerServices: {
                    ...prev.developerServices,
                    workersPages: {
                      ...prev.developerServices.workersPages,
                      enabled: e.target.checked
                    }
                  }
                }))}
                className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
              />
              <span className="text-sm font-medium text-gray-700">Enable</span>
            </label>
          </div>

          {devServices.workersPages.enabled && (
            <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
              {/* Account Selection */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Select Accounts with Workers & Pages
                </label>
                <p className="text-xs text-gray-500 mb-3">
                  Choose which accounts have Workers & Pages usage to track
                </p>
                
                {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                  <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                    <p className="text-sm text-yellow-800">
                      No accounts configured. Please add account IDs in the Account IDs step first.
                    </p>
                  </div>
                ) : (
                  <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = devServices.workersPages.accountIds.includes(accountId);
                      
                      return (
                        <label 
                          key={accountId} 
                          className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer"
                        >
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={(e) => {
                              const newAccountIds = e.target.checked
                                ? [...devServices.workersPages.accountIds, accountId]
                                : devServices.workersPages.accountIds.filter(id => id !== accountId);
                              
                              setFormData(prev => ({
                                ...prev,
                                developerServices: {
                                  ...prev.developerServices,
                                  workersPages: {
                                    ...prev.developerServices.workersPages,
                                    accountIds: newAccountIds
                                  }
                                }
                              }));
                            }}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                        </label>
                      );
                    })}
                  </div>
                )}
              </div>

              {/* Requests Threshold */}
              <div className="max-w-md">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Contracted Requests (millions)
                </label>
                <input
                  type="number"
                  value={devServices.workersPages.requestsThreshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    developerServices: {
                      ...prev.developerServices,
                      workersPages: {
                        ...prev.developerServices.workersPages,
                        requestsThreshold: e.target.value
                      }
                    }
                  }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., 50"
                  min="0"
                />
                <p className="text-xs text-gray-500 mt-1">Monthly request threshold in millions</p>
              </div>

              {/* CPU Time Threshold */}
              <div className="max-w-md">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Contracted CPU Time (million ms)
                </label>
                <input
                  type="number"
                  value={devServices.workersPages.cpuTimeThreshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    developerServices: {
                      ...prev.developerServices,
                      workersPages: {
                        ...prev.developerServices.workersPages,
                        cpuTimeThreshold: e.target.value
                      }
                    }
                  }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., 100"
                  min="0"
                />
                <p className="text-xs text-gray-500 mt-1">Monthly CPU time threshold in million milliseconds</p>
              </div>
            </div>
          )}
        </div>

        {/* R2 Storage Section */}
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h4 className="text-lg font-semibold text-gray-900">R2 Storage</h4>
              <p className="text-sm text-gray-600 mt-1">
                Object storage operations and capacity
              </p>
            </div>
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                checked={devServices.r2Storage.enabled}
                onChange={(e) => setFormData(prev => ({
                  ...prev,
                  developerServices: {
                    ...prev.developerServices,
                    r2Storage: {
                      ...prev.developerServices.r2Storage,
                      enabled: e.target.checked
                    }
                  }
                }))}
                className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
              />
              <span className="text-sm font-medium text-gray-700">Enable</span>
            </label>
          </div>

          {devServices.r2Storage.enabled && (
            <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
              {/* Account Selection */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Select Accounts with R2 Storage
                </label>
                <p className="text-xs text-gray-500 mb-3">
                  Choose which accounts have R2 Storage usage to track
                </p>
                
                {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                  <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                    <p className="text-sm text-yellow-800">
                      No accounts configured. Please add account IDs in the Account IDs step first.
                    </p>
                  </div>
                ) : (
                  <div className="space-y-2 bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = devServices.r2Storage.accountIds.includes(accountId);
                      
                      return (
                        <label 
                          key={accountId} 
                          className="flex items-center space-x-3 p-2 hover:bg-gray-100 rounded cursor-pointer"
                        >
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={(e) => {
                              const newAccountIds = e.target.checked
                                ? [...devServices.r2Storage.accountIds, accountId]
                                : devServices.r2Storage.accountIds.filter(id => id !== accountId);
                              
                              setFormData(prev => ({
                                ...prev,
                                developerServices: {
                                  ...prev.developerServices,
                                  r2Storage: {
                                    ...prev.developerServices.r2Storage,
                                    accountIds: newAccountIds
                                  }
                                }
                              }));
                            }}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                          />
                          <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                        </label>
                      );
                    })}
                  </div>
                )}
              </div>

              {/* Class A Operations Threshold */}
              <div className="max-w-md">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Contracted Class A Operations (millions)
                </label>
                <input
                  type="number"
                  value={devServices.r2Storage.classAOpsThreshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    developerServices: {
                      ...prev.developerServices,
                      r2Storage: {
                        ...prev.developerServices.r2Storage,
                        classAOpsThreshold: e.target.value
                      }
                    }
                  }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., 10"
                  min="0"
                />
                <p className="text-xs text-gray-500 mt-1">Monthly Class A operations threshold in millions (list, write, delete)</p>
              </div>

              {/* Class B Operations Threshold */}
              <div className="max-w-md">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Contracted Class B Operations (millions)
                </label>
                <input
                  type="number"
                  value={devServices.r2Storage.classBOpsThreshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    developerServices: {
                      ...prev.developerServices,
                      r2Storage: {
                        ...prev.developerServices.r2Storage,
                        classBOpsThreshold: e.target.value
                      }
                    }
                  }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., 100"
                  min="0"
                />
                <p className="text-xs text-gray-500 mt-1">Monthly Class B operations threshold in millions (read)</p>
              </div>

              {/* Storage Threshold */}
              <div className="max-w-md">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Contracted Storage (GB)
                </label>
                <input
                  type="number"
                  value={devServices.r2Storage.storageThreshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    developerServices: {
                      ...prev.developerServices,
                      r2Storage: {
                        ...prev.developerServices.r2Storage,
                        storageThreshold: e.target.value
                      }
                    }
                  }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., 500"
                  min="0"
                />
                <p className="text-xs text-gray-500 mt-1">Total storage capacity threshold in GB</p>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };

  // Placeholder for future service configurations
  const renderPlaceholderConfig = (serviceName) => (
    <div className="text-center py-12">
      <div className="text-gray-400 mb-4">
        <TrendingUp className="w-16 h-16 mx-auto" />
      </div>
      <h4 className="text-lg font-semibold text-gray-700 mb-2">
        {serviceName} Configuration
      </h4>
      <p className="text-sm text-gray-500 max-w-md mx-auto">
        SKU configuration for {serviceName} will be added here. This service will support both account-level and zone-level metrics.
      </p>
    </div>
  );

  return (
    <form onSubmit={handleSubmit} className="bg-white rounded-lg shadow-lg border border-gray-200 overflow-hidden">
      {/* Header */}
      <div className="bg-gradient-to-r from-slate-700 to-slate-600 px-6 py-4">
        <h2 className="text-xl font-bold text-white">Dashboard Configuration</h2>
        <p className="text-slate-200 text-sm mt-1">
          {configStep === 1 
            ? 'Configure your accounts and notification settings' 
            : 'Set contracted thresholds for each service'}
        </p>
      </div>

      {/* Step Indicator */}
      <div className="bg-gray-50 px-6 py-3 border-b border-gray-200">
        <div className="flex items-center justify-center space-x-4">
          <div className={`flex items-center space-x-2 ${configStep === 1 ? 'text-blue-600 font-semibold' : 'text-gray-500'}`}>
            <div className={`w-8 h-8 rounded-full flex items-center justify-center ${configStep === 1 ? 'bg-blue-600 text-white' : 'bg-gray-300 text-gray-600'}`}>
              1
            </div>
            <span className="hidden sm:inline">Settings</span>
          </div>
          <div className="w-16 h-0.5 bg-gray-300"></div>
          <div className={`flex items-center space-x-2 ${configStep === 2 ? 'text-blue-600 font-semibold' : 'text-gray-500'}`}>
            <div className={`w-8 h-8 rounded-full flex items-center justify-center ${configStep === 2 ? 'bg-blue-600 text-white' : 'bg-gray-300 text-gray-600'}`}>
              2
            </div>
            <span className="hidden sm:inline">Service Thresholds</span>
          </div>
        </div>
      </div>

      {/* Content */}
      {configStep === 1 ? renderAccountIdsStep() : renderServiceThresholdsStep()}
    </form>
  );
}

export default ConfigFormNew;

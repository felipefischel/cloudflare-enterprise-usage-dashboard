import React, { useState, useEffect } from 'react';
import { Save, X, TrendingUp, Key, AlertTriangle, Plus, Trash2, RefreshCw, CheckCircle, ChevronRight, ChevronLeft, Bell } from 'lucide-react';
import { MessageSquare } from 'lucide-react';
import { SERVICE_CATEGORIES, SERVICE_METADATA, APPLICATION_SERVICES_SKUS } from '../constants/services';

function ConfigFormNew({ onSave, initialConfig, onCancel, cachedZones }) {
  // Configuration step: 1 = Account IDs, 2 = Notifications, 3 = Service Thresholds
  const [configStep, setConfigStep] = useState(initialConfig?.accountIds?.length > 0 ? 3 : 1);
  
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
        enabled: initialConfig?.applicationServices?.core?.enabled !== false,
        trafficEnabled: initialConfig?.applicationServices?.core?.trafficEnabled !== undefined ? initialConfig.applicationServices.core.trafficEnabled : (initialConfig?.applicationServices?.core?.enabled !== false),
        dnsEnabled: initialConfig?.applicationServices?.core?.dnsEnabled !== undefined ? initialConfig.applicationServices.core.dnsEnabled : (initialConfig?.applicationServices?.core?.enabled !== false),
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
      argo: {
        enabled: initialConfig?.applicationServices?.argo?.enabled || false,
        threshold: initialConfig?.applicationServices?.argo?.threshold ? (initialConfig.applicationServices.argo.threshold / 1e12) : '',
        zones: initialConfig?.applicationServices?.argo?.zones || [],
      },
      loadBalancing: {
        enabled: initialConfig?.applicationServices?.loadBalancing?.enabled || false,
        threshold: initialConfig?.applicationServices?.loadBalancing?.threshold || '',
        accountIds: initialConfig?.applicationServices?.loadBalancing?.accountIds || [],
      },
      customHostnames: {
        enabled: initialConfig?.applicationServices?.customHostnames?.enabled || false,
        threshold: initialConfig?.applicationServices?.customHostnames?.threshold || '',
        accountIds: initialConfig?.applicationServices?.customHostnames?.accountIds || [],
      },
      logExplorer: {
        enabled: initialConfig?.applicationServices?.logExplorer?.enabled || false,
        threshold: initialConfig?.applicationServices?.logExplorer?.threshold || '',
        accountIds: initialConfig?.applicationServices?.logExplorer?.accountIds || [],
      },
      cacheReserve: {
        enabled: initialConfig?.applicationServices?.cacheReserve?.enabled || false,
        storageThreshold: initialConfig?.applicationServices?.cacheReserve?.storageThreshold || '',
        classAOpsThreshold: initialConfig?.applicationServices?.cacheReserve?.classAOpsThreshold ? initialConfig.applicationServices.cacheReserve.classAOpsThreshold / 1e6 : '',
        classBOpsThreshold: initialConfig?.applicationServices?.cacheReserve?.classBOpsThreshold ? initialConfig.applicationServices.cacheReserve.classBOpsThreshold / 1e6 : '',
        zones: initialConfig?.applicationServices?.cacheReserve?.zones || [],
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
      spectrum: {
        enabled: initialConfig?.networkServices?.spectrum?.enabled || false,
        dataTransferThreshold: initialConfig?.networkServices?.spectrum?.dataTransferThreshold || '',
        connectionsThreshold: initialConfig?.networkServices?.spectrum?.connectionsThreshold || '',
        zones: initialConfig?.networkServices?.spectrum?.zones || [],
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
      d1: {
        enabled: initialConfig?.developerServices?.d1?.enabled || false,
        rowsReadThreshold: initialConfig?.developerServices?.d1?.rowsReadThreshold || '',
        rowsWrittenThreshold: initialConfig?.developerServices?.d1?.rowsWrittenThreshold || '',
        storageThreshold: initialConfig?.developerServices?.d1?.storageThreshold || '',
        accountIds: initialConfig?.developerServices?.d1?.accountIds || [],
      },
      kv: {
        enabled: initialConfig?.developerServices?.kv?.enabled || false,
        readsThreshold: initialConfig?.developerServices?.kv?.readsThreshold || '',
        writesThreshold: initialConfig?.developerServices?.kv?.writesThreshold || '',
        deletesThreshold: initialConfig?.developerServices?.kv?.deletesThreshold || '',
        listsThreshold: initialConfig?.developerServices?.kv?.listsThreshold || '',
        storageThreshold: initialConfig?.developerServices?.kv?.storageThreshold || '',
        accountIds: initialConfig?.developerServices?.kv?.accountIds || [],
      },
      stream: {
        enabled: initialConfig?.developerServices?.stream?.enabled || false,
        minutesStoredThreshold: initialConfig?.developerServices?.stream?.minutesStoredThreshold || '',
        minutesDeliveredThreshold: initialConfig?.developerServices?.stream?.minutesDeliveredThreshold || '',
        accountIds: initialConfig?.developerServices?.stream?.accountIds || [],
      },
      images: {
        enabled: initialConfig?.developerServices?.images?.enabled || false,
        imagesStoredThreshold: initialConfig?.developerServices?.images?.imagesStoredThreshold || '',
        imagesDeliveredThreshold: initialConfig?.developerServices?.images?.imagesDeliveredThreshold || '',
        accountIds: initialConfig?.developerServices?.images?.accountIds || [],
      },
      workersAI: {
        enabled: initialConfig?.developerServices?.workersAI?.enabled || false,
        neuronsThreshold: initialConfig?.developerServices?.workersAI?.neuronsThreshold || '',
        accountIds: initialConfig?.developerServices?.workersAI?.accountIds || [],
      },
      queues: {
        enabled: initialConfig?.developerServices?.queues?.enabled || false,
        operationsThreshold: initialConfig?.developerServices?.queues?.operationsThreshold || '',
        accountIds: initialConfig?.developerServices?.queues?.accountIds || [],
      },
      workersLogsTraces: {
        enabled: initialConfig?.developerServices?.workersLogsTraces?.enabled || false,
        eventsThreshold: initialConfig?.developerServices?.workersLogsTraces?.eventsThreshold || '',
        accountIds: initialConfig?.developerServices?.workersLogsTraces?.accountIds || [],
      },
      durableObjects: {
        enabled: initialConfig?.developerServices?.durableObjects?.enabled || false,
        sqliteEnabled: initialConfig?.developerServices?.durableObjects?.sqliteEnabled || false,
        kvStorageEnabled: initialConfig?.developerServices?.durableObjects?.kvStorageEnabled || false,
        requestsThreshold: initialConfig?.developerServices?.durableObjects?.requestsThreshold || '',
        durationThreshold: initialConfig?.developerServices?.durableObjects?.durationThreshold || '',
        sqliteRowsReadThreshold: initialConfig?.developerServices?.durableObjects?.sqliteRowsReadThreshold || '',
        sqliteRowsWrittenThreshold: initialConfig?.developerServices?.durableObjects?.sqliteRowsWrittenThreshold || '',
        kvReadUnitsThreshold: initialConfig?.developerServices?.durableObjects?.kvReadUnitsThreshold || '',
        kvWriteUnitsThreshold: initialConfig?.developerServices?.durableObjects?.kvWriteUnitsThreshold || '',
        kvDeletesThreshold: initialConfig?.developerServices?.durableObjects?.kvDeletesThreshold || '',
        storageThreshold: initialConfig?.developerServices?.durableObjects?.storageThreshold || '',
        accountIds: initialConfig?.developerServices?.durableObjects?.accountIds || [],
      },
    },
    
    slackEnabled: !!initialConfig?.slackWebhook,
    slackWebhook: initialConfig?.slackWebhook || '',
    alertFrequency: initialConfig?.alertFrequency || 'monthly',
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

  const toggleCacheReserveZone = (zoneId) => {
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        cacheReserve: {
          ...prev.applicationServices.cacheReserve,
          zones: prev.applicationServices.cacheReserve.zones.includes(zoneId)
            ? prev.applicationServices.cacheReserve.zones.filter(id => id !== zoneId)
            : [...prev.applicationServices.cacheReserve.zones, zoneId]
        }
      }
    }));
  };

  const toggleAllCacheReserveZones = () => {
    const currentZones = formData.applicationServices.cacheReserve.zones;
    const allZoneIds = availableZones.map(z => z.id);
    
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        cacheReserve: {
          ...prev.applicationServices.cacheReserve,
          zones: currentZones.length === allZoneIds.length ? [] : allZoneIds
        }
      }
    }));
  };

  const toggleArgoZone = (zoneId) => {
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        argo: {
          ...prev.applicationServices.argo,
          zones: prev.applicationServices.argo.zones.includes(zoneId)
            ? prev.applicationServices.argo.zones.filter(id => id !== zoneId)
            : [...prev.applicationServices.argo.zones, zoneId]
        }
      }
    }));
  };

  const toggleAllArgoZones = () => {
    const currentZones = formData.applicationServices.argo.zones;
    const allZoneIds = availableZones.map(z => z.id);
    
    setFormData(prev => ({
      ...prev,
      applicationServices: {
        ...prev.applicationServices,
        argo: {
          ...prev.applicationServices.argo,
          zones: currentZones.length === allZoneIds.length ? [] : allZoneIds
        }
      }
    }));
  };

  const toggleSpectrumZone = (zoneId) => {
    setFormData(prev => ({
      ...prev,
      networkServices: {
        ...prev.networkServices,
        spectrum: {
          ...prev.networkServices.spectrum,
          zones: prev.networkServices.spectrum.zones.includes(zoneId)
            ? prev.networkServices.spectrum.zones.filter(id => id !== zoneId)
            : [...prev.networkServices.spectrum.zones, zoneId]
        }
      }
    }));
  };

  const toggleAllSpectrumZones = () => {
    const currentZones = formData.networkServices.spectrum.zones;
    const allZoneIds = availableZones.map(z => z.id);
    setFormData(prev => ({
      ...prev,
      networkServices: {
        ...prev.networkServices,
        spectrum: {
          ...prev.networkServices.spectrum,
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
      setConfigStep(2); // Move to notifications step
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
    if (formData.slackEnabled && formData.slackWebhook && !formData.slackWebhook.startsWith('https://hooks.slack.com/')) {
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
            trafficEnabled: formData.applicationServices.core.trafficEnabled,
            dnsEnabled: formData.applicationServices.core.dnsEnabled,
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
          argo: {
            enabled: formData.applicationServices.argo.enabled,
            threshold: formData.applicationServices.argo.threshold ? parseFloat(formData.applicationServices.argo.threshold) * 1e12 : null,
            zones: formData.applicationServices.argo.zones,
          },
          loadBalancing: {
            enabled: formData.applicationServices.loadBalancing.enabled,
            threshold: formData.applicationServices.loadBalancing.threshold ? parseInt(formData.applicationServices.loadBalancing.threshold, 10) : null,
            accountIds: formData.applicationServices.loadBalancing.accountIds || [],
          },
          customHostnames: {
            enabled: formData.applicationServices.customHostnames.enabled,
            threshold: formData.applicationServices.customHostnames.threshold ? parseInt(formData.applicationServices.customHostnames.threshold, 10) : null,
            accountIds: formData.applicationServices.customHostnames.accountIds || [],
          },
          logExplorer: {
            enabled: formData.applicationServices.logExplorer.enabled,
            threshold: formData.applicationServices.logExplorer.threshold ? parseFloat(formData.applicationServices.logExplorer.threshold) : null,
            accountIds: formData.applicationServices.logExplorer.accountIds || [],
          },
          cacheReserve: {
            enabled: formData.applicationServices.cacheReserve.enabled,
            storageThreshold: formData.applicationServices.cacheReserve.storageThreshold ? parseFloat(formData.applicationServices.cacheReserve.storageThreshold) : null,
            classAOpsThreshold: formData.applicationServices.cacheReserve.classAOpsThreshold ? parseFloat(formData.applicationServices.cacheReserve.classAOpsThreshold) * 1e6 : null,
            classBOpsThreshold: formData.applicationServices.cacheReserve.classBOpsThreshold ? parseFloat(formData.applicationServices.cacheReserve.classBOpsThreshold) * 1e6 : null,
            zones: formData.applicationServices.cacheReserve.zones,
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
          spectrum: {
            enabled: formData.networkServices.spectrum.enabled,
            dataTransferThreshold: formData.networkServices.spectrum.dataTransferThreshold ? Number(formData.networkServices.spectrum.dataTransferThreshold) : null,
            connectionsThreshold: formData.networkServices.spectrum.connectionsThreshold ? Number(formData.networkServices.spectrum.connectionsThreshold) : null,
            zones: formData.networkServices.spectrum.zones || [],
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
          d1: {
            enabled: formData.developerServices.d1.enabled,
            rowsReadThreshold: formData.developerServices.d1.rowsReadThreshold ? Number(formData.developerServices.d1.rowsReadThreshold) : null,
            rowsWrittenThreshold: formData.developerServices.d1.rowsWrittenThreshold ? Number(formData.developerServices.d1.rowsWrittenThreshold) : null,
            storageThreshold: formData.developerServices.d1.storageThreshold ? Number(formData.developerServices.d1.storageThreshold) : null,
            accountIds: formData.developerServices.d1.accountIds || [],
          },
          kv: {
            enabled: formData.developerServices.kv.enabled,
            readsThreshold: formData.developerServices.kv.readsThreshold ? Number(formData.developerServices.kv.readsThreshold) : null,
            writesThreshold: formData.developerServices.kv.writesThreshold ? Number(formData.developerServices.kv.writesThreshold) : null,
            deletesThreshold: formData.developerServices.kv.deletesThreshold ? Number(formData.developerServices.kv.deletesThreshold) : null,
            listsThreshold: formData.developerServices.kv.listsThreshold ? Number(formData.developerServices.kv.listsThreshold) : null,
            storageThreshold: formData.developerServices.kv.storageThreshold ? Number(formData.developerServices.kv.storageThreshold) : null,
            accountIds: formData.developerServices.kv.accountIds || [],
          },
          stream: {
            enabled: formData.developerServices.stream.enabled,
            minutesStoredThreshold: formData.developerServices.stream.minutesStoredThreshold ? Number(formData.developerServices.stream.minutesStoredThreshold) : null,
            minutesDeliveredThreshold: formData.developerServices.stream.minutesDeliveredThreshold ? Number(formData.developerServices.stream.minutesDeliveredThreshold) : null,
            accountIds: formData.developerServices.stream.accountIds || [],
          },
          images: {
            enabled: formData.developerServices.images.enabled,
            imagesStoredThreshold: formData.developerServices.images.imagesStoredThreshold ? Number(formData.developerServices.images.imagesStoredThreshold) : null,
            imagesDeliveredThreshold: formData.developerServices.images.imagesDeliveredThreshold ? Number(formData.developerServices.images.imagesDeliveredThreshold) : null,
            accountIds: formData.developerServices.images.accountIds || [],
          },
          workersAI: {
            enabled: formData.developerServices.workersAI.enabled,
            neuronsThreshold: formData.developerServices.workersAI.neuronsThreshold ? Number(formData.developerServices.workersAI.neuronsThreshold) : null,
            accountIds: formData.developerServices.workersAI.accountIds || [],
          },
          queues: {
            enabled: formData.developerServices.queues.enabled,
            operationsThreshold: formData.developerServices.queues.operationsThreshold ? Number(formData.developerServices.queues.operationsThreshold) : null,
            accountIds: formData.developerServices.queues.accountIds || [],
          },
          workersLogsTraces: {
            enabled: formData.developerServices.workersLogsTraces.enabled,
            eventsThreshold: formData.developerServices.workersLogsTraces.eventsThreshold ? Number(formData.developerServices.workersLogsTraces.eventsThreshold) : null,
            accountIds: formData.developerServices.workersLogsTraces.accountIds || [],
          },
          durableObjects: {
            enabled: formData.developerServices.durableObjects.enabled,
            sqliteEnabled: formData.developerServices.durableObjects.sqliteEnabled,
            kvStorageEnabled: formData.developerServices.durableObjects.kvStorageEnabled,
            requestsThreshold: formData.developerServices.durableObjects.requestsThreshold ? Number(formData.developerServices.durableObjects.requestsThreshold) : null,
            durationThreshold: formData.developerServices.durableObjects.durationThreshold ? Number(formData.developerServices.durableObjects.durationThreshold) : null,
            sqliteRowsReadThreshold: formData.developerServices.durableObjects.sqliteRowsReadThreshold ? Number(formData.developerServices.durableObjects.sqliteRowsReadThreshold) : null,
            sqliteRowsWrittenThreshold: formData.developerServices.durableObjects.sqliteRowsWrittenThreshold ? Number(formData.developerServices.durableObjects.sqliteRowsWrittenThreshold) : null,
            kvReadUnitsThreshold: formData.developerServices.durableObjects.kvReadUnitsThreshold ? Number(formData.developerServices.durableObjects.kvReadUnitsThreshold) : null,
            kvWriteUnitsThreshold: formData.developerServices.durableObjects.kvWriteUnitsThreshold ? Number(formData.developerServices.durableObjects.kvWriteUnitsThreshold) : null,
            kvDeletesThreshold: formData.developerServices.durableObjects.kvDeletesThreshold ? Number(formData.developerServices.durableObjects.kvDeletesThreshold) : null,
            storageThreshold: formData.developerServices.durableObjects.storageThreshold ? Number(formData.developerServices.durableObjects.storageThreshold) : null,
            accountIds: formData.developerServices.durableObjects.accountIds || [],
          },
        },
        
        slackWebhook: formData.slackWebhook || '',
        alertFrequency: formData.alertFrequency || 'monthly',
        alertsEnabled: initialConfig?.alertsEnabled !== undefined ? initialConfig.alertsEnabled : false,
      };
      
      onSave(config);
    }
  };

  // Render Account IDs Step
  const renderAccountIdsStep = () => (
    <div className="p-6">
      <div className="max-w-2xl mx-auto space-y-6">
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

          <div className="flex items-center space-x-3 pt-2">
            <button
              type="button"
              onClick={handleLoadZones}
              disabled={loadingZones}
              className="px-5 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors flex items-center space-x-2 text-sm disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 ${loadingZones ? 'animate-spin' : ''}`} />
              <span>{loadingZones ? 'Fetching Data...' : 'Save & Fetch Data'}</span>
            </button>
            {zonesLoaded && (
              <div className="flex items-center space-x-2 text-green-600 text-sm">
                <CheckCircle className="w-4 h-4" />
                <span>Data loaded ({availableZones.length} zones)</span>
              </div>
            )}
          </div>
        </div>

        {/* Action Buttons */}
        <div className="flex items-center justify-between pt-6 border-t border-gray-200">
          <div>
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
          </div>
          <button
            type="button"
            onClick={() => {
              if (!zonesLoaded) {
                handleLoadZones();
              } else {
                setConfigStep(2);
              }
            }}
            disabled={loadingZones}
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center space-x-2 shadow-sm disabled:opacity-50"
          >
            <span>{zonesLoaded ? 'Next' : 'Save & Continue'}</span>
            <ChevronRight className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );

  // Render Notifications Step
  const renderNotificationsStep = () => (
    <div className="p-6">
      <div className="max-w-2xl mx-auto space-y-6">
        <div className="space-y-4">
          <div className="flex items-start space-x-3">
            <input
              type="checkbox"
              checked={formData.slackEnabled}
              onChange={(e) => setFormData(prev => ({ ...prev, slackEnabled: e.target.checked, slackWebhook: e.target.checked ? prev.slackWebhook : '' }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500 mt-0.5"
            />
            <div>
              <h3 className="text-lg font-semibold text-gray-900 flex items-center space-x-2">
                <Bell className="w-5 h-5" />
                <span>Slack Notifications</span>
              </h3>
              <p className="text-sm text-gray-500 mt-1">
                Receive alerts when usage reaches 90% of contracted thresholds
              </p>
            </div>
          </div>

          {formData.slackEnabled && (
            <div className="ml-8 space-y-4 pt-2">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  <MessageSquare className="inline w-4 h-4 mr-1" />
                  Slack Webhook URL
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

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Alert Frequency</label>
                <p className="text-xs text-gray-500 mb-2">How often threshold alerts (≥90%) can be sent per product</p>
                <div className="flex space-x-4">
                  <label className="flex items-center space-x-2 cursor-pointer">
                    <input type="radio" name="alertFrequency" value="monthly" checked={formData.alertFrequency === 'monthly'}
                      onChange={() => setFormData(prev => ({ ...prev, alertFrequency: 'monthly' }))}
                      className="w-4 h-4 text-blue-600 border-gray-300 focus:ring-blue-500" />
                    <span className="text-sm text-gray-700">Monthly</span>
                  </label>
                  <label className="flex items-center space-x-2 cursor-pointer">
                    <input type="radio" name="alertFrequency" value="weekly" checked={formData.alertFrequency === 'weekly'}
                      onChange={() => setFormData(prev => ({ ...prev, alertFrequency: 'weekly' }))}
                      className="w-4 h-4 text-blue-600 border-gray-300 focus:ring-blue-500" />
                    <span className="text-sm text-gray-700">Weekly</span>
                  </label>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Action Buttons */}
        <div className="flex items-center justify-between pt-6 border-t border-gray-200">
          <button
            type="button"
            onClick={() => setConfigStep(1)}
            className="px-6 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors flex items-center space-x-2"
          >
            <ChevronLeft className="w-4 h-4" />
            <span>Back</span>
          </button>
          <button
            type="button"
            onClick={() => setConfigStep(3)}
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center space-x-2 shadow-sm"
          >
            <span>Next</span>
            <ChevronRight className="w-4 h-4" />
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
          {activeServiceTab === SERVICE_CATEGORIES.NETWORK_SERVICES && renderNetworkServicesWithSidebar()}
          {activeServiceTab === SERVICE_CATEGORIES.CLOUDFLARE_ONE && renderCloudflareOneWithSidebar()}
          {activeServiceTab === SERVICE_CATEGORIES.DEVELOPER_PLATFORM && renderDevPlatformWithSidebar()}
        </div>

        {/* Action Buttons */}
        <div className="flex items-center justify-between px-6 pt-6 pb-4 border-t border-gray-200">
          <button
            type="button"
            onClick={() => setConfigStep(2)}
            className="px-6 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors"
          >
            ← Back to Notifications
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
    const firstClickable = sidebarItems.find(item => item.type !== 'header');
    const activeProduct = selectedConfigProduct || (firstClickable ? firstClickable.id : null);

    return (
      <div className="flex min-h-[400px]">
        <div className="w-56 border-r border-gray-200 bg-white flex-shrink-0">
          <nav className="py-2">
            {sidebarItems.map((item, index) => {
              if (item.type === 'header') {
                return (
                  <div key={item.label} className={`px-4 pt-3 pb-1 text-[10px] font-semibold uppercase tracking-wider text-gray-400`}>
                    {item.label}
                  </div>
                );
              }
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
      { type: 'header', label: 'Zones & Traffic' },
      { id: 'enterpriseZones', label: 'Enterprise Zones' },
      { id: 'core', label: 'Traffic' },
      { id: 'dns', label: 'DNS' },
      { type: 'header', label: 'Application Security' },
      { id: 'botManagement', label: 'Bot Management' },
      { id: 'apiShield', label: 'API Shield' },
      { id: 'pageShield', label: 'Page Shield' },
      { id: 'advancedRateLimiting', label: 'Adv. Rate Limiting' },
      { type: 'header', label: 'Delivery & Performance' },
      { id: 'argo', label: 'Argo Smart Routing' },
      { id: 'loadBalancing', label: 'Load Balancing' },
      { id: 'customHostnames', label: 'Custom Hostnames' },
      { id: 'cacheReserve', label: 'Cache Reserve' },
      { type: 'header', label: 'Logs & Analytics' },
      { id: 'logExplorer', label: 'Log Explorer' },
    ];
    return renderConfigSidebar(items, (active) => {
      switch (active) {
        case 'enterpriseZones': return renderEnterpriseZonesConfig();
        case 'core': return renderCoreConfig();
        case 'dns': return renderDnsConfig();
        case 'botManagement': return renderBotManagementConfig();
        case 'apiShield': return renderApiShieldConfig();
        case 'pageShield': return renderPageShieldConfig();
        case 'advancedRateLimiting': return renderAdvancedRateLimitingConfig();
        case 'argo': return renderArgoConfig();
        case 'loadBalancing': return renderLoadBalancingConfig();
        case 'customHostnames': return renderCustomHostnamesConfig();
        case 'logExplorer': return renderLogExplorerConfig();
        case 'cacheReserve': return renderCacheReserveConfig();
        default: return null;
      }
    });
  };

  const renderNetworkServicesWithSidebar = () => {
    const items = [
      { id: 'magicTransit', label: 'Magic Transit' },
      { id: 'spectrum', label: 'Spectrum' },
    ];
    return renderConfigSidebar(items, (active) => {
      switch (active) {
        case 'magicTransit': return renderMagicTransitConfig();
        case 'spectrum': return renderSpectrumConfig();
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
      { type: 'header', label: 'Compute' },
      { id: 'workersPages', label: 'Workers & Pages' },
      { id: 'queues', label: 'Queues' },
      { id: 'durableObjects', label: 'Durable Objects' },
      { id: 'workersLogsTraces', label: 'Workers Observability' },
      { type: 'header', label: 'AI' },
      { id: 'workersAI', label: 'Workers AI' },
      { type: 'header', label: 'Storage & Databases' },
      { id: 'r2Storage', label: 'R2 Storage' },
      { id: 'd1', label: 'D1 Database' },
      { id: 'kv', label: 'Workers KV' },
      { type: 'header', label: 'Media' },
      { id: 'stream', label: 'Stream' },
      { id: 'images', label: 'Images' },
    ];
    return renderConfigSidebar(items, (active) => {
      switch (active) {
        case 'workersPages': return renderWorkersPagesConfig();
        case 'r2Storage': return renderR2StorageConfig();
        case 'd1': return renderD1Config();
        case 'kv': return renderKVConfig();
        case 'stream': return renderStreamConfig();
        case 'images': return renderImagesConfig();
        case 'workersAI': return renderWorkersAIConfig();
        case 'queues': return renderQueuesConfig();
        case 'workersLogsTraces': return renderWorkersLogsTracesConfig();
        case 'durableObjects': return renderDurableObjectsConfig();
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
            <h4 className="text-lg font-semibold text-gray-900">Traffic</h4>
            <p className="text-sm text-gray-600 mt-1">HTTP Requests and Data Transfer</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={appServices.core.trafficEnabled}
              onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, core: { ...prev.applicationServices.core, trafficEnabled: e.target.checked } } }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {appServices.core.trafficEnabled ? (
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

  const renderDnsConfig = () => {
    const appServices = formData.applicationServices;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">DNS</h4>
            <p className="text-sm text-gray-600 mt-1">DNS query volume threshold</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={appServices.core.dnsEnabled}
              onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, core: { ...prev.applicationServices.core, dnsEnabled: e.target.checked } } }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {appServices.core.dnsEnabled ? (
          <div className="space-y-6 mt-4 pt-4 border-t border-gray-300">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
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

  const getZonesGroupedByAccount = () => {
    const groups = {};
    availableZones.forEach(zone => {
      const accountId = zone.account?.id || 'unknown';
      if (!groups[accountId]) {
        groups[accountId] = { name: zone.account?.name || getAccountName(accountId), zones: [] };
      }
      groups[accountId].zones.push(zone);
    });
    return groups;
  };

  const renderGroupedZoneSelector = (selectedZones, toggleZoneFn, setZonesFn) => {
    const groups = getZonesGroupedByAccount();
    const accountIds = Object.keys(groups);
    const multiAccount = accountIds.length > 1;

    if (!multiAccount) {
      return availableZones.map((zone) => (
        <label key={zone.id} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
          <input type="checkbox" checked={selectedZones.includes(zone.id)} onChange={() => toggleZoneFn(zone.id)}
            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
          <span className="text-sm text-gray-900 font-medium">{zone.name}</span>
        </label>
      ));
    }

    return accountIds.map(accountId => {
      const group = groups[accountId];
      const groupZoneIds = group.zones.map(z => z.id);
      const allSelected = groupZoneIds.every(id => selectedZones.includes(id));
      const someSelected = groupZoneIds.some(id => selectedZones.includes(id));

      const toggleAccountZones = () => {
        if (allSelected) {
          setZonesFn(selectedZones.filter(id => !groupZoneIds.includes(id)));
        } else {
          const newZones = [...new Set([...selectedZones, ...groupZoneIds])];
          setZonesFn(newZones);
        }
      };

      return (
        <div key={accountId}>
          <div className="flex items-center space-x-3 px-4 py-2 bg-gray-100 border-b border-gray-200 sticky top-0">
            <input
              type="checkbox"
              checked={allSelected}
              ref={el => { if (el) el.indeterminate = someSelected && !allSelected; }}
              onChange={toggleAccountZones}
              className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
            />
            <span className="text-xs font-semibold text-gray-600 uppercase tracking-wide">{group.name}</span>
          </div>
          {group.zones.map(zone => (
            <label key={zone.id} className="flex items-center space-x-3 px-4 py-3 pl-8 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
              <input type="checkbox" checked={selectedZones.includes(zone.id)} onChange={() => toggleZoneFn(zone.id)}
                className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
              <span className="text-sm text-gray-900 font-medium">{zone.name}</span>
            </label>
          ))}
        </div>
      );
    });
  };

  const renderAddonZoneConfig = (addonKey, title, thresholdLabel, toggleZoneFn, toggleAllFn, thresholdSubtitle) => {
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
        {addon.enabled && !appServices.core.enabled && !appServices.core.trafficEnabled && !appServices.core.dnsEnabled && (
          <div className="mt-4 pt-4 border-t border-gray-300">
            <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
              <p className="text-sm text-amber-800 font-medium">Requires Enterprise Zones, Traffic, or DNS to be enabled.</p>
              <p className="text-xs text-amber-600 mt-1">This product uses zone HTTP request data from those tabs. Enable at least one of them for data to appear.</p>
            </div>
          </div>
        )}
        {addon.enabled && (appServices.core.enabled || appServices.core.trafficEnabled || appServices.core.dnsEnabled) && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div className="max-w-xs">
              <label className="block text-sm font-medium text-gray-700 mb-2">{thresholdLabel}</label>
              <input type="number" value={addon.threshold}
                onChange={(e) => handleChange('applicationServices', addonKey, { ...addon, threshold: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="e.g., 1000" min="0" step="0.01" />
              <p className="text-xs text-gray-500 mt-1">Total contracted across all selected zones{thresholdSubtitle && <span className="ml-1 text-gray-400">{thresholdSubtitle}</span>}</p>
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
                  <p className="text-sm text-blue-800">Fetching data...</p>
                </div>
              ) : availableZones.length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                  <p className="text-sm text-yellow-800">Please save and fetch data in Step 1 first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-72 overflow-y-auto">
                  {renderGroupedZoneSelector(addon.zones, toggleZoneFn, (zones) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, [addonKey]: { ...prev.applicationServices[addonKey], zones } } })))}
                </div>
              )}
              <p className="text-xs text-gray-500 mt-2">{addon.zones.length} zone{addon.zones.length !== 1 ? 's' : ''} selected</p>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderBotManagementConfig = () => renderAddonZoneConfig('botManagement', 'Bot Management', 'Contracted Billable Human Requests (Millions)', toggleBotManagementZone, toggleAllBotManagementZones, '(Bot Score ≥ 30)');
  const renderApiShieldConfig = () => renderAddonZoneConfig('apiShield', 'API Shield', 'Contracted Billable HTTP Requests (Millions)', toggleApiShieldZone, toggleAllApiShieldZones);
  const renderPageShieldConfig = () => renderAddonZoneConfig('pageShield', 'Page Shield', 'Contracted Billable HTTP Requests (Millions)', togglePageShieldZone, toggleAllPageShieldZones);
  const renderAdvancedRateLimitingConfig = () => renderAddonZoneConfig('advancedRateLimiting', 'Advanced Rate Limiting', 'Contracted Billable HTTP Requests (Millions)', toggleAdvancedRateLimitingZone, toggleAllAdvancedRateLimitingZones);
  const renderArgoConfig = () => renderAddonZoneConfig('argo', 'Argo Smart Routing', 'Contracted Billable Data Transfer (TB)', toggleArgoZone, toggleAllArgoZones);

  const renderLoadBalancingConfig = () => {
    const lb = formData.applicationServices.loadBalancing;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Load Balancing</h4>
            <p className="text-sm text-gray-600 mt-1">Load balancer endpoint usage</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={lb.enabled}
              onChange={(e) => setFormData(prev => ({
                ...prev,
                applicationServices: {
                  ...prev.applicationServices,
                  loadBalancing: { ...prev.applicationServices.loadBalancing, enabled: e.target.checked }
                }
              }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {lb.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Load Balancing</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Load Balancing usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={lb.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...lb.accountIds, accountId] : lb.accountIds.filter(id => id !== accountId);
                          setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, loadBalancing: { ...prev.applicationServices.loadBalancing, accountIds: newIds } } }));
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Contracted Endpoints</label>
                <input type="number" value={lb.threshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, loadBalancing: { ...prev.applicationServices.loadBalancing, threshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 50" min="0" />
                <p className="text-xs text-gray-500 mt-1">Number of endpoints in contract</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderCustomHostnamesConfig = () => {
    const ch = formData.applicationServices.customHostnames;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Custom Hostnames</h4>
            <p className="text-sm text-gray-600 mt-1">Custom hostname usage across accounts</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={ch.enabled}
              onChange={(e) => setFormData(prev => ({
                ...prev,
                applicationServices: {
                  ...prev.applicationServices,
                  customHostnames: { ...prev.applicationServices.customHostnames, enabled: e.target.checked }
                }
              }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {ch.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Custom Hostnames</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Custom Hostnames usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={ch.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...ch.accountIds, accountId] : ch.accountIds.filter(id => id !== accountId);
                          setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, customHostnames: { ...prev.applicationServices.customHostnames, accountIds: newIds } } }));
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Contracted Custom Hostnames</label>
                <input type="number" value={ch.threshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, customHostnames: { ...prev.applicationServices.customHostnames, threshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 100" min="0" />
                <p className="text-xs text-gray-500 mt-1">Number of custom hostnames in contract</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderLogExplorerConfig = () => {
    const le = formData.applicationServices.logExplorer;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Log Explorer</h4>
            <p className="text-sm text-gray-600 mt-1">Data ingestion and retention usage</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={le.enabled}
              onChange={(e) => setFormData(prev => ({
                ...prev,
                applicationServices: {
                  ...prev.applicationServices,
                  logExplorer: { ...prev.applicationServices.logExplorer, enabled: e.target.checked }
                }
              }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {le.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Log Explorer</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Log Explorer usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={le.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...le.accountIds, accountId] : le.accountIds.filter(id => id !== accountId);
                          setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, logExplorer: { ...prev.applicationServices.logExplorer, accountIds: newIds } } }));
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Contracted Data Retention (GB)</label>
                <input type="number" value={le.threshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, logExplorer: { ...prev.applicationServices.logExplorer, threshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 100" min="0" step="0.01" />
                <p className="text-xs text-gray-500 mt-1">In GB per month</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderCacheReserveConfig = () => {
    const cr = formData.applicationServices.cacheReserve;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Cache Reserve</h4>
            <p className="text-sm text-gray-600 mt-1">Storage and operations for Cache Reserve</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={cr.enabled}
              onChange={(e) => setFormData(prev => ({
                ...prev,
                applicationServices: {
                  ...prev.applicationServices,
                  cacheReserve: { ...prev.applicationServices.cacheReserve, enabled: e.target.checked }
                }
              }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {cr.enabled && (
          <div className="space-y-4">
            <div className="grid grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Storage Threshold (TB)</label>
                <input
                  type="number"
                  value={cr.storageThreshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    applicationServices: {
                      ...prev.applicationServices,
                      cacheReserve: { ...prev.applicationServices.cacheReserve, storageThreshold: e.target.value }
                    }
                  }))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="e.g. 5"
                  step="0.1"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Class A Ops (Millions)</label>
                <input
                  type="number"
                  value={cr.classAOpsThreshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    applicationServices: {
                      ...prev.applicationServices,
                      cacheReserve: { ...prev.applicationServices.cacheReserve, classAOpsThreshold: e.target.value }
                    }
                  }))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="e.g. 10"
                  step="1"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Class B Ops (Millions)</label>
                <input
                  type="number"
                  value={cr.classBOpsThreshold}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    applicationServices: {
                      ...prev.applicationServices,
                      cacheReserve: { ...prev.applicationServices.cacheReserve, classBOpsThreshold: e.target.value }
                    }
                  }))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="e.g. 100"
                  step="1"
                />
              </div>
            </div>
            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="block text-sm font-medium text-gray-700">Select Zones</label>
                {availableZones.length > 0 && (
                  <button
                    type="button"
                    onClick={toggleAllCacheReserveZones}
                    className="text-sm text-blue-600 hover:text-blue-700 font-medium"
                  >
                    {cr.zones.length === availableZones.length ? 'Deselect All' : 'Select All'}
                  </button>
                )}
              </div>
              <div className="max-h-60 overflow-y-auto border border-gray-200 rounded-lg">
                {renderGroupedZoneSelector(cr.zones, toggleCacheReserveZone, (zones) => setFormData(prev => ({ ...prev, applicationServices: { ...prev.applicationServices, cacheReserve: { ...prev.applicationServices.cacheReserve, zones } } })))}
              </div>
              {cr.zones.length > 0 && (
                <p className="text-xs text-gray-500 mt-1">{cr.zones.length} zone(s) selected</p>
              )}
            </div>
          </div>
        )}
      </div>
    );
  };

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
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
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
            <div className="max-w-xs">
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
                    className="w-full max-w-xs px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 500" min="0" />
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
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
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
            <div className="max-w-xs">
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
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
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
            <div className="max-w-xs">
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

  const renderSpectrumConfig = () => {
    const spec = formData.networkServices.spectrum;
    const setSpec = (field, value) => setFormData(prev => ({ ...prev, networkServices: { ...prev.networkServices, spectrum: { ...prev.networkServices.spectrum, [field]: value } } }));
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Spectrum</h4>
            <p className="text-sm text-gray-600 mt-1">TCP/UDP proxy data transfer and connections</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={spec.enabled}
              onChange={(e) => setSpec('enabled', e.target.checked)}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {spec.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="block text-sm font-medium text-gray-700">Select Zones with Spectrum</label>
                {availableZones.length > 0 && (
                  <button type="button" onClick={toggleAllSpectrumZones} className="text-sm text-blue-600 hover:text-blue-700 font-medium">
                    {spec.zones.length === availableZones.length ? 'Deselect All' : 'Select All'}
                  </button>
                )}
              </div>
              {loadingZones ? (
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-center">
                  <RefreshCw className="w-5 h-5 text-blue-600 animate-spin mx-auto mb-2" />
                  <p className="text-sm text-blue-800">Fetching data...</p>
                </div>
              ) : availableZones.length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                  <p className="text-sm text-yellow-800">Please save and fetch data in Step 1 first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-72 overflow-y-auto">
                  {renderGroupedZoneSelector(spec.zones, toggleSpectrumZone, (zones) => setFormData(prev => ({ ...prev, networkServices: { ...prev.networkServices, spectrum: { ...prev.networkServices.spectrum, zones } } })))}
                </div>
              )}
              <p className="text-xs text-gray-500 mt-2">{spec.zones.length} zone{spec.zones.length !== 1 ? 's' : ''} selected</p>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Data Transfer (TB)</label>
                <input type="number" value={spec.dataTransferThreshold}
                  onChange={(e) => setSpec('dataTransferThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 10" min="0" step="0.1" />
                <p className="text-xs text-gray-500 mt-1">Monthly ingress + egress data transfer in TB</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Concurrent Connections</label>
                <input type="number" value={spec.connectionsThreshold}
                  onChange={(e) => setSpec('connectionsThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1000" min="0" />
                <p className="text-xs text-gray-500 mt-1">Max concurrent connections threshold</p>
              </div>
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
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
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
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Contracted Requests (millions)</label>
                <input type="number" value={wp.requestsThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, workersPages: { ...prev.developerServices.workersPages, requestsThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 50" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly request threshold</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Contracted CPU Time (million ms)</label>
                <input type="number" value={wp.cpuTimeThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, workersPages: { ...prev.developerServices.workersPages, cpuTimeThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 100" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly CPU time threshold</p>
              </div>
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
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Storage (TB)</label>
                <input type="number" value={r2.storageThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, r2Storage: { ...prev.developerServices.r2Storage, storageThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1" min="0" step="0.1" />
                <p className="text-xs text-gray-500 mt-1">Total storage capacity</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderD1Config = () => {
    const d1 = formData.developerServices.d1;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">D1 Database</h4>
            <p className="text-sm text-gray-600 mt-1">Serverless SQL database usage</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={d1.enabled}
              onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, d1: { ...prev.developerServices.d1, enabled: e.target.checked } } }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {d1.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with D1 Databases</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have D1 usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={d1.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...d1.accountIds, accountId] : d1.accountIds.filter(id => id !== accountId);
                          setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, d1: { ...prev.developerServices.d1, accountIds: newIds } } }));
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Rows Read (millions)</label>
                <input type="number" value={d1.rowsReadThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, d1: { ...prev.developerServices.d1, rowsReadThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 25000" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly rows read threshold</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Rows Written (millions)</label>
                <input type="number" value={d1.rowsWrittenThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, d1: { ...prev.developerServices.d1, rowsWrittenThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 50" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly rows written threshold</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Storage (GB)</label>
                <input type="number" value={d1.storageThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, d1: { ...prev.developerServices.d1, storageThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 5" min="0" step="0.1" />
                <p className="text-xs text-gray-500 mt-1">Total database storage</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderKVConfig = () => {
    const kv = formData.developerServices.kv;
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Workers KV</h4>
            <p className="text-sm text-gray-600 mt-1">Key-value storage operations and capacity</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={kv.enabled}
              onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, kv: { ...prev.developerServices.kv, enabled: e.target.checked } } }))}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {kv.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Workers KV</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have KV usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={kv.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...kv.accountIds, accountId] : kv.accountIds.filter(id => id !== accountId);
                          setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, kv: { ...prev.developerServices.kv, accountIds: newIds } } }));
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Reads (millions)</label>
                <input type="number" value={kv.readsThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, kv: { ...prev.developerServices.kv, readsThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 10" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly key reads</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Writes (millions)</label>
                <input type="number" value={kv.writesThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, kv: { ...prev.developerServices.kv, writesThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly key writes</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Deletes (millions)</label>
                <input type="number" value={kv.deletesThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, kv: { ...prev.developerServices.kv, deletesThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly key deletes</p>
              </div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">List Requests (millions)</label>
                <input type="number" value={kv.listsThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, kv: { ...prev.developerServices.kv, listsThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly list operations</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Storage (GB)</label>
                <input type="number" value={kv.storageThreshold}
                  onChange={(e) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, kv: { ...prev.developerServices.kv, storageThreshold: e.target.value } } }))}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1" min="0" step="0.1" />
                <p className="text-xs text-gray-500 mt-1">Total stored data</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderStreamConfig = () => {
    const stream = formData.developerServices.stream;
    const setStream = (field, value) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, stream: { ...prev.developerServices.stream, [field]: value } } }));
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Stream</h4>
            <p className="text-sm text-gray-600 mt-1">Video storage and delivery minutes</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={stream.enabled}
              onChange={(e) => setStream('enabled', e.target.checked)}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {stream.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Stream</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Stream usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={stream.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...stream.accountIds, accountId] : stream.accountIds.filter(id => id !== accountId);
                          setStream('accountIds', newIds);
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Minutes Stored (thousands)</label>
                <input type="number" value={stream.minutesStoredThreshold}
                  onChange={(e) => setStream('minutesStoredThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 100" min="0" />
                <p className="text-xs text-gray-500 mt-1">Total video duration stored</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Minutes Delivered (thousands)</label>
                <input type="number" value={stream.minutesDeliveredThreshold}
                  onChange={(e) => setStream('minutesDeliveredThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 500" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly video minutes delivered (in thousands)</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderImagesConfig = () => {
    const images = formData.developerServices.images;
    const setImages = (field, value) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, images: { ...prev.developerServices.images, [field]: value } } }));
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Images</h4>
            <p className="text-sm text-gray-600 mt-1">Image storage and delivery</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={images.enabled}
              onChange={(e) => setImages('enabled', e.target.checked)}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {images.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Images</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Images usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={images.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...images.accountIds, accountId] : images.accountIds.filter(id => id !== accountId);
                          setImages('accountIds', newIds);
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Images Stored (thousands)</label>
                <input type="number" value={images.imagesStoredThreshold}
                  onChange={(e) => setImages('imagesStoredThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 100" min="0" />
                <p className="text-xs text-gray-500 mt-1">Total images stored (in thousands)</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Images Delivered (thousands)</label>
                <input type="number" value={images.imagesDeliveredThreshold}
                  onChange={(e) => setImages('imagesDeliveredThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 500" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly images delivered (in thousands)</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderWorkersAIConfig = () => {
    const wai = formData.developerServices.workersAI;
    const setWAI = (field, value) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, workersAI: { ...prev.developerServices.workersAI, [field]: value } } }));
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Workers AI</h4>
            <p className="text-sm text-gray-600 mt-1">AI inference usage measured in neurons</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={wai.enabled}
              onChange={(e) => setWAI('enabled', e.target.checked)}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {wai.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Workers AI</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Workers AI usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={wai.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...wai.accountIds, accountId] : wai.accountIds.filter(id => id !== accountId);
                          setWAI('accountIds', newIds);
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Neurons (millions)</label>
                <input type="number" value={wai.neuronsThreshold}
                  onChange={(e) => setWAI('neuronsThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 50000" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly neurons allocation (in millions)</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderQueuesConfig = () => {
    const queues = formData.developerServices.queues;
    const setQueues = (field, value) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, queues: { ...prev.developerServices.queues, [field]: value } } }));
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Queues</h4>
            <p className="text-sm text-gray-600 mt-1">Message queue operations (write, read, delete)</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={queues.enabled}
              onChange={(e) => setQueues('enabled', e.target.checked)}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {queues.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Queues</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Queues usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={queues.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...queues.accountIds, accountId] : queues.accountIds.filter(id => id !== accountId);
                          setQueues('accountIds', newIds);
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Operations (millions)</label>
                <input type="number" value={queues.operationsThreshold}
                  onChange={(e) => setQueues('operationsThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 10" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly billable operations (in millions)</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderWorkersLogsTracesConfig = () => {
    const wlt = formData.developerServices.workersLogsTraces;
    const setWLT = (field, value) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, workersLogsTraces: { ...prev.developerServices.workersLogsTraces, [field]: value } } }));
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Workers Logs & Traces</h4>
            <p className="text-sm text-gray-600 mt-1">Observability events from Workers Logs and Traces</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={wlt.enabled}
              onChange={(e) => setWLT('enabled', e.target.checked)}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {wlt.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Workers Logs & Traces usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={wlt.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...wlt.accountIds, accountId] : wlt.accountIds.filter(id => id !== accountId);
                          setWLT('accountIds', newIds);
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Events (millions)</label>
                <input type="number" value={wlt.eventsThreshold}
                  onChange={(e) => setWLT('eventsThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 50" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly observability events (in millions)</p>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderDurableObjectsConfig = () => {
    const doObj = formData.developerServices.durableObjects;
    const setDO = (field, value) => setFormData(prev => ({ ...prev, developerServices: { ...prev.developerServices, durableObjects: { ...prev.developerServices.durableObjects, [field]: value } } }));
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-5">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h4 className="text-lg font-semibold text-gray-900">Durable Objects</h4>
            <p className="text-sm text-gray-600 mt-1">Compute, SQLite storage, and KV storage metrics</p>
          </div>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input type="checkbox" checked={doObj.enabled}
              onChange={(e) => setDO('enabled', e.target.checked)}
              className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
            <span className="text-sm font-medium text-gray-700">Enable</span>
          </label>
        </div>
        {doObj.enabled && (
          <div className="space-y-4 mt-4 pt-4 border-t border-gray-300">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Select Accounts with Durable Objects</label>
              <p className="text-xs text-gray-500 mb-3">Choose which accounts have Durable Objects usage to track</p>
              {formData.accountIds.filter(id => id.trim()).length === 0 ? (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                  <p className="text-sm text-yellow-800">No accounts configured. Please add account IDs first.</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                  {formData.accountIds.filter(id => id.trim()).map((accountId) => (
                    <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
                      <input type="checkbox" checked={doObj.accountIds.includes(accountId)}
                        onChange={(e) => {
                          const newIds = e.target.checked ? [...doObj.accountIds, accountId] : doObj.accountIds.filter(id => id !== accountId);
                          setDO('accountIds', newIds);
                        }}
                        className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
                      <span className="text-sm text-gray-700">{getAccountName(accountId)}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
            <h5 className="text-sm font-semibold text-gray-800 pt-2">Compute</h5>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Requests (millions)</label>
                <input type="number" value={doObj.requestsThreshold}
                  onChange={(e) => setDO('requestsThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly requests</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Duration (GB-s)</label>
                <input type="number" value={doObj.durationThreshold}
                  onChange={(e) => setDO('durationThreshold', e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 400000" min="0" />
                <p className="text-xs text-gray-500 mt-1">Monthly wall-clock duration</p>
              </div>
            </div>
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
              <label className="flex items-center space-x-3 cursor-pointer">
                <input type="checkbox" checked={doObj.sqliteEnabled || false}
                  onChange={(e) => setDO('sqliteEnabled', e.target.checked)}
                  className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
                <div>
                  <span className="text-sm font-medium text-gray-900">SQLite Storage Backend</span>
                  <p className="text-xs text-gray-600 mt-0.5">Enable if your Durable Objects use the SQLite storage backend</p>
                </div>
              </label>
              {doObj.sqliteEnabled && (
                <div className="mt-4 pt-4 border-t border-blue-200">
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">Rows Read (millions)</label>
                      <input type="number" value={doObj.sqliteRowsReadThreshold}
                        onChange={(e) => setDO('sqliteRowsReadThreshold', e.target.value)}
                        className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 25000" min="0" />
                      <p className="text-xs text-gray-500 mt-1">Monthly rows read</p>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">Rows Written (millions)</label>
                      <input type="number" value={doObj.sqliteRowsWrittenThreshold}
                        onChange={(e) => setDO('sqliteRowsWrittenThreshold', e.target.value)}
                        className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 50" min="0" />
                      <p className="text-xs text-gray-500 mt-1">Monthly rows written</p>
                    </div>
                  </div>
                </div>
              )}
            </div>
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
              <label className="flex items-center space-x-3 cursor-pointer">
                <input type="checkbox" checked={doObj.kvStorageEnabled || false}
                  onChange={(e) => setDO('kvStorageEnabled', e.target.checked)}
                  className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500" />
                <div>
                  <span className="text-sm font-medium text-gray-900">KV Storage Backend</span>
                  <p className="text-xs text-gray-600 mt-0.5">Enable if your Durable Objects use the KV storage backend</p>
                </div>
              </label>
              {doObj.kvStorageEnabled && (
                <div className="mt-4 pt-4 border-t border-blue-200">
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">Read Units (millions)</label>
                      <input type="number" value={doObj.kvReadUnitsThreshold}
                        onChange={(e) => setDO('kvReadUnitsThreshold', e.target.value)}
                        className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1" min="0" />
                      <p className="text-xs text-gray-500 mt-1">Monthly read request units</p>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">Write Units (millions)</label>
                      <input type="number" value={doObj.kvWriteUnitsThreshold}
                        onChange={(e) => setDO('kvWriteUnitsThreshold', e.target.value)}
                        className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1" min="0" />
                      <p className="text-xs text-gray-500 mt-1">Monthly write request units</p>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">Delete Requests (millions)</label>
                      <input type="number" value={doObj.kvDeletesThreshold}
                        onChange={(e) => setDO('kvDeletesThreshold', e.target.value)}
                        className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1" min="0" />
                      <p className="text-xs text-gray-500 mt-1">Monthly delete requests</p>
                    </div>
                  </div>
                </div>
              )}
            </div>
            {(doObj.sqliteEnabled || doObj.kvStorageEnabled) && (
              <>
                <h5 className="text-sm font-semibold text-gray-800 pt-2">Storage</h5>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">Stored Data (GB)</label>
                    <input type="number" value={doObj.storageThreshold}
                      onChange={(e) => setDO('storageThreshold', e.target.value)}
                      className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent" placeholder="e.g., 1" min="0" step="0.1" />
                    <p className="text-xs text-gray-500 mt-1">Total stored data (all backends)</p>
                  </div>
                </div>
              </>
            )}
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
                        Fetching data...
                      </p>
                    </div>
                  ) : availableZones.length === 0 ? (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                      <p className="text-sm text-yellow-800">
                        Please save and fetch data in Step 1 first to select zones for Bot Management.
                      </p>
                    </div>
                  ) : (
                    <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
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
                      <p className="text-sm text-blue-800">Fetching data...</p>
                    </div>
                  ) : availableZones.length === 0 ? (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                      <p className="text-sm text-yellow-800">
                        Please save and fetch data in Step 1 first.
                      </p>
                    </div>
                  ) : (
                    <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
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
                      <p className="text-sm text-blue-800">Fetching data...</p>
                    </div>
                  ) : availableZones.length === 0 ? (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                      <p className="text-sm text-yellow-800">
                        Please save and fetch data in Step 1 first.
                      </p>
                    </div>
                  ) : (
                    <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
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
                      <p className="text-sm text-blue-800">Fetching data...</p>
                    </div>
                  ) : availableZones.length === 0 ? (
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
                      <p className="text-sm text-yellow-800">
                        Please save and fetch data in Step 1 first.
                      </p>
                    </div>
                  ) : (
                    <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
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
                  <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = formData.networkServices.magicTransit.accountIds.includes(accountId);
                      return (
                        <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
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
                  <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = ztServices.seats.accountIds.includes(accountId);
                      return (
                        <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
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
                  <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = netServices.magicWan.accountIds.includes(accountId);
                      return (
                        <label key={accountId} className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0">
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
                  <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId, index) => {
                      const isSelected = ztServices.seats.accountIds.includes(accountId);
                      
                      return (
                        <label 
                          key={accountId} 
                          className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0"
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
                  <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = service.accountIds.includes(accountId);
                      
                      return (
                        <label 
                          key={accountId} 
                          className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0"
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
                  <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = devServices.workersPages.accountIds.includes(accountId);
                      
                      return (
                        <label 
                          key={accountId} 
                          className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0"
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
                  <div className="bg-white border border-gray-300 rounded-lg max-h-60 overflow-y-auto">
                    {formData.accountIds.filter(id => id.trim()).map((accountId) => {
                      const isSelected = devServices.r2Storage.accountIds.includes(accountId);
                      
                      return (
                        <label 
                          key={accountId} 
                          className="flex items-center space-x-3 px-4 py-3 hover:bg-gray-50 cursor-pointer border-b border-gray-200 last:border-b-0"
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
          {configStep === 1 && 'Configure your Cloudflare accounts'}
          {configStep === 2 && 'Configure notification settings'}
          {configStep === 3 && 'Set contracted thresholds for each service'}
        </p>
      </div>

      {/* Step Indicator */}
      <div className="bg-gray-50 px-6 py-3 border-b border-gray-200">
        <div className="flex items-center justify-center space-x-4">
          <button type="button" onClick={() => zonesLoaded && setConfigStep(1)} className={`flex items-center space-x-2 ${configStep === 1 ? 'text-blue-600 font-semibold' : 'text-gray-500 hover:text-gray-700'}`}>
            <div className={`w-8 h-8 rounded-full flex items-center justify-center ${configStep === 1 ? 'bg-blue-600 text-white' : configStep > 1 ? 'bg-green-500 text-white' : 'bg-gray-300 text-gray-600'}`}>
              {configStep > 1 ? <CheckCircle className="w-4 h-4" /> : '1'}
            </div>
            <span className="hidden sm:inline">Accounts</span>
          </button>
          <div className="w-12 h-0.5 bg-gray-300"></div>
          <button type="button" onClick={() => zonesLoaded && setConfigStep(2)} className={`flex items-center space-x-2 ${configStep === 2 ? 'text-blue-600 font-semibold' : 'text-gray-500 hover:text-gray-700'}`}>
            <div className={`w-8 h-8 rounded-full flex items-center justify-center ${configStep === 2 ? 'bg-blue-600 text-white' : configStep > 2 ? 'bg-green-500 text-white' : 'bg-gray-300 text-gray-600'}`}>
              {configStep > 2 ? <CheckCircle className="w-4 h-4" /> : '2'}
            </div>
            <span className="hidden sm:inline">Notifications</span>
          </button>
          <div className="w-12 h-0.5 bg-gray-300"></div>
          <button type="button" onClick={() => zonesLoaded && setConfigStep(3)} className={`flex items-center space-x-2 ${configStep === 3 ? 'text-blue-600 font-semibold' : 'text-gray-500'}`}>
            <div className={`w-8 h-8 rounded-full flex items-center justify-center ${configStep === 3 ? 'bg-blue-600 text-white' : 'bg-gray-300 text-gray-600'}`}>
              3
            </div>
            <span className="hidden sm:inline">Service Thresholds</span>
          </button>
        </div>
      </div>

      {/* Content */}
      {configStep === 1 && renderAccountIdsStep()}
      {configStep === 2 && renderNotificationsStep()}
      {configStep === 3 && renderServiceThresholdsStep()}
    </form>
  );
}

export default ConfigFormNew;

/**
 * Cloudflare Worker for Enterprise Usage Dashboard
 * Handles API requests and serves static React assets
 */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    
    // API routes
    if (url.pathname.startsWith('/api/')) {
      return handleApiRequest(request, env, url, ctx);
    }
    
    // Serve static assets
    return env.ASSETS.fetch(request);
  },

  async scheduled(event, env, ctx) {
    const cron = event.cron;
    if (cron === '* * * * *') {
      ctx.waitUntil(pollSpectrumConcurrent(env));
    } else {
      ctx.waitUntil(Promise.all([
        runScheduledThresholdCheck(env),
        preWarmCache(env)
      ]));
    }
  },
};

/**
 * Handle API requests
 */
async function handleApiRequest(request, env, url, ctx) {
  // CORS headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  };

  // Handle preflight requests
  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Route API requests
    if (url.pathname === '/api/metrics' && request.method === 'POST') {
      return await getMetrics(request, env, corsHeaders);
    }
    
    if (url.pathname === '/api/metrics/progressive' && request.method === 'POST') {
      return await getMetricsProgressive(request, env, corsHeaders);
    }
    
    if (url.pathname === '/api/cache/status' && request.method === 'POST') {
      return await getCacheStatus(request, env, corsHeaders);
    }
    
    if (url.pathname === '/api/cache/warm' && request.method === 'POST') {
      // Manually trigger cache pre-warming (for testing)
      ctx.waitUntil(preWarmCache(env));
      return new Response(JSON.stringify({ message: 'Cache warming triggered! Check logs with: npx wrangler tail' }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }
    
    if (url.pathname === '/api/zones' && request.method === 'POST') {
      return await getZones(request, env, corsHeaders);
    }
    
    if (url.pathname === '/api/config' && request.method === 'GET') {
      return await getConfig(request, env, corsHeaders);
    }
    
    if (url.pathname === '/api/config' && request.method === 'POST') {
      return await saveConfig(request, env, corsHeaders);
    }
    
    if (url.pathname === '/api/webhook/check' && request.method === 'POST') {
      return await checkThresholds(request, env, corsHeaders);
    }
    
    if (url.pathname === '/api/firewall/test' && request.method === 'POST') {
      return await testFirewallQuery(request, env, corsHeaders);
    }

    
    if (url.pathname === '/api/cache/prewarm' && request.method === 'POST') {
      return await triggerPrewarm(request, env, corsHeaders);
    }

    return new Response(JSON.stringify({ error: 'Not found' }), {
      status: 404,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('API Error:', error);
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
}

/**
 * Parse and normalize account IDs from request
 * Handles both old single accountId and new accountIds array
 * Account IDs always come from KV/UI (no env var support)
 */
function parseAccountIds(body) {
  // New format: accountIds array (from body/KV)
  if (body.accountIds && Array.isArray(body.accountIds) && body.accountIds.length > 0) {
    return body.accountIds.filter(id => id && id.trim());
  }
  
  // Legacy format: single accountId from body/KV
  if (body.accountId) {
    return [body.accountId];
  }
  
  return [];
}

/**
 * Fetch metrics from Cloudflare GraphQL API
 * Now supports multiple accounts - aggregates metrics across all accounts
 */
async function getMetrics(request, env, corsHeaders) {
  const body = await request.json();
  
  // API Token: Read from wrangler secret (secure storage)
  const apiKey = env.CLOUDFLARE_API_TOKEN;
  // Account IDs: From KV/UI (supports multi-account)
  const accountIds = parseAccountIds(body);

  if (!apiKey) {
    return new Response(JSON.stringify({ error: 'API token not configured. Set it using: npx wrangler secret put CLOUDFLARE_API_TOKEN' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  if (accountIds.length === 0) {
    return new Response(JSON.stringify({ error: 'Account IDs not configured. Please configure them in Settings.' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  // Fetch metrics for each account in parallel
  const accountMetricsPromises = accountIds.map(accountId => 
    fetchAccountMetrics(apiKey, accountId, env)
  );
  
  const accountMetricsResults = await Promise.allSettled(accountMetricsPromises);
  
  // Filter successful results
  const successfulMetrics = accountMetricsResults
    .filter(result => result.status === 'fulfilled')
    .map(result => result.value);
  
  if (successfulMetrics.length === 0) {
    return new Response(JSON.stringify({ error: 'Failed to fetch metrics from any account' }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  // Aggregate metrics across all accounts
  const aggregated = aggregateAccountMetrics(successfulMetrics);
  
  return new Response(
    JSON.stringify(aggregated),
    {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    }
  );
}

/**
 * Progressive Loading: Return metrics in phases for faster perceived performance
 * Phase 1 (<2s): Core metrics + zone count
 * Phase 2 (3-5s): Zone breakdown
 * Phase 3 (full): Historical time series
 */
async function getMetricsProgressive(request, env, corsHeaders) {
  const body = await request.json();
  const phase = body.phase || 1; // Which phase to return
  
  const apiKey = env.CLOUDFLARE_API_TOKEN;
  const accountIds = parseAccountIds(body);

  if (!apiKey) {
    return new Response(JSON.stringify({ error: 'API token not configured' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  if (accountIds.length === 0) {
    return new Response(JSON.stringify({ error: 'Account IDs not configured' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  try {
    // Check if we have fully cached data (from cron pre-warming)
    const cacheKey = `pre-warmed:${accountIds.join(',')}`;
    const cachedData = await env.CONFIG_KV.get(cacheKey, 'json');
    
    if (cachedData && cachedData.data) {
      
      // Check if cache is complete (has all ENABLED metrics)
      const configData = await env.CONFIG_KV.get('config:default');
      let cacheIsComplete = true;
      
      if (configData) {
        const config = JSON.parse(configData);
        
        // Check App Services Core
        if (config?.applicationServices?.core?.enabled && !cachedData.data.current) {
          cacheIsComplete = false;
        }
        
        // Check Bot Management
        if (config?.applicationServices?.botManagement?.enabled) {
          if (!cachedData.data.botManagement) {
            cacheIsComplete = false;
          } else if (!cachedData.data.botManagement.timeSeries) {
            cacheIsComplete = false;
          } else if (!cachedData.data.botManagement.perAccountData) {
            cacheIsComplete = false;
          }
        }
        
        // Check API Shield
        if (config?.applicationServices?.apiShield?.enabled) {
          if (!cachedData.data.apiShield) {
            cacheIsComplete = false;
          } else if (!cachedData.data.apiShield.timeSeries) {
            cacheIsComplete = false;
          } else if (!cachedData.data.apiShield.perAccountData) {
            cacheIsComplete = false;
          }
        }
        
        // Check Page Shield
        if (config?.applicationServices?.pageShield?.enabled) {
          if (!cachedData.data.pageShield) {
            cacheIsComplete = false;
          } else if (!cachedData.data.pageShield.timeSeries) {
            cacheIsComplete = false;
          } else if (!cachedData.data.pageShield.perAccountData) {
            cacheIsComplete = false;
          }
        }
        
        // Check Advanced Rate Limiting
        if (config?.applicationServices?.advancedRateLimiting?.enabled) {
          if (!cachedData.data.advancedRateLimiting) {
            cacheIsComplete = false;
          } else if (!cachedData.data.advancedRateLimiting.timeSeries) {
            cacheIsComplete = false;
          } else if (!cachedData.data.advancedRateLimiting.perAccountData) {
            cacheIsComplete = false;
          }
        }

        // Check Argo Smart Routing
        if (config?.applicationServices?.argo?.enabled) {
          if (!cachedData.data.argo) {
            cacheIsComplete = false;
          } else if (!cachedData.data.argo.timeSeries) {
            cacheIsComplete = false;
          } else if (!cachedData.data.argo.perAccountData) {
            cacheIsComplete = false;
          }
        }

        // Check Cache Reserve
        if (config?.applicationServices?.cacheReserve?.enabled) {
          if (!cachedData.data.cacheReserve) {
            cacheIsComplete = false;
          } else if (!cachedData.data.cacheReserve.timeSeries) {
            cacheIsComplete = false;
          } else if (!cachedData.data.cacheReserve.perZoneData) {
            cacheIsComplete = false;
          }
        }

        // Future: Check other SKUs
        // if (config?.zeroTrust?.access?.enabled && !cachedData.data.zeroTrustAccess) {
        //   cacheIsComplete = false;
        // }
      }
      
      // Only use cache if it's complete
      if (cacheIsComplete) {
        return new Response(
          JSON.stringify({ 
            ...cachedData.data,
            phase: 'cached',
            cacheAge: Date.now() - cachedData.timestamp 
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      } else {
        console.log('🔄 Cache incomplete - falling through to fresh fetch');
        // Don't return - fall through to fetch fresh data below
      }
    }

    console.log(`Cache MISS or incomplete: Fetching phase ${phase} data`);

    // Phase 1: Core metrics + zone count (FAST - 1-2s)
    if (phase === 1) {
      const phase1Data = await fetchPhase1Data(apiKey, accountIds, env);
      return new Response(
        JSON.stringify({ ...phase1Data, phase: 1 }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Phase 2: Add zone breakdown (MEDIUM - 3-5s)
    if (phase === 2) {
      const phase2Data = await fetchPhase2Data(apiKey, accountIds, env);
      return new Response(
        JSON.stringify({ ...phase2Data, phase: 2 }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Phase 3: Full data including historical
    const configData = await env.CONFIG_KV.get('config:default');
    const config = configData ? JSON.parse(configData) : {};
    const data = await fetchAllMetrics(apiKey, accountIds, config, env);
    return new Response(
      JSON.stringify({ ...data, phase: 3 }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error('Progressive metrics error:', error);
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
}

/**
 * Phase 1: Fast core metrics (1-2s)
 * Returns: Current month totals + zone count
 */
async function fetchPhase1Data(apiKey, accountIds, env) {
  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  
  // Fetch zone counts in parallel
  const zonesPromises = accountIds.map(accountId => fetchEnterpriseZones(apiKey, accountId));
  const zonesResults = await Promise.allSettled(zonesPromises);
  const allZones = zonesResults
    .filter(result => result.status === 'fulfilled')
    .flatMap(result => result.value || []);
  
  const totalZones = allZones.length;
  
  // For Phase 1, return cached current month data if available
  // Otherwise return estimated/placeholder data
  const phase1Metrics = {
    current: {
      requests: 0,
      bytes: 0,
      dnsQueries: 0,
    },
    zonesCount: totalZones,
    loading: true, // Indicates more data is being fetched
  };
  
  return phase1Metrics;
}

/**
 * Phase 2: Add zone breakdown (3-5s)
 * Returns: Phase 1 + zone breakdown + current month details
 */
async function fetchPhase2Data(apiKey, accountIds, env) {
  // Fetch current month metrics for all accounts (without historical data)
  const accountMetricsPromises = accountIds.map(async (accountId) => {
    const metrics = await fetchAccountMetrics(apiKey, accountId, env);
    // Strip historical data to make it faster
    return {
      ...metrics,
      timeSeries: [], // Exclude historical for Phase 2
    };
  });
  
  const accountMetricsResults = await Promise.allSettled(accountMetricsPromises);
  const successfulMetrics = accountMetricsResults
    .filter(result => result.status === 'fulfilled')
    .map(result => result.value);
  
  const aggregated = aggregateAccountMetrics(successfulMetrics);
  
  return {
    ...aggregated,
    loading: true, // Still loading historical data
  };
}

function mergeTS(entries, fields) {
  const map = new Map();
  entries.forEach(ts => {
    const ex = map.get(ts.month) || { month: ts.month, timestamp: ts.timestamp, ...Object.fromEntries(fields.map(f => [f, 0])) };
    fields.forEach(f => { ex[f] = (ex[f] || 0) + (ts[f] || 0); });
    map.set(ts.month, ex);
  });
  return Array.from(map.values()).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
}

async function fetchAllMetrics(apiKey, accountIds, config, env) {
  const timings = {};
  const overallStart = Date.now();
  let coreMetrics = null;
  let successfulMetrics = [];
  let zonesCount = 0;
  let zonesData = null;

  const coreEnabled = config?.applicationServices?.core?.enabled !== false;
  const trafficEnabled = config?.applicationServices?.core?.trafficEnabled !== undefined ? config.applicationServices.core.trafficEnabled : coreEnabled;
  const dnsEnabled = config?.applicationServices?.core?.dnsEnabled !== undefined ? config.applicationServices.core.dnsEnabled : coreEnabled;
  const anyCoreEnabled = coreEnabled || trafficEnabled || dnsEnabled;

  const independentPromises = [];

  if (config?.applicationServices?.botManagement?.enabled && accountIds.length > 0) {
    const c = config.applicationServices.botManagement;
    independentPromises.push(Promise.allSettled(accountIds.map(id => fetchBotManagementForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'botManagement', cfg: c, results })));
  }
  const ztIds = config?.zeroTrust?.seats?.accountIds || []; if (config?.zeroTrust?.seats?.enabled && ztIds.length > 0) { const c = config.zeroTrust.seats; independentPromises.push(Promise.allSettled(ztIds.map(id => fetchZeroTrustSeatsForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'zeroTrustSeats', cfg: c, results }))); }
  const wpIds = config?.developerServices?.workersPages?.accountIds || []; if (config?.developerServices?.workersPages?.enabled && wpIds.length > 0) { const c = config.developerServices.workersPages; independentPromises.push(Promise.allSettled(wpIds.map(id => fetchWorkersPagesForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'workersPages', cfg: c, results }))); }
  const r2Ids = config?.developerServices?.r2Storage?.accountIds || []; if (config?.developerServices?.r2Storage?.enabled && r2Ids.length > 0) { const c = config.developerServices.r2Storage; independentPromises.push(Promise.allSettled(r2Ids.map(id => fetchR2StorageForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'r2Storage', cfg: c, results }))); }
  const d1Ids = config?.developerServices?.d1?.accountIds || []; if (config?.developerServices?.d1?.enabled && d1Ids.length > 0) { const c = config.developerServices.d1; independentPromises.push(Promise.allSettled(d1Ids.map(id => fetchD1ForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'd1', cfg: c, results }))); }
  const kvIds = config?.developerServices?.kv?.accountIds || []; if (config?.developerServices?.kv?.enabled && kvIds.length > 0) { const c = config.developerServices.kv; independentPromises.push(Promise.allSettled(kvIds.map(id => fetchKVForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'kv', cfg: c, results }))); }
  const streamIds = config?.developerServices?.stream?.accountIds || []; if (config?.developerServices?.stream?.enabled && streamIds.length > 0) { const c = config.developerServices.stream; independentPromises.push(Promise.allSettled(streamIds.map(id => fetchStreamForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'stream', cfg: c, results }))); }
  const imgIds = config?.developerServices?.images?.accountIds || []; if (config?.developerServices?.images?.enabled && imgIds.length > 0) { const c = config.developerServices.images; independentPromises.push(Promise.allSettled(imgIds.map(id => fetchImagesForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'images', cfg: c, results }))); }
  const waiIds = config?.developerServices?.workersAI?.accountIds || []; if (config?.developerServices?.workersAI?.enabled && waiIds.length > 0) { const c = config.developerServices.workersAI; independentPromises.push(Promise.allSettled(waiIds.map(id => fetchWorkersAIForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'workersAI', cfg: c, results }))); }
  const qIds = config?.developerServices?.queues?.accountIds || []; if (config?.developerServices?.queues?.enabled && qIds.length > 0) { const c = config.developerServices.queues; independentPromises.push(Promise.allSettled(qIds.map(id => fetchQueuesForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'queues', cfg: c, results }))); }
  const wltIds = config?.developerServices?.workersLogsTraces?.accountIds || []; if (config?.developerServices?.workersLogsTraces?.enabled && wltIds.length > 0) { const c = config.developerServices.workersLogsTraces; independentPromises.push(Promise.allSettled(wltIds.map(id => fetchWorkersLogsTracesForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'workersLogsTraces', cfg: c, results }))); }
  const specZones = config?.networkServices?.spectrum?.zones || []; if (config?.networkServices?.spectrum?.enabled && specZones.length > 0) { const c = config.networkServices.spectrum; independentPromises.push(Promise.allSettled(specZones.map(zoneId => fetchSpectrumForZone(apiKey, zoneId, c, env).then(data => ({ zoneId, data })))).then(results => ({ key: 'spectrum', cfg: c, results }))); }
  const lbIds = config?.applicationServices?.loadBalancing?.accountIds || []; if (config?.applicationServices?.loadBalancing?.enabled && lbIds.length > 0) { const c = config.applicationServices.loadBalancing; independentPromises.push(Promise.allSettled(lbIds.map(id => fetchLoadBalancingForAccount(apiKey, id, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'loadBalancing', cfg: c, results }))); }
  const chIds = config?.applicationServices?.customHostnames?.accountIds || []; if (config?.applicationServices?.customHostnames?.enabled && chIds.length > 0) { const c = config.applicationServices.customHostnames; independentPromises.push(Promise.allSettled(chIds.map(id => fetchCustomHostnamesForAccount(apiKey, id, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'customHostnames', cfg: c, results }))); }
  const leIds = config?.applicationServices?.logExplorer?.accountIds || []; if (config?.applicationServices?.logExplorer?.enabled && leIds.length > 0) { const c = config.applicationServices.logExplorer; independentPromises.push(Promise.allSettled(leIds.map(id => fetchLogExplorerForAccount(apiKey, id, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'logExplorer', cfg: c, results }))); }
  const doIds = config?.developerServices?.durableObjects?.accountIds || []; if (config?.developerServices?.durableObjects?.enabled && doIds.length > 0) { const c = config.developerServices.durableObjects; independentPromises.push(Promise.allSettled(doIds.map(id => fetchDurableObjectsForAccount(apiKey, id, c, env).then(data => ({ accountId: id, data })))).then(results => ({ key: 'durableObjects', cfg: c, results }))); }
  const mtIds = config?.networkServices?.magicTransit?.accountIds || []; if (config?.networkServices?.magicTransit?.enabled && mtIds.length > 0) { const c = config.networkServices.magicTransit; independentPromises.push(Promise.allSettled(mtIds.map(id => fetchMagicBandwidthForAccount(apiKey, id, c, env, 'magicTransit').then(data => ({ accountId: id, data })))).then(results => ({ key: 'magicTransit', cfg: c, results }))); }
  const mwIds = config?.networkServices?.magicWan?.accountIds || []; if (config?.networkServices?.magicWan?.enabled && mwIds.length > 0) { const c = config.networkServices.magicWan; independentPromises.push(Promise.allSettled(mwIds.map(id => fetchMagicBandwidthForAccount(apiKey, id, c, env, 'magicWan').then(data => ({ accountId: id, data })))).then(results => ({ key: 'magicWan', cfg: c, results }))); }

  console.log(`🚀 [Independent] Started ${independentPromises.length} independent product fetches`);

  const corePromise = (async () => {
    if (!anyCoreEnabled) {
      console.log('⏭️ App Services Core disabled - skipping');
      return { successfulMetrics: [], coreMetrics: null, zonesData: null, zonesCount: 0 };
    }
    const coreStart = Date.now();
    console.log('📊 [Core] Fetching App Services Core metrics + zones...');
    const accountFetches = await Promise.allSettled(
      accountIds.map(async accountId => {
        const [metrics, accountName, zones] = await Promise.all([
          fetchAccountMetrics(apiKey, accountId, env),
          fetchAccountName(apiKey, accountId),
          fetchEnterpriseZones(apiKey, accountId),
        ]);
        return { accountId, metrics, accountName, zones: zones || [] };
      })
    );
    const successfulFetches = accountFetches.filter(r => r.status === 'fulfilled').map(r => r.value);
    const sm = successfulFetches.map(f => f.metrics).filter(Boolean);
    let cm = null;
    if (sm.length > 0) {
      cm = aggregateAccountMetrics(sm);
    } else {
      console.warn('⚠️ Failed to fetch core metrics from any account');
    }
    const accountNames = {};
    const allZones = [];
    successfulFetches.forEach(({ accountId, accountName, zones }) => {
      accountNames[accountId] = accountName || accountId;
      zones.forEach(z => allZones.push({ ...z, account: { id: accountId, name: accountNames[accountId] } }));
    });
    const zc = allZones.length;
    const zoneMonthKey = `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`;
    try {
      await env.CONFIG_KV.put(`monthly-zone-count:${zoneMonthKey}`, JSON.stringify({ count: zc, timestamp: new Date().toISOString() }), { expirationTtl: 31536000 });
    } catch (e) { console.error('Failed to store zone count snapshot:', e); }
    const zonesTimeSeries = await getHistoricalZoneCountData(env, zc);
    const zd = { zones: allZones.map(z => ({ id: z.id, name: z.name, account: z.account })), accounts: accountNames, enterprise: zc, zonesTimeSeries };
    timings.core = Date.now() - coreStart;
    console.log(`⏱️ Core: ${timings.core}ms`);
    return { successfulMetrics: sm, coreMetrics: cm, zonesData: zd, zonesCount: zc };
  })();

  const coreResult = await corePromise;
  successfulMetrics = coreResult.successfulMetrics;
  coreMetrics = coreResult.coreMetrics;
  zonesData = coreResult.zonesData;
  zonesCount = coreResult.zonesCount;

  const dependentPromises = [];
  if (successfulMetrics.length > 0) {
    if (config?.applicationServices?.apiShield?.enabled) { const c = config.applicationServices.apiShield; dependentPromises.push(Promise.allSettled(successfulMetrics.map(a => calculateZoneBasedAddonForAccount(a, c, env, 'api-shield').then(d => ({ accountId: a.accountId, data: d })))).then(results => ({ key: 'apiShield', cfg: c, results }))); }
    if (config?.applicationServices?.pageShield?.enabled) { const c = config.applicationServices.pageShield; dependentPromises.push(Promise.allSettled(successfulMetrics.map(a => calculateZoneBasedAddonForAccount(a, c, env, 'page-shield').then(d => ({ accountId: a.accountId, data: d })))).then(results => ({ key: 'pageShield', cfg: c, results }))); }
    if (config?.applicationServices?.advancedRateLimiting?.enabled) { const c = config.applicationServices.advancedRateLimiting; dependentPromises.push(Promise.allSettled(successfulMetrics.map(a => calculateZoneBasedAddonForAccount(a, c, env, 'advanced-rate-limiting').then(d => ({ accountId: a.accountId, data: d })))).then(results => ({ key: 'advancedRateLimiting', cfg: c, results }))); }
    if (config?.applicationServices?.argo?.enabled) { const c = config.applicationServices.argo; dependentPromises.push(Promise.allSettled(successfulMetrics.map(a => calculateArgoForAccount(a, c, env).then(d => ({ accountId: a.accountId, data: d })))).then(results => ({ key: 'argo', cfg: c, results }))); }
  }
  const crZones = config?.applicationServices?.cacheReserve?.zones || [];
  if (config?.applicationServices?.cacheReserve?.enabled && crZones.length > 0) {
    const c = config.applicationServices.cacheReserve;
    const znMap = {};
    successfulMetrics.forEach(a => (a.zoneBreakdown?.zones || []).forEach(z => { if (z.zoneTag && z.zoneName) znMap[z.zoneTag] = z.zoneName; }));
    dependentPromises.push(Promise.allSettled(crZones.map(zoneId => fetchCacheReserveForZone(apiKey, zoneId, znMap[zoneId] || zoneId, env))).then(results => ({ key: 'cacheReserve', cfg: c, results })));
  }

  console.log(`🔗 [Dependent] Started ${dependentPromises.length} dependent product fetches`);
  const [independentResults, dependentResults] = await Promise.all([
    Promise.allSettled(independentPromises),
    Promise.allSettled(dependentPromises),
  ]);
  const wave2Results = [...independentResults, ...dependentResults];
  timings.total = Date.now() - overallStart;
  console.log(`⏱️ Total: ${timings.total}ms (independent: ${independentPromises.length}, dependent: ${dependentPromises.length})`);
  let botManagementData = null, apiShieldData = null, pageShieldData = null;
  let advancedRateLimitingData = null, argoData = null, zeroTrustSeatsData = null;
  let workersPagesData = null, r2StorageData = null, d1Data = null, kvData = null;
  let streamData = null, imagesData = null, workersAIData = null, queuesData = null;
  let workersLogsTracesData = null, spectrumData = null, cacheReserveData = null;
  let loadBalancingData = null, customHostnamesData = null, logExplorerData = null;
  let durableObjectsData = null, magicTransitData = null, magicWanData = null;

  for (const settled of wave2Results) {
    if (settled.status !== 'fulfilled') continue;
    const { key, cfg, results } = settled.value;
    const ok = results.filter(r => r.status === 'fulfilled' && r.value?.data).map(r => r.value);
    if (key !== 'cacheReserve' && key !== 'spectrum' && ok.length === 0) continue;
    const pad = (e, f) => e.data.current[f] || 0;
    const padP = (e, f) => e.data.previous[f] || 0;
    const sumC = (f) => ok.reduce((s, e) => s + pad(e, f), 0);
    const sumP = (f) => ok.reduce((s, e) => s + padP(e, f), 0);
    const ts = (fields) => mergeTS(ok.flatMap(e => e.data.timeSeries || []), fields);
    const pad2 = (e) => ({ accountId: e.accountId, current: e.data.current, previous: e.data.previous, timeSeries: e.data.timeSeries });
    if (key === 'botManagement') {
      botManagementData = { enabled: true, threshold: cfg.threshold, current: { likelyHuman: sumC('likelyHuman'), botTraffic: sumC('botTraffic'), totalTraffic: sumC('totalTraffic'), zones: ok.flatMap(e => e.data.current.zones || []), confidence: ok.find(e => e.data.current?.confidence)?.data.current.confidence || null }, previous: { likelyHuman: sumP('likelyHuman'), zones: ok.flatMap(e => e.data.previous.zones || []) }, timeSeries: ts(['likelyHuman']), perAccountData: ok.map(pad2) };
    } else if (key === 'apiShield' || key === 'pageShield' || key === 'advancedRateLimiting') {
      const out = { enabled: true, threshold: cfg.threshold, current: { requests: sumC('requests'), zones: ok.flatMap(e => e.data.current.zones), confidence: ok.find(e => e.data.current?.confidence)?.data.current.confidence || null }, previous: { requests: sumP('requests'), zones: ok.flatMap(e => e.data.previous.zones) }, timeSeries: ts(['requests']), perAccountData: ok.map(pad2) };
      if (key === 'apiShield') apiShieldData = out; else if (key === 'pageShield') pageShieldData = out; else advancedRateLimitingData = out;
    } else if (key === 'argo') {
      argoData = { enabled: true, threshold: cfg.threshold, current: { bytes: sumC('bytes'), zones: ok.flatMap(e => e.data.current.zones), confidence: ok.find(e => e.data.current?.confidence)?.data.current.confidence || null }, previous: { bytes: sumP('bytes'), zones: ok.flatMap(e => e.data.previous.zones) }, timeSeries: ts(['bytes']), perAccountData: ok.map(pad2) };
    } else if (key === 'zeroTrustSeats') {
      zeroTrustSeatsData = { enabled: true, threshold: cfg.threshold, current: { seats: sumC('seats') }, previous: { seats: sumP('seats') }, timeSeries: ts(['seats']), perAccountData: ok.map(pad2) };
    } else if (key === 'workersPages') {
      const ca = ok.reduce((a, e) => { const c = e.data.current.confidence; if (c) { a.estimate += c.estimate||0; a.lower += c.lower||0; a.upper += c.upper||0; a.sampleSize += c.sampleSize||0; a.hasData = true; } return a; }, { estimate:0, lower:0, upper:0, sampleSize:0, hasData:false });
      let wpc = null;
      if (ca.hasData && ca.estimate > 0) { const rw = (ca.upper-ca.lower)/(2*ca.estimate); wpc = { percent: Math.round(Math.max(0,Math.min(100,100*(1-rw)))*10)/10, sampleSize: ca.sampleSize, estimate: ca.estimate, lower: ca.lower, upper: ca.upper }; }
      workersPagesData = { enabled: true, requestsThreshold: cfg.requestsThreshold, cpuTimeThreshold: cfg.cpuTimeThreshold, current: { requests: sumC('requests'), cpuTimeMs: sumC('cpuTimeMs'), confidence: wpc }, previous: { requests: sumP('requests'), cpuTimeMs: sumP('cpuTimeMs') }, timeSeries: ts(['requests','cpuTimeMs']), perAccountData: ok.map(pad2) };
    } else if (key === 'r2Storage') {
      r2StorageData = { enabled: true, classAOpsThreshold: cfg.classAOpsThreshold, classBOpsThreshold: cfg.classBOpsThreshold, storageThreshold: cfg.storageThreshold, current: { classAOps: ok.reduce((s,e)=>s+(e.data.current?.classAOps||0),0), classBOps: ok.reduce((s,e)=>s+(e.data.current?.classBOps||0),0), storageGB: ok.reduce((s,e)=>s+(e.data.current?.storageGB||0),0) }, previous: { classAOps: ok.reduce((s,e)=>s+(e.data.previous?.classAOps||0),0), classBOps: ok.reduce((s,e)=>s+(e.data.previous?.classBOps||0),0), storageGB: ok.reduce((s,e)=>s+(e.data.previous?.storageGB||0),0) }, timeSeries: ts(['classAOps','classBOps','storageGB']), perAccountData: ok.map(pad2) };
    } else if (key === 'd1') {
      d1Data = { enabled: true, rowsReadThreshold: cfg.rowsReadThreshold, rowsWrittenThreshold: cfg.rowsWrittenThreshold, storageThreshold: cfg.storageThreshold, current: { rowsRead: ok.reduce((s,e)=>s+(e.data.current?.rowsRead||0),0), rowsWritten: ok.reduce((s,e)=>s+(e.data.current?.rowsWritten||0),0), storageMB: ok.reduce((s,e)=>s+(e.data.current?.storageMB||0),0) }, previous: { rowsRead: ok.reduce((s,e)=>s+(e.data.previous?.rowsRead||0),0), rowsWritten: ok.reduce((s,e)=>s+(e.data.previous?.rowsWritten||0),0), storageMB: ok.reduce((s,e)=>s+(e.data.previous?.storageMB||0),0) }, timeSeries: ts(['rowsRead','rowsWritten','storageMB']), perAccountData: ok.map(pad2) };
    } else if (key === 'kv') {
      kvData = { enabled: true, readsThreshold: cfg.readsThreshold, writesThreshold: cfg.writesThreshold, deletesThreshold: cfg.deletesThreshold, listsThreshold: cfg.listsThreshold, storageThreshold: cfg.storageThreshold, current: { reads: sumC('reads'), writes: sumC('writes'), deletes: sumC('deletes'), lists: sumC('lists'), storageMB: sumC('storageMB') }, previous: { reads: sumP('reads'), writes: sumP('writes'), deletes: sumP('deletes'), lists: sumP('lists'), storageMB: sumP('storageMB') }, timeSeries: ts(['reads','writes','deletes','lists','storageMB']), perAccountData: ok.map(pad2) };
    } else if (key === 'stream') {
      streamData = { enabled: true, minutesStoredThreshold: cfg.minutesStoredThreshold, minutesDeliveredThreshold: cfg.minutesDeliveredThreshold, current: { minutesStored: sumC('minutesStored'), minutesDelivered: sumC('minutesDelivered') }, previous: { minutesStored: sumP('minutesStored'), minutesDelivered: sumP('minutesDelivered') }, timeSeries: ts(['minutesStored','minutesDelivered']), perAccountData: ok.map(pad2) };
    } else if (key === 'images') {
      imagesData = { enabled: true, imagesStoredThreshold: cfg.imagesStoredThreshold, imagesDeliveredThreshold: cfg.imagesDeliveredThreshold, current: { imagesStored: sumC('imagesStored'), imagesDelivered: sumC('imagesDelivered') }, previous: { imagesStored: sumP('imagesStored'), imagesDelivered: sumP('imagesDelivered') }, timeSeries: ts(['imagesStored','imagesDelivered']), perAccountData: ok.map(pad2) };
    } else if (key === 'workersAI') {
      workersAIData = { enabled: true, neuronsThreshold: cfg.neuronsThreshold, current: { neurons: sumC('neurons') }, previous: { neurons: sumP('neurons') }, timeSeries: ts(['neurons']), perAccountData: ok.map(pad2) };
    } else if (key === 'queues') {
      queuesData = { enabled: true, operationsThreshold: cfg.operationsThreshold, current: { operations: sumC('operations') }, previous: { operations: sumP('operations') }, timeSeries: ts(['operations']), perAccountData: ok.map(pad2) };
    } else if (key === 'workersLogsTraces') {
      workersLogsTracesData = { enabled: true, eventsThreshold: cfg.eventsThreshold, current: { events: sumC('events') }, previous: { events: sumP('events') }, timeSeries: ts(['events']), perAccountData: ok.map(pad2) };
    } else if (key === 'spectrum') {
      const sp = results.filter(r => r.status === 'fulfilled' && r.value?.data).map(r => r.value);
      if (sp.length > 0) spectrumData = { enabled: true, dataTransferThreshold: cfg.dataTransferThreshold, connectionsThreshold: cfg.connectionsThreshold, current: { dataTransfer: sp.reduce((s,e)=>s+(e.data.current.dataTransfer||0),0), p95Concurrent: Math.max(...sp.map(e=>e.data.current.p95Concurrent||0)) }, previous: { dataTransfer: sp.reduce((s,e)=>s+(e.data.previous.dataTransfer||0),0), p95Concurrent: Math.max(...sp.map(e=>e.data.previous.p95Concurrent||0)) }, timeSeries: mergeTS(sp.flatMap(e=>e.data.timeSeries||[]),['dataTransfer']), perZoneData: sp.map(e=>({ zoneId: e.zoneId, current: e.data.current, previous: e.data.previous, timeSeries: e.data.timeSeries })) };
    } else if (key === 'cacheReserve') {
      const cr = results.filter(r => r.status === 'fulfilled' && r.value).map(r => r.value);
      if (cr.length > 0) cacheReserveData = { enabled: true, storageThreshold: cfg.storageThreshold, classAOpsThreshold: cfg.classAOpsThreshold, classBOpsThreshold: cfg.classBOpsThreshold, current: { storageGBDays: cr.reduce((s,e)=>s+(e.current.storageGBDays||0),0), classAOps: cr.reduce((s,e)=>s+(e.current.classAOps||0),0), classBOps: cr.reduce((s,e)=>s+(e.current.classBOps||0),0), zones: cr.map(e=>({ zoneId:e.zoneId, zoneName:e.zoneName, storageGBDays:e.current.storageGBDays, classAOps:e.current.classAOps, classBOps:e.current.classBOps })) }, previous: { storageGBDays: cr.reduce((s,e)=>s+(e.previous.storageGBDays||0),0), classAOps: cr.reduce((s,e)=>s+(e.previous.classAOps||0),0), classBOps: cr.reduce((s,e)=>s+(e.previous.classBOps||0),0), zones: cr.map(e=>({ zoneId:e.zoneId, zoneName:e.zoneName, storageGBDays:e.previous.storageGBDays, classAOps:e.previous.classAOps, classBOps:e.previous.classBOps })) }, timeSeries: mergeTS(cr.flatMap(e=>e.timeSeries||[]),['storageGBDays','classAOps','classBOps']), perZoneData: cr.map(e=>({ zoneId:e.zoneId, zoneName:e.zoneName, current:e.current, previous:e.previous, timeSeries:e.timeSeries })) };
    } else if (key === 'loadBalancing') {
      loadBalancingData = { enabled: true, threshold: cfg.threshold, current: { endpoints: ok.reduce((s,e)=>s+e.data.current.endpoints,0) }, previous: { endpoints: ok.reduce((s,e)=>s+e.data.previous.endpoints,0) }, timeSeries: ts(['endpoints']), perAccountData: ok.map(pad2) };
    } else if (key === 'customHostnames') {
      customHostnamesData = { enabled: true, threshold: cfg.threshold, current: { hostnames: ok.reduce((s,e)=>s+e.data.current.hostnames,0) }, previous: { hostnames: ok.reduce((s,e)=>s+e.data.previous.hostnames,0) }, timeSeries: ts(['hostnames']), perAccountData: ok.map(pad2) };
    } else if (key === 'logExplorer') {
      logExplorerData = { enabled: true, threshold: cfg.threshold, current: { billableGB: ok.reduce((s,e)=>s+e.data.current.billableGB,0) }, previous: { billableGB: ok.reduce((s,e)=>s+e.data.previous.billableGB,0) }, timeSeries: ts(['billableGB']), perAccountData: ok.map(pad2) };
    } else if (key === 'durableObjects') {
      const sf = (f) => ok.reduce((s,e)=>s+(e.data.current[f]||0),0);
      const spf = (f) => ok.reduce((s,e)=>s+(e.data.previous[f]||0),0);
      durableObjectsData = { enabled: true, requestsThreshold: cfg.requestsThreshold, durationThreshold: cfg.durationThreshold, sqliteRowsReadThreshold: cfg.sqliteRowsReadThreshold, sqliteRowsWrittenThreshold: cfg.sqliteRowsWrittenThreshold, kvReadUnitsThreshold: cfg.kvReadUnitsThreshold, kvWriteUnitsThreshold: cfg.kvWriteUnitsThreshold, kvDeletesThreshold: cfg.kvDeletesThreshold, storageThreshold: cfg.storageThreshold, current: { requests:sf('requests'), durationGBs:sf('durationGBs'), sqliteRowsRead:sf('sqliteRowsRead'), sqliteRowsWritten:sf('sqliteRowsWritten'), kvReadUnits:sf('kvReadUnits'), kvWriteUnits:sf('kvWriteUnits'), kvDeletes:sf('kvDeletes'), storageMB:sf('storageMB') }, previous: { requests:spf('requests'), durationGBs:spf('durationGBs'), sqliteRowsRead:spf('sqliteRowsRead'), sqliteRowsWritten:spf('sqliteRowsWritten'), kvReadUnits:spf('kvReadUnits'), kvWriteUnits:spf('kvWriteUnits'), kvDeletes:spf('kvDeletes'), storageMB:spf('storageMB') }, timeSeries: ts(['requests','durationGBs','sqliteRowsRead','sqliteRowsWritten','kvReadUnits','kvWriteUnits','kvDeletes','storageMB']), perAccountData: ok.map(pad2) };
    } else if (key === 'magicTransit' || key === 'magicWan') {
      const sd = { enabled: true, threshold: cfg.threshold, current: { p95Mbps: ok.reduce((s,e)=>s+(e.data.current?.p95Mbps||0),0), ingressP95Mbps: ok.reduce((s,e)=>s+(e.data.current?.ingressP95Mbps||0),0), egressP95Mbps: ok.reduce((s,e)=>s+(e.data.current?.egressP95Mbps||0),0) }, previous: { p95Mbps: ok.reduce((s,e)=>s+(e.data.previous?.p95Mbps||0),0), ingressP95Mbps: ok.reduce((s,e)=>s+(e.data.previous?.ingressP95Mbps||0),0), egressP95Mbps: ok.reduce((s,e)=>s+(e.data.previous?.egressP95Mbps||0),0) }, timeSeries: ts(['p95Mbps','ingressP95Mbps','egressP95Mbps']), perAccountData: ok.map(pad2) };
      if (key === 'magicTransit') magicTransitData = sd; else magicWanData = sd;
    }
  }

  return {
    timings,
    ...(coreMetrics || {}),
    zonesCount,
    ...(zonesData && { zones: zonesData }),
    ...(botManagementData && { botManagement: botManagementData }),
    ...(apiShieldData && { apiShield: apiShieldData }),
    ...(pageShieldData && { pageShield: pageShieldData }),
    ...(advancedRateLimitingData && { advancedRateLimiting: advancedRateLimitingData }),
    ...(argoData && { argo: argoData }),
    ...(cacheReserveData && { cacheReserve: cacheReserveData }),
    ...(loadBalancingData && { loadBalancing: loadBalancingData }),
    ...(customHostnamesData && { customHostnames: customHostnamesData }),
    ...(logExplorerData && { logExplorer: logExplorerData }),
    ...(zeroTrustSeatsData && { zeroTrustSeats: zeroTrustSeatsData }),
    ...(workersPagesData && { workersPages: workersPagesData }),
    ...(r2StorageData && { r2Storage: r2StorageData }),
    ...(d1Data && { d1: d1Data }),
    ...(kvData && { kv: kvData }),
    ...(streamData && { stream: streamData }),
    ...(imagesData && { images: imagesData }),
    ...(workersAIData && { workersAI: workersAIData }),
    ...(queuesData && { queues: queuesData }),
    ...(workersLogsTracesData && { workersLogsTraces: workersLogsTracesData }),
    ...(spectrumData && { spectrum: spectrumData }),
    ...(durableObjectsData && { durableObjects: durableObjectsData }),
    ...(magicTransitData && { magicTransit: magicTransitData }),
    ...(magicWanData && { magicWan: magicWanData }),
  };
}

/**
 * Check cache status for monitoring/debugging
 */
async function getCacheStatus(request, env, corsHeaders) {
  const body = await request.json();
  const accountIds = parseAccountIds(body);
  
  const cacheKey = `pre-warmed:${accountIds.join(',')}`;
  const cachedData = await env.CONFIG_KV.get(cacheKey, 'json');
  
  const status = {
    preWarmedCache: {
      exists: !!cachedData,
      age: cachedData ? Date.now() - cachedData.timestamp : null,
      ageMinutes: cachedData ? Math.floor((Date.now() - cachedData.timestamp) / 60000) : null,
    },
    accountIds: accountIds,
  };
  
  return new Response(
    JSON.stringify(status),
    { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
  );
}

/**
 * Fetch account name from Cloudflare API
 */
async function fetchAccountName(apiKey, accountId) {
  try {
    const response = await fetch(
      `https://api.cloudflare.com/client/v4/accounts/${accountId}`,
      {
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
      }
    );
    
    const data = await response.json();
    if (response.ok && data.result?.name) {
      return data.result.name;
    }
    return null;
  } catch (error) {
    return null;
  }
}

/**
 * Fetch metrics for a single account
 * Returns structured data for aggregation
 */
async function fetchAccountMetrics(apiKey, accountId, env) {
  // Fetch account name
  const accountName = await fetchAccountName(apiKey, accountId);
  
  // Calculate date ranges first (needed for cache keys)
  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth() + 1;
  const currentDay = now.getDate();
  const currentHour = now.getHours();
  
  // Try to get cached current month data (10 min TTL with hour-based key)
  const CACHE_VERSION = 2; // Increment this when data structure changes
  const currentMonthCacheKey = `current-month:${accountId}:${currentYear}-${String(currentMonth).padStart(2, '0')}-${String(currentDay).padStart(2, '0')}-${String(Math.floor(currentHour / 1) * 1).padStart(2, '0')}`;
  const cachedCurrentMonth = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
  
  // Check if we have a recent cache (within 10 minutes) and correct version
  if (cachedCurrentMonth && cachedCurrentMonth.cachedAt && cachedCurrentMonth.version === CACHE_VERSION) {
    const cacheAge = Date.now() - cachedCurrentMonth.cachedAt;
    if (cacheAge < 10 * 60 * 1000) { // 10 minutes
      console.log(`Using cached current month data for account ${accountId} (age: ${Math.floor(cacheAge / 1000)}s)`);
      return cachedCurrentMonth.data;
    }
  }
  
  // Check cached zones list (1 hour TTL)
  const zonesCacheKey = `zones:${accountId}`;
  let enterpriseZones = await env.CONFIG_KV.get(zonesCacheKey, 'json');
  
  if (!enterpriseZones) {
    // Fetch Enterprise zones to get their IDs
    enterpriseZones = await fetchEnterpriseZones(apiKey, accountId);
    
    // Cache the zones list for 1 hour
    if (enterpriseZones && enterpriseZones.length > 0) {
      await env.CONFIG_KV.put(zonesCacheKey, JSON.stringify(enterpriseZones), { expirationTtl: 3600 });
    }
  } else {
    console.log(`Using cached zones list for account ${accountId}`);
  }
  
  // If no enterprise zones, return empty metrics (don't throw error)
  if (!enterpriseZones || enterpriseZones.length === 0) {
    return {
      accountId,
      accountName,
      current: {
        requests: 0,
        bytes: 0,
        dnsQueries: 0,
      },
      previous: {
        requests: 0,
        bytes: 0,
        dnsQueries: 0,
      },
      timeSeries: [],
      zoneBreakdown: {
        primary: 0,
        secondary: 0,
        zones: [],
      },
      previousMonthZoneBreakdown: {
        primary: 0,
        secondary: 0,
        zones: [],
      },
    };
  }

  const zoneIds = enterpriseZones.map(z => z.id);

  // Date ranges (currentMonthStart already calculated above)
  const currentMonthEnd = now;
  
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59);
  
  // Check if we have cached previous month data
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(`monthly-stats:${accountId}:${previousMonthKey}`, 'json');

  // Build GraphQL query for current month (Enterprise zones only)
  // Use datetime format for httpRequestsAdaptiveGroups with eyeball filter
  const currentMonthDatetimeStart = currentMonthStart.toISOString();
  const currentMonthDatetimeEnd = currentMonthEnd.toISOString();
  
  // Query for clean/billable requests only (excludes blocked traffic)
  const currentMonthQuery = {
    operationName: 'GetEnterpriseZoneStats',
    variables: {
      zoneIds: zoneIds,
      filter: {
        AND: [
          { datetime_geq: currentMonthDatetimeStart },
          { datetime_leq: currentMonthDatetimeEnd },
          { requestSource: 'eyeball' },
          { securitySource_neq: 'l7ddos' },
          { securityAction_neq: 'block' },
          { securityAction_neq: 'challenge_failed' },
          { securityAction_neq: 'jschallenge_failed' },
          { securityAction_neq: 'managed_challenge_failed' }
        ]
      }
    },
    query: `query GetEnterpriseZoneStats($zoneIds: [String!]!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject) {
      viewer {
        zones(filter: {zoneTag_in: $zoneIds}) {
          zoneTag
          totals: httpRequestsAdaptiveGroups(filter: $filter, limit: 1) {
            count
            sum {
              edgeResponseBytes
            }
            confidence(level: 0.95) {
              count {
                estimate
                lower
                upper
                sampleSize
              }
              sum {
                edgeResponseBytes {
                  estimate
                  lower
                  upper
                  sampleSize
                }
              }
            }
          }
        }
      }
    }`,
  };

  // Separate query for TOTAL eyeball HTTP traffic (includes blocked + clean)
  const totalTrafficQuery = {
    operationName: 'GetEnterpriseZoneTotalTraffic',
    variables: {
      zoneIds: zoneIds,
      filter: {
        AND: [
          { datetime_geq: currentMonthDatetimeStart },
          { datetime_leq: currentMonthDatetimeEnd },
          { requestSource: 'eyeball' }
        ]
      }
    },
    query: `query GetEnterpriseZoneTotalTraffic($zoneIds: [String!]!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject) {
      viewer {
        zones(filter: {zoneTag_in: $zoneIds}) {
          zoneTag
          totals: httpRequestsAdaptiveGroups(filter: $filter, limit: 1) {
            count
            sum {
              edgeResponseBytes
            }
          }
        }
      }
    }`,
  };

  // Make request to Cloudflare GraphQL API for clean/billable traffic
  const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
    },
    body: JSON.stringify(currentMonthQuery),
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(`Failed to fetch metrics for account ${accountId}: ${JSON.stringify(data)}`);
  }

  // Fetch TOTAL traffic in parallel (best-effort; UI treats this as vanity metric)
  let totalTrafficByZone = {};
  try {
    const totalResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`,
      },
      body: JSON.stringify(totalTrafficQuery),
    });

    const totalData = await totalResponse.json();
    const totalZones = totalData.data?.viewer?.zones || [];

    totalZones.forEach(zone => {
      const totals = zone.totals?.[0];
      const totalRequests = totals?.count || 0;
      const totalBytes = totals?.sum?.edgeResponseBytes || 0;
      totalTrafficByZone[zone.zoneTag] = {
        totalRequests,
        totalBytes,
      };
    });
  } catch (e) {
    console.error('Failed to fetch total HTTP traffic (vanity metric):', e);
  }

  // Process and aggregate current month data from all Enterprise zones
  const zones = data.data?.viewer?.zones || [];
  
  if (zones.length === 0) {
    throw new Error(`No zone data found for account ${accountId}`);
  }

  // Aggregate current month stats across all Enterprise zones
  // Now tracking only clean/billable traffic directly, plus vanity total/blocked metrics
  let currentMonthTotal = { 
    requests: 0,        // Clean/billable requests only
    bytes: 0,           // Clean/billable bytes only
    dnsQueries: 0,
    confidence: {
      requests: null,
      bytes: null,
      dnsQueries: null
    },
    // Vanity metrics used only for display in the HTTP/Data Transfer cards
    totalRequests: 0,
    blockedRequests: 0,
    totalBytes: 0,
    blockedBytes: 0,
  };
  
  // Create zone name lookup map
  const zoneNameMap = {};
  enterpriseZones.forEach(z => {
    zoneNameMap[z.id] = z.name;
  });
  
  // Helper function to calculate confidence percentage from interval
  const calculateConfidencePercentage = (confidence) => {
    if (!confidence || !confidence.estimate) return null;
    const estimate = confidence.estimate;
    const lower = confidence.lower || estimate;
    const upper = confidence.upper || estimate;
    
    // Calculate interval width as percentage of estimate
    // Higher % = tighter interval = more confident
    const intervalWidth = upper - lower;
    const relativeWidth = intervalWidth / (2 * estimate);
    const confidencePercent = Math.max(0, Math.min(100, 100 * (1 - relativeWidth)));
    
    return {
      percent: Math.round(confidencePercent * 10) / 10, // Round to 1 decimal
      sampleSize: confidence.sampleSize,
      estimate: confidence.estimate,
      lower: confidence.lower,
      upper: confidence.upper
    };
  };
  
  // Aggregate confidence data for total requests, bytes, and DNS
  let totalRequestsConfidenceData = { estimates: [], lowers: [], uppers: [], sampleSizes: [] };
  let totalBytesConfidenceData = { estimates: [], lowers: [], uppers: [], sampleSizes: [] };
  let totalDnsConfidenceData = { estimates: [], lowers: [], uppers: [], sampleSizes: [] };
  
  // Track per-zone metrics for primary/secondary classification
  const zoneMetrics = [];
  const SECONDARY_ZONE_THRESHOLD = 50 * (1024 ** 3); // 50GB in bytes
  let hasCurrentTotalsForAccount = false;
  
  zones.forEach(zone => {
    // Get aggregated CLEAN/BILLABLE totals (single result, no loop needed)
    const totals = zone.totals?.[0];
    const zoneRequests = totals?.count || 0;
    const zoneBytes = totals?.sum?.edgeResponseBytes || 0;

    // Look up TOTAL eyeball traffic for this zone.
    // If totalTrafficByZone is missing (e.g. vanity query failed),
    // we leave total/blocked null rather than fabricating from billable.
    const totalTraffic = totalTrafficByZone[zone.zoneTag];
    const hasTotalTraffic = totalTraffic && typeof totalTraffic.totalRequests === 'number' && typeof totalTraffic.totalBytes === 'number';
    const zoneTotalRequests = hasTotalTraffic ? totalTraffic.totalRequests : null;
    const zoneTotalBytes = hasTotalTraffic ? totalTraffic.totalBytes : null;

    // Derive BLOCKED as total - clean, clamp at 0 to avoid negatives from rounding
    const zoneBlockedRequests = hasTotalTraffic ? Math.max(0, zoneTotalRequests - zoneRequests) : null;
    const zoneBlockedBytes = hasTotalTraffic ? Math.max(0, zoneTotalBytes - zoneBytes) : null;
    
    // Collect confidence data
    const requestsConf = totals?.confidence?.count;
    const bytesConf = totals?.confidence?.sum?.edgeResponseBytes;
    
    if (requestsConf) {
      totalRequestsConfidenceData.estimates.push(requestsConf.estimate || zoneRequests);
      totalRequestsConfidenceData.lowers.push(requestsConf.lower || zoneRequests);
      totalRequestsConfidenceData.uppers.push(requestsConf.upper || zoneRequests);
      totalRequestsConfidenceData.sampleSizes.push(requestsConf.sampleSize || 0);
    }
    
    if (bytesConf) {
      totalBytesConfidenceData.estimates.push(bytesConf.estimate || zoneBytes);
      totalBytesConfidenceData.lowers.push(bytesConf.lower || zoneBytes);
      totalBytesConfidenceData.uppers.push(bytesConf.upper || zoneBytes);
      totalBytesConfidenceData.sampleSizes.push(bytesConf.sampleSize || 0);
    }
    
    // Add to account totals (clean/billable) and vanity metrics
    currentMonthTotal.requests += zoneRequests;
    currentMonthTotal.bytes += zoneBytes;

    if (hasTotalTraffic) {
      currentMonthTotal.totalRequests += zoneTotalRequests;
      currentMonthTotal.totalBytes += zoneTotalBytes;
      currentMonthTotal.blockedRequests += zoneBlockedRequests;
      currentMonthTotal.blockedBytes += zoneBlockedBytes;
      hasCurrentTotalsForAccount = true;
    }
    
    // Classify zone as primary or secondary based on bandwidth
    const isPrimary = zoneBytes >= SECONDARY_ZONE_THRESHOLD;
    
    zoneMetrics.push({
      zoneTag: zone.zoneTag,
      zoneName: zoneNameMap[zone.zoneTag] || zone.zoneTag,
      requests: zoneRequests,
      bytes: zoneBytes,
      dnsQueries: 0,
      isPrimary,
    });
  });
  
  // If we never saw TOTAL traffic for any zone in this account, treat
  // vanity totals as unavailable so the UI hides the badge instead of
  // showing 0 derived from missing data.
  if (!hasCurrentTotalsForAccount) {
    currentMonthTotal.totalRequests = null;
    currentMonthTotal.totalBytes = null;
    currentMonthTotal.blockedRequests = null;
    currentMonthTotal.blockedBytes = null;
  }
  
  // Calculate aggregated confidence for total requests and bytes
  if (totalRequestsConfidenceData.estimates.length > 0) {
    const totalEstimate = totalRequestsConfidenceData.estimates.reduce((a, b) => a + b, 0);
    const totalLower = totalRequestsConfidenceData.lowers.reduce((a, b) => a + b, 0);
    const totalUpper = totalRequestsConfidenceData.uppers.reduce((a, b) => a + b, 0);
    const totalSampleSize = totalRequestsConfidenceData.sampleSizes.reduce((a, b) => a + b, 0);
    
    currentMonthTotal.confidence.requests = calculateConfidencePercentage({
      estimate: totalEstimate,
      lower: totalLower,
      upper: totalUpper,
      sampleSize: totalSampleSize
    });
  }
  
  if (totalBytesConfidenceData.estimates.length > 0) {
    const totalEstimate = totalBytesConfidenceData.estimates.reduce((a, b) => a + b, 0);
    const totalLower = totalBytesConfidenceData.lowers.reduce((a, b) => a + b, 0);
    const totalUpper = totalBytesConfidenceData.uppers.reduce((a, b) => a + b, 0);
    const totalSampleSize = totalBytesConfidenceData.sampleSizes.reduce((a, b) => a + b, 0);
    
    currentMonthTotal.confidence.bytes = calculateConfidencePercentage({
      estimate: totalEstimate,
      lower: totalLower,
      upper: totalUpper,
      sampleSize: totalSampleSize
    });
  }
  
  // Fetch DNS queries for each zone IN PARALLEL
  try {
    const datetimeStart = currentMonthStart.toISOString();
    const datetimeEnd = currentMonthEnd.toISOString();
    
    // Process all zones in parallel - fetch DNS queries only
    await Promise.all(zoneMetrics.map(async (zoneMetric) => {
      try {
        // Fetch DNS queries
        const dnsResult = await (async () => {
          const dnsQuery = {
            operationName: 'DnsTotals',
            variables: {
              zoneTag: zoneMetric.zoneTag,
              filter: {
                AND: [{
                  datetime_geq: datetimeStart,
                  datetime_leq: datetimeEnd
                }]
              }
            },
            query: `query DnsTotals($zoneTag: string, $filter: ZoneDnsAnalyticsAdaptiveGroupsFilter_InputObject) {
              viewer {
                zones(filter: {zoneTag: $zoneTag}) {
                  queryTotals: dnsAnalyticsAdaptiveGroups(limit: 5000, filter: $filter) {
                    count
                    confidence(level: 0.95) {
                      count {
                        estimate
                        lower
                        upper
                        sampleSize
                      }
                    }
                  }
                }
              }
            }`
          };

          const dnsResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${apiKey}`,
            },
            body: JSON.stringify(dnsQuery),
          });

          const dnsData = await dnsResponse.json();
          
          if (dnsResponse.ok && dnsData.data?.viewer?.zones?.[0]?.queryTotals?.[0]) {
            const queryData = dnsData.data.viewer.zones[0].queryTotals[0];
            return {
              count: queryData.count || 0,
              confidence: queryData.confidence?.count || null
            };
          }
          return { count: 0, confidence: null };
        })();
        
        // Update zone metrics and collect confidence
        zoneMetric.dnsQueries = dnsResult.count;
        currentMonthTotal.dnsQueries += dnsResult.count;
        
        // Collect DNS confidence data
        if (dnsResult.confidence) {
          totalDnsConfidenceData.estimates.push(dnsResult.confidence.estimate || dnsResult.count);
          totalDnsConfidenceData.lowers.push(dnsResult.confidence.lower || dnsResult.count);
          totalDnsConfidenceData.uppers.push(dnsResult.confidence.upper || dnsResult.count);
          totalDnsConfidenceData.sampleSizes.push(dnsResult.confidence.sampleSize || 0);
        }
      } catch (error) {
        console.error(`Error fetching DNS for zone ${zoneMetric.zoneTag}:`, error);
        zoneMetric.dnsQueries = 0;
      }
    }));
  } catch (error) {
    console.error('Error fetching zone metrics:', error);
  }
  
  // Calculate aggregated confidence for DNS queries
  if (totalDnsConfidenceData.estimates.length > 0) {
    const totalEstimate = totalDnsConfidenceData.estimates.reduce((a, b) => a + b, 0);
    const totalLower = totalDnsConfidenceData.lowers.reduce((a, b) => a + b, 0);
    const totalUpper = totalDnsConfidenceData.uppers.reduce((a, b) => a + b, 0);
    const totalSampleSize = totalDnsConfidenceData.sampleSizes.reduce((a, b) => a + b, 0);
    
    currentMonthTotal.confidence.dnsQueries = calculateConfidencePercentage({
      estimate: totalEstimate,
      lower: totalLower,
      upper: totalUpper,
      sampleSize: totalSampleSize
    });
  }

  // Handle previous month data
  let previousMonthStats = { 
    requests: 0,  // Clean/billable requests only
    bytes: 0,     // Clean/billable bytes only
    dnsQueries: 0,
    // Vanity HTTP metrics for previous month (for display only).
    // Use null so that older cached records (without these fields)
    // fall back to billable values instead of forcing 0 totals.
    totalRequests: null,
    blockedRequests: null,
    totalBytes: null,
    blockedBytes: null,
  };
  
  if (cachedPreviousMonth) {
    // Use cached data for complete previous month
    previousMonthStats = {
      ...previousMonthStats,
      ...cachedPreviousMonth
    };
  } else if (now.getDate() >= 2) {
    // Only query if we're at least 2 days into current month (previous month is complete)
    const previousMonthDatetimeStart = previousMonthStart.toISOString();
    const previousMonthDatetimeEnd = previousMonthEnd.toISOString();
    
    // Clean/billable HTTP traffic for previous month
    const previousMonthQuery = {
      operationName: 'GetPreviousMonthStats',
      variables: {
        zoneIds: zoneIds,
        filter: {
          AND: [
            { datetime_geq: previousMonthDatetimeStart },
            { datetime_leq: previousMonthDatetimeEnd },
            { requestSource: 'eyeball' },
            { securitySource_neq: 'l7ddos' },
            { securityAction_neq: 'block' },
            { securityAction_neq: 'challenge_failed' },
            { securityAction_neq: 'jschallenge_failed' },
            { securityAction_neq: 'managed_challenge_failed' }
          ]
        }
      },
      query: `query GetPreviousMonthStats($zoneIds: [String!]!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject) {
        viewer {
          zones(filter: {zoneTag_in: $zoneIds}) {
            zoneTag
            totals: httpRequestsAdaptiveGroups(filter: $filter, limit: 1) {
              count
              sum {
                edgeResponseBytes
              }
            }
          }
        }
      }`,
    };

    // TOTAL eyeball HTTP traffic for previous month (includes blocked + clean)
    const previousMonthTotalTrafficQuery = {
      operationName: 'GetPreviousMonthTotalTraffic',
      variables: {
        zoneIds: zoneIds,
        filter: {
          AND: [
            { datetime_geq: previousMonthDatetimeStart },
            { datetime_leq: previousMonthDatetimeEnd },
            { requestSource: 'eyeball' }
          ]
        }
      },
      query: `query GetPreviousMonthTotalTraffic($zoneIds: [String!]!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject) {
        viewer {
          zones(filter: {zoneTag_in: $zoneIds}) {
            zoneTag
            totals: httpRequestsAdaptiveGroups(filter: $filter, limit: 1) {
              count
              sum {
                edgeResponseBytes
              }
            }
          }
        }
      }`,
    };

    const prevResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`,
      },
      body: JSON.stringify(previousMonthQuery),
    });

    const prevData = await prevResponse.json();
    const prevZones = prevData.data?.viewer?.zones || [];

    // Fetch TOTAL previous-month traffic (best-effort)
    let prevTotalTrafficByZone = {};
    try {
      const prevTotalResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${apiKey}`,
        },
        body: JSON.stringify(previousMonthTotalTrafficQuery),
      });

      const prevTotalData = await prevTotalResponse.json();
      const prevTotalZones = prevTotalData.data?.viewer?.zones || [];

      prevTotalZones.forEach(zone => {
        const totals = zone.totals?.[0];
        const totalRequests = totals?.count || 0;
        const totalBytes = totals?.sum?.edgeResponseBytes || 0;
        prevTotalTrafficByZone[zone.zoneTag] = {
          totalRequests,
          totalBytes,
        };
      });
    } catch (e) {
      console.error('Failed to fetch previous month total HTTP traffic (vanity metric):', e);
    }
    
    // Track per-zone metrics for previous month
    const prevZoneMetrics = [];
    const SECONDARY_ZONE_THRESHOLD = 50 * (1024 ** 3); // 50GB in bytes
    
    prevZones.forEach(zone => {
      // Get aggregated CLEAN/BILLABLE totals (single result, no loop needed)
      const totals = zone.totals?.[0];
      const zoneRequests = totals?.count || 0;
      const zoneBytes = totals?.sum?.edgeResponseBytes || 0;

      // Look up TOTAL eyeball traffic for this zone
      const totalTraffic = prevTotalTrafficByZone[zone.zoneTag];
      const hasTotalTraffic = totalTraffic && typeof totalTraffic.totalRequests === 'number' && typeof totalTraffic.totalBytes === 'number';
      const zoneTotalRequests = hasTotalTraffic ? totalTraffic.totalRequests : null;
      const zoneTotalBytes = hasTotalTraffic ? totalTraffic.totalBytes : null;

      // Derive BLOCKED as total - clean, clamp at 0
      const zoneBlockedRequests = hasTotalTraffic ? Math.max(0, zoneTotalRequests - zoneRequests) : null;
      const zoneBlockedBytes = hasTotalTraffic ? Math.max(0, zoneTotalBytes - zoneBytes) : null;

      // Add to previous month totals (clean/billable and vanity)
      previousMonthStats.requests += zoneRequests;
      previousMonthStats.bytes += zoneBytes;
      if (hasTotalTraffic) {
        previousMonthStats.totalRequests += zoneTotalRequests;
        previousMonthStats.totalBytes += zoneTotalBytes;
        previousMonthStats.blockedRequests += zoneBlockedRequests;
        previousMonthStats.blockedBytes += zoneBlockedBytes;
      }
      
      // Classify zone as primary or secondary based on bandwidth
      const isPrimary = zoneBytes >= SECONDARY_ZONE_THRESHOLD;
      
      prevZoneMetrics.push({
        zoneTag: zone.zoneTag,
        zoneName: zoneNameMap[zone.zoneTag] || zone.zoneTag,
        requests: zoneRequests,
        bytes: zoneBytes,
        dnsQueries: 0,
        isPrimary,
      });
    });
    
    // Fetch DNS queries for previous month IN PARALLEL
    try {
      const prevDatetimeStart = previousMonthStart.toISOString();
      const prevDatetimeEnd = previousMonthEnd.toISOString();
      
      const dnsResults = await Promise.allSettled(
        prevZoneMetrics.map(async (prevZoneMetric) => {
          const dnsQuery = {
            operationName: 'DnsTotals',
            variables: {
              zoneTag: prevZoneMetric.zoneTag,
              filter: {
                AND: [{
                  datetime_geq: prevDatetimeStart,
                  datetime_leq: prevDatetimeEnd
                }]
              }
            },
            query: `query DnsTotals($zoneTag: string, $filter: ZoneDnsAnalyticsAdaptiveGroupsFilter_InputObject) {
              viewer {
                zones(filter: {zoneTag: $zoneTag}) {
                  queryTotals: dnsAnalyticsAdaptiveGroups(limit: 5000, filter: $filter) {
                    count
                  }
                }
              }
            }`
          };

          const dnsResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${apiKey}`,
            },
            body: JSON.stringify(dnsQuery),
          });

          const dnsData = await dnsResponse.json();
          
          if (dnsResponse.ok && dnsData.data?.viewer?.zones?.[0]?.queryTotals?.[0]?.count) {
            return { zoneMetric: prevZoneMetric, count: dnsData.data.viewer.zones[0].queryTotals[0].count };
          }
          return { zoneMetric: prevZoneMetric, count: 0 };
        })
      );
      
      // Process results
      dnsResults.forEach((result) => {
        if (result.status === 'fulfilled') {
          result.value.zoneMetric.dnsQueries = result.value.count;
          previousMonthStats.dnsQueries += result.value.count;
        }
      });
    } catch (prevDnsError) {
      console.error('Error fetching previous month DNS queries:', prevDnsError);
    }
    
    // Store zone metrics in previous month stats for caching
    previousMonthStats.zoneMetrics = prevZoneMetrics;

    // Cache the previous month data since it's now complete
    await env.CONFIG_KV.put(
      `monthly-stats:${accountId}:${previousMonthKey}`,
      JSON.stringify(previousMonthStats),
      { expirationTtl: 31536000 } // 1 year
    );
  }

  // Fetch DNS queries for previous month (even if other data is cached)
  // This handles cases where data was cached before DNS tracking was added
  if (now.getDate() >= 2 && (!previousMonthStats.dnsQueries || previousMonthStats.dnsQueries === 0)) {
    try {
      const prevDatetimeStart = previousMonthStart.toISOString();
      const prevDatetimeEnd = previousMonthEnd.toISOString();
      
      // Get zone metrics from cached data or rebuild from zones list
      let prevZoneMetricsForDns = previousMonthStats.zoneMetrics || [];
      
      // If we don't have zone metrics, we need to get the zones list
      if (prevZoneMetricsForDns.length === 0) {
        prevZoneMetricsForDns = enterpriseZones.map(z => ({ zoneTag: z.id, dnsQueries: 0 }));
      }
      
      const dnsResults = await Promise.allSettled(
        prevZoneMetricsForDns.map(async (prevZoneMetric) => {
          const dnsQuery = {
            operationName: 'DnsTotals',
            variables: {
              zoneTag: prevZoneMetric.zoneTag,
              filter: {
                AND: [{
                  datetime_geq: prevDatetimeStart,
                  datetime_leq: prevDatetimeEnd
                }]
              }
            },
            query: `query DnsTotals($zoneTag: string, $filter: ZoneDnsAnalyticsAdaptiveGroupsFilter_InputObject) {
              viewer {
                zones(filter: {zoneTag: $zoneTag}) {
                  queryTotals: dnsAnalyticsAdaptiveGroups(limit: 5000, filter: $filter) {
                    count
                  }
                }
              }
            }`
          };

          const dnsResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${apiKey}`,
            },
            body: JSON.stringify(dnsQuery),
          });

          const dnsData = await dnsResponse.json();
          
          if (dnsResponse.ok && dnsData.data?.viewer?.zones?.[0]?.queryTotals?.[0]?.count) {
            return { zoneMetric: prevZoneMetric, count: dnsData.data.viewer.zones[0].queryTotals[0].count };
          }
          return { zoneMetric: prevZoneMetric, count: 0 };
        })
      );
      
      // Process results
      dnsResults.forEach((result) => {
        if (result.status === 'fulfilled') {
          result.value.zoneMetric.dnsQueries = result.value.count;
          previousMonthStats.dnsQueries = (previousMonthStats.dnsQueries || 0) + result.value.count;
        }
      });
      
      // Update zone metrics with DNS data
      if (previousMonthStats.zoneMetrics) {
        previousMonthStats.zoneMetrics = prevZoneMetricsForDns;
      }
      
      // Update the cache with DNS query data
      await env.CONFIG_KV.put(
        `monthly-stats:${accountId}:${previousMonthKey}`,
        JSON.stringify(previousMonthStats),
        { expirationTtl: 31536000 } // 1 year
      );
    } catch (prevDnsError) {
      console.error('Error fetching previous month DNS queries retroactively:', prevDnsError);
    }
  }

  // Get historical monthly data from KV
  const historicalData = await getHistoricalMonthlyData(env, accountId);
  
  // Add current month to time series
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const timeSeriesData = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      requests: currentMonthTotal.requests, // Clean/billable requests
      bytes: currentMonthTotal.bytes,
      dnsQueries: currentMonthTotal.dnsQueries,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  // Calculate primary/secondary zone counts for current month
  const primaryZonesCount = zoneMetrics.filter(z => z.isPrimary).length;
  const secondaryZonesCount = zoneMetrics.filter(z => !z.isPrimary).length;

  // Calculate primary/secondary zone counts for previous month
  const prevZoneMetrics = previousMonthStats.zoneMetrics || [];
  
  // ✅ Add zoneName to cached previous month zones (they might not have it from old cache)
  prevZoneMetrics.forEach(zone => {
    if (!zone.zoneName && zoneNameMap[zone.zoneTag]) {
      zone.zoneName = zoneNameMap[zone.zoneTag];
    }
  });
  
  const prevPrimaryZonesCount = prevZoneMetrics.filter(z => z.isPrimary).length;
  const prevSecondaryZonesCount = prevZoneMetrics.filter(z => !z.isPrimary).length;

  // Return structured data (not Response object)
  const result = {
    accountId,
    accountName,
    current: {
      requests: currentMonthTotal.requests,  // Clean/billable requests only
      bytes: currentMonthTotal.bytes,        // Clean/billable bytes only
      dnsQueries: currentMonthTotal.dnsQueries,
      confidence: currentMonthTotal.confidence,
      // Vanity HTTP metrics for display only
      totalRequests: currentMonthTotal.totalRequests,
      blockedRequests: currentMonthTotal.blockedRequests,
      totalBytes: currentMonthTotal.totalBytes,
      blockedBytes: currentMonthTotal.blockedBytes,
    },
    previous: previousMonthStats,
    timeSeries: timeSeriesData,
    zoneBreakdown: {
      primary: primaryZonesCount,
      secondary: secondaryZonesCount,
      zones: zoneMetrics,
    },
    previousMonthZoneBreakdown: {
      primary: prevPrimaryZonesCount,
      secondary: prevSecondaryZonesCount,
      zones: prevZoneMetrics,
    },
  };
  
  // Cache the current month data (10 min TTL)
  try {
    await env.CONFIG_KV.put(
      currentMonthCacheKey,
      JSON.stringify({
        version: 2, // Must match CACHE_VERSION above
        cachedAt: Date.now(),
        data: result
      }),
      { expirationTtl: 600 } // 10 minutes
    );
    console.log(`Cached current month data for account ${accountId}`);
  } catch (cacheError) {
    console.error('Failed to cache current month data:', cacheError);
  }
  
  return result;
}

/**
 * Aggregate metrics from multiple accounts
 */
function aggregateAccountMetrics(accountMetrics) {
  const aggregated = {
    current: {
      requests: 0,  // Clean/billable requests only
      bytes: 0,     // Clean/billable bytes only
      dnsQueries: 0,
      confidence: {
        requests: null,
        bytes: null,
        dnsQueries: null
      },
      // Vanity HTTP metrics used only for display in the HTTP/Data Transfer cards
      totalRequests: null,
      blockedRequests: null,
      totalBytes: null,
      blockedBytes: null,
    },
    previous: {
      requests: 0,
      bytes: 0,
      dnsQueries: 0,
      // Vanity HTTP metrics for previous month
      totalRequests: null,
      blockedRequests: null,
      totalBytes: null,
      blockedBytes: null,
    },
    timeSeries: [],
    zoneBreakdown: {
      primary: 0,
      secondary: 0,
      zones: [],
    },
    previousMonthZoneBreakdown: {
      primary: 0,
      secondary: 0,
      zones: [],
    },
    perAccountData: accountMetrics,  // Store for future filtering
  };

  // Aggregate current month
  const confidenceAggregator = {
    requests: { estimates: [], lowers: [], uppers: [], sampleSizes: [] },
    bytes: { estimates: [], lowers: [], uppers: [], sampleSizes: [] },
    dnsQueries: { estimates: [], lowers: [], uppers: [], sampleSizes: [] }
  };
  
  let hasCurrentTotals = false;
  let hasPreviousTotals = false;

  accountMetrics.forEach(accountData => {
    aggregated.current.requests += accountData.current.requests || 0;
    aggregated.current.bytes += accountData.current.bytes || 0;
    aggregated.current.dnsQueries += accountData.current.dnsQueries || 0;
    // Vanity HTTP metrics (only when real totals are present)
    if (typeof accountData.current.totalRequests === 'number') {
      aggregated.current.totalRequests = (aggregated.current.totalRequests || 0) + accountData.current.totalRequests;
      hasCurrentTotals = true;
    }
    if (typeof accountData.current.blockedRequests === 'number') {
      aggregated.current.blockedRequests = (aggregated.current.blockedRequests || 0) + accountData.current.blockedRequests;
      hasCurrentTotals = true;
    }
    if (typeof accountData.current.totalBytes === 'number') {
      aggregated.current.totalBytes = (aggregated.current.totalBytes || 0) + accountData.current.totalBytes;
      hasCurrentTotals = true;
    }
    if (typeof accountData.current.blockedBytes === 'number') {
      aggregated.current.blockedBytes = (aggregated.current.blockedBytes || 0) + accountData.current.blockedBytes;
      hasCurrentTotals = true;
    }
    
    // Collect confidence data from each account
    if (accountData.current.confidence) {
      if (accountData.current.confidence.requests) {
        const conf = accountData.current.confidence.requests;
        confidenceAggregator.requests.estimates.push(conf.estimate);
        confidenceAggregator.requests.lowers.push(conf.lower);
        confidenceAggregator.requests.uppers.push(conf.upper);
        confidenceAggregator.requests.sampleSizes.push(conf.sampleSize);
      }
      if (accountData.current.confidence.bytes) {
        const conf = accountData.current.confidence.bytes;
        confidenceAggregator.bytes.estimates.push(conf.estimate);
        confidenceAggregator.bytes.lowers.push(conf.lower);
        confidenceAggregator.bytes.uppers.push(conf.upper);
        confidenceAggregator.bytes.sampleSizes.push(conf.sampleSize);
      }
      if (accountData.current.confidence.dnsQueries) {
        const conf = accountData.current.confidence.dnsQueries;
        confidenceAggregator.dnsQueries.estimates.push(conf.estimate);
        confidenceAggregator.dnsQueries.lowers.push(conf.lower);
        confidenceAggregator.dnsQueries.uppers.push(conf.upper);
        confidenceAggregator.dnsQueries.sampleSizes.push(conf.sampleSize);
      }
    }
  });
  
  // Calculate aggregated confidence percentages
  const calculateConfidencePercentage = (confidence) => {
    if (!confidence || !confidence.estimate) return null;
    const estimate = confidence.estimate;
    const lower = confidence.lower || estimate;
    const upper = confidence.upper || estimate;
    const intervalWidth = upper - lower;
    const relativeWidth = intervalWidth / (2 * estimate);
    const confidencePercent = Math.max(0, Math.min(100, 100 * (1 - relativeWidth)));
    return {
      percent: Math.round(confidencePercent * 10) / 10,
      sampleSize: confidence.sampleSize,
      estimate: confidence.estimate,
      lower: confidence.lower,
      upper: confidence.upper
    };
  };
  
  // Aggregate confidence for requests
  if (confidenceAggregator.requests.estimates.length > 0) {
    const totalEstimate = confidenceAggregator.requests.estimates.reduce((a, b) => a + b, 0);
    const totalLower = confidenceAggregator.requests.lowers.reduce((a, b) => a + b, 0);
    const totalUpper = confidenceAggregator.requests.uppers.reduce((a, b) => a + b, 0);
    const totalSampleSize = confidenceAggregator.requests.sampleSizes.reduce((a, b) => a + b, 0);
    aggregated.current.confidence.requests = calculateConfidencePercentage({
      estimate: totalEstimate,
      lower: totalLower,
      upper: totalUpper,
      sampleSize: totalSampleSize
    });
  }
  
  // Aggregate confidence for bytes
  if (confidenceAggregator.bytes.estimates.length > 0) {
    const totalEstimate = confidenceAggregator.bytes.estimates.reduce((a, b) => a + b, 0);
    const totalLower = confidenceAggregator.bytes.lowers.reduce((a, b) => a + b, 0);
    const totalUpper = confidenceAggregator.bytes.uppers.reduce((a, b) => a + b, 0);
    const totalSampleSize = confidenceAggregator.bytes.sampleSizes.reduce((a, b) => a + b, 0);
    aggregated.current.confidence.bytes = calculateConfidencePercentage({
      estimate: totalEstimate,
      lower: totalLower,
      upper: totalUpper,
      sampleSize: totalSampleSize
    });
  }
  
  // Aggregate confidence for DNS queries
  if (confidenceAggregator.dnsQueries.estimates.length > 0) {
    const totalEstimate = confidenceAggregator.dnsQueries.estimates.reduce((a, b) => a + b, 0);
    const totalLower = confidenceAggregator.dnsQueries.lowers.reduce((a, b) => a + b, 0);
    const totalUpper = confidenceAggregator.dnsQueries.uppers.reduce((a, b) => a + b, 0);
    const totalSampleSize = confidenceAggregator.dnsQueries.sampleSizes.reduce((a, b) => a + b, 0);
    aggregated.current.confidence.dnsQueries = calculateConfidencePercentage({
      estimate: totalEstimate,
      lower: totalLower,
      upper: totalUpper,
      sampleSize: totalSampleSize
    });
  }

  // Aggregate previous month
  accountMetrics.forEach(accountData => {
    aggregated.previous.requests += accountData.previous.requests || 0;
    aggregated.previous.bytes += accountData.previous.bytes || 0;
    aggregated.previous.dnsQueries += accountData.previous.dnsQueries || 0;
    if (typeof accountData.previous.totalRequests === 'number') {
      aggregated.previous.totalRequests = (aggregated.previous.totalRequests || 0) + accountData.previous.totalRequests;
      hasPreviousTotals = true;
    }
    if (typeof accountData.previous.blockedRequests === 'number') {
      aggregated.previous.blockedRequests = (aggregated.previous.blockedRequests || 0) + accountData.previous.blockedRequests;
      hasPreviousTotals = true;
    }
    if (typeof accountData.previous.totalBytes === 'number') {
      aggregated.previous.totalBytes = (aggregated.previous.totalBytes || 0) + accountData.previous.totalBytes;
      hasPreviousTotals = true;
    }
    if (typeof accountData.previous.blockedBytes === 'number') {
      aggregated.previous.blockedBytes = (aggregated.previous.blockedBytes || 0) + accountData.previous.blockedBytes;
      hasPreviousTotals = true;
    }
  });

  // If no real totals were seen, keep them as null so the UI hides the badge
  if (!hasCurrentTotals) {
    aggregated.current.totalRequests = null;
    aggregated.current.blockedRequests = null;
    aggregated.current.totalBytes = null;
    aggregated.current.blockedBytes = null;
  }
  if (!hasPreviousTotals) {
    aggregated.previous.totalRequests = null;
    aggregated.previous.blockedRequests = null;
    aggregated.previous.totalBytes = null;
    aggregated.previous.blockedBytes = null;
  }

  // Aggregate zone breakdowns
  accountMetrics.forEach(accountData => {
    aggregated.zoneBreakdown.primary += accountData.zoneBreakdown.primary || 0;
    aggregated.zoneBreakdown.secondary += accountData.zoneBreakdown.secondary || 0;
    if (accountData.zoneBreakdown.zones) {
      aggregated.zoneBreakdown.zones.push(...accountData.zoneBreakdown.zones);
    }

    aggregated.previousMonthZoneBreakdown.primary += accountData.previousMonthZoneBreakdown.primary || 0;
    aggregated.previousMonthZoneBreakdown.secondary += accountData.previousMonthZoneBreakdown.secondary || 0;
    if (accountData.previousMonthZoneBreakdown.zones) {
      aggregated.previousMonthZoneBreakdown.zones.push(...accountData.previousMonthZoneBreakdown.zones);
    }
  });

  // Merge time series data from all accounts
  const timeSeriesMap = new Map();
  accountMetrics.forEach(accountData => {
    if (accountData.timeSeries) {
      accountData.timeSeries.forEach(entry => {
        const existing = timeSeriesMap.get(entry.month);
        if (existing) {
          existing.requests += entry.requests || 0;
          existing.bytes += entry.bytes || 0;
          existing.dnsQueries += entry.dnsQueries || 0;
        } else {
          timeSeriesMap.set(entry.month, {
            month: entry.month,
            timestamp: entry.timestamp,
            requests: entry.requests || 0,
            bytes: entry.bytes || 0,
            dnsQueries: entry.dnsQueries || 0,
          });
        }
      });
    }
  });

  aggregated.timeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return aggregated;
}

/**
 * Fetch enterprise zones count (supports multiple accounts)
 */
async function getZones(request, env, corsHeaders) {
  const body = await request.json();
  
  // API Token: Read from wrangler secret (secure storage)
  const apiKey = env.CLOUDFLARE_API_TOKEN;
  // Account IDs: From KV/UI (supports multi-account)
  const accountIds = parseAccountIds(body);

  if (!apiKey) {
    return new Response(JSON.stringify({ error: 'API token not configured. Set it using: npx wrangler secret put CLOUDFLARE_API_TOKEN' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  if (accountIds.length === 0) {
    return new Response(JSON.stringify({ error: 'Account IDs not configured. Please configure them in Settings.' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  // Fetch zones and account names from all accounts
  const allEnterpriseZones = [];
  const accountNames = {}; // Map of accountId -> accountName
  
  for (const accountId of accountIds) {
    try {
      // Fetch account name
      const accountName = await fetchAccountName(apiKey, accountId);
      accountNames[accountId] = accountName || accountId;
      
      // Fetch zones
      const zones = await fetchEnterpriseZones(apiKey, accountId);
      if (zones && zones.length > 0) {
        // Add account info to each zone
        zones.forEach(z => {
          allEnterpriseZones.push({
            ...z,
            account: { id: accountId, name: accountNames[accountId] }
          });
        });
      }
    } catch (error) {
      console.error(`Error fetching zones for account ${accountId}:`, error);
      // Still store account ID as fallback name
      accountNames[accountId] = accountId;
    }
  }

  const zoneCount = allEnterpriseZones.length;
  const now = new Date();
  const monthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  try {
    await env.CONFIG_KV.put(
      `monthly-zone-count:${monthKey}`,
      JSON.stringify({ count: zoneCount, timestamp: now.toISOString() }),
      { expirationTtl: 31536000 }
    );
  } catch (e) {
    console.error('Failed to store zone count snapshot:', e);
  }

  const zonesTimeSeries = await getHistoricalZoneCountData(env, zoneCount);

  return new Response(
    JSON.stringify({
      total: zoneCount,
      enterprise: zoneCount,
      zones: allEnterpriseZones.map(z => ({ 
        id: z.id, 
        name: z.name,
        account: z.account 
      })),
      accounts: accountNames,
      zonesTimeSeries,
    }),
    {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    }
  );
}

async function getHistoricalZoneCountData(env, currentCount) {
  const timeSeries = [];
  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

  try {
    const listResult = await env.CONFIG_KV.list({ prefix: 'monthly-zone-count:' });
    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.replace('monthly-zone-count:', '');
        const [year, monthNum] = month.split('-');
        timeSeries.push({
          month,
          timestamp: new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString(),
          zones: data.count,
        });
      }
    }
  } catch (e) {
    console.error('Failed to load historical zone count data:', e);
  }

  const hasCurrentMonth = timeSeries.some(e => e.month === currentMonthKey);
  if (!hasCurrentMonth && currentCount > 0) {
    timeSeries.push({
      month: currentMonthKey,
      timestamp: new Date(now.getFullYear(), now.getMonth(), 1).toISOString(),
      zones: currentCount,
    });
  } else if (hasCurrentMonth) {
    const entry = timeSeries.find(e => e.month === currentMonthKey);
    if (entry) entry.zones = currentCount;
  }

  return timeSeries.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
}

/**
 * Get stored configuration
 */
async function getConfig(request, env, corsHeaders) {
  const url = new URL(request.url);
  const userId = url.searchParams.get('userId') || 'default';
  
  const config = await env.CONFIG_KV.get(`config:${userId}`, 'json');
  
  return new Response(
    JSON.stringify(config || {}),
    {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    }
  );
}

/**
 * Save configuration to KV
 */
async function saveConfig(request, env, corsHeaders) {
  const body = await request.json();
  const { userId = 'default', config } = body;

  if (!config) {
    return new Response(JSON.stringify({ error: 'Missing config' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  // Store config in KV (API token is stored separately as a wrangler secret)
  await env.CONFIG_KV.put(`config:${userId}`, JSON.stringify(config));

  return new Response(
    JSON.stringify({ success: true }),
    {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    }
  );
}

/**
 * Check thresholds and send Slack notifications
 * Supports two modes:
 *   mode='alert' - Only sends SKUs at ≥90% utilization
 *   mode='report' - Sends full usage report for all SKUs
 */
async function checkThresholds(request, env, corsHeaders) {
  const body = await request.json();
  const { skuMetrics, slackWebhook, accountIds, accountId, mode, alertFrequency } = body;

  const accounts = accountIds || (accountId ? [accountId] : []);
  const accountsDisplay = accounts.length > 1 ? `${accounts.length} accounts` : accounts[0] || 'Unknown';
  const dashboardUrl = new URL(request.url).origin;

  if (!slackWebhook) {
    return new Response(
      JSON.stringify({ success: false, message: 'No Slack webhook configured' }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' }, status: 400 }
    );
  }

  if (!skuMetrics || skuMetrics.length === 0) {
    return new Response(
      JSON.stringify({ success: false, message: 'No metrics provided' }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' }, status: 400 }
    );
  }

  if (mode === 'report') {
    try {
      await sendSlackReport(skuMetrics, slackWebhook, dashboardUrl, accountsDisplay);
      return new Response(
        JSON.stringify({ success: true, message: 'Usage report sent to Slack!' }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      console.error('Slack report error:', error);
      return new Response(
        JSON.stringify({ success: false, message: 'Failed to send report: ' + error.message }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' }, status: 500 }
      );
    }
  }

  const alerts = skuMetrics.filter(m => m.percentage >= 90);

  if (alerts.length === 0) {
    return new Response(
      JSON.stringify({ alerts: [], alertsTriggered: false, slackSent: false }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }

  try {
    const now = new Date();
    const freq = alertFrequency || 'monthly';
    let periodKey;
    if (freq === 'weekly') {
      const startOfYear = new Date(now.getFullYear(), 0, 1);
      const weekNum = Math.ceil(((now - startOfYear) / 86400000 + startOfYear.getDay() + 1) / 7);
      periodKey = `${now.getFullYear()}-W${String(weekNum).padStart(2, '0')}`;
    } else {
      periodKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    }

    const accountsKey = accounts.sort().join('-');
    const newAlerts = [];
    for (const alert of alerts) {
      const alertKey = `alert-sent:${accountsKey}:${alert.key}:${periodKey}`;
      const alreadySent = await env.CONFIG_KV.get(alertKey);
      if (!alreadySent) {
        newAlerts.push(alert);
        await env.CONFIG_KV.put(alertKey, 'true', { expirationTtl: 3888000 });
      }
    }

    if (newAlerts.length > 0) {
      await sendSlackAlert(newAlerts, slackWebhook, dashboardUrl);
      return new Response(
        JSON.stringify({ alerts: newAlerts, alertsTriggered: true, slackSent: true, skipped: alerts.length - newAlerts.length }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
      const freqLabel = freq === 'weekly' ? 'this week' : 'this month';
      return new Response(
        JSON.stringify({ alerts: [], alertsTriggered: true, slackSent: false, message: `All alerts already sent ${freqLabel}`, skipped: alerts.length }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
  } catch (error) {
    console.error('Slack alert error:', error);
    return new Response(
      JSON.stringify({ alerts, alertsTriggered: true, slackSent: false, error: error.message }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
}

async function sendSlackAlert(alerts, webhookUrl, dashboardUrl) {
  const alertBlocks = alerts.map(alert => {
    const emoji = alert.percentage >= 100 ? '🔴' : '🟡';
    return {
      type: "section",
      fields: [
        { type: "mrkdwn", text: `${emoji} *${alert.name}*\n${alert.percentage.toFixed(1)}% of threshold` },
        { type: "mrkdwn", text: `*Current:* ${alert.formatted}\n*Threshold:* ${alert.thresholdFormatted}` }
      ]
    };
  });

  const slackPayload = {
    blocks: [
      { type: "header", text: { type: "plain_text", text: "⚠️ Cloudflare Usage Alert", emoji: true } },
      { type: "section", text: { type: "mrkdwn", text: `*${alerts.length} product(s) at ≥90% of contracted thresholds:*` } },
      { type: "divider" },
      ...alertBlocks,
      { type: "divider" },
      { type: "context", elements: [{ type: "mrkdwn", text: `🕐 <!date^${Math.floor(Date.now() / 1000)}^{date_short_pretty} at {time}|${new Date().toUTCString()}>` }] },
      { type: "actions", elements: [{ type: "button", text: { type: "plain_text", text: "View Dashboard", emoji: true }, url: dashboardUrl, style: "primary" }] }
    ]
  };

  const response = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(slackPayload),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to send Slack alert: ${response.status} - ${errorText}`);
  }
  return true;
}

async function sendSlackReport(skuMetrics, webhookUrl, dashboardUrl, accountsDisplay) {
  const categories = {};
  skuMetrics.forEach(m => {
    const cat = m.category || 'Other';
    if (!categories[cat]) categories[cat] = [];
    categories[cat].push(m);
  });

  const reportBlocks = [];
  for (const [category, items] of Object.entries(categories)) {
    reportBlocks.push({ type: "section", text: { type: "mrkdwn", text: `*${category}*` } });
    const lines = items.map(m => {
      let emoji = '🟢';
      if (m.percentage >= 100) emoji = '🔴';
      else if (m.percentage >= 90) emoji = '🟡';
      const pctStr = m.threshold ? `${m.percentage.toFixed(1)}%` : 'N/A';
      return `${emoji} *${m.name}:* ${m.formatted} ${m.threshold ? `(${pctStr} of ${m.thresholdFormatted})` : '_(no threshold)_'}`;
    });
    reportBlocks.push({ type: "section", text: { type: "mrkdwn", text: lines.join('\n') } });
  }

  const slackPayload = {
    blocks: [
      { type: "header", text: { type: "plain_text", text: "📊 Enterprise Usage Report", emoji: true } },
      { type: "section", text: { type: "mrkdwn", text: `Usage summary for *${accountsDisplay}*` } },
      { type: "divider" },
      ...reportBlocks,
      { type: "divider" },
      { type: "context", elements: [{ type: "mrkdwn", text: `🕐 Report generated: <!date^${Math.floor(Date.now() / 1000)}^{date_short_pretty} at {time}|${new Date().toUTCString()}>` }] },
      { type: "actions", elements: [{ type: "button", text: { type: "plain_text", text: "View Dashboard", emoji: true }, url: dashboardUrl, style: "primary" }] }
    ]
  };

  const response = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(slackPayload),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to send Slack report: ${response.status} - ${errorText}`);
  }
  return true;
}

/**
 * Fetch Enterprise zones from account
 */
async function fetchEnterpriseZones(apiKey, accountId) {
  const response = await fetch(
    `https://api.cloudflare.com/client/v4/zones?per_page=1000`,
    {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
    }
  );

  const data = await response.json();
  if (!response.ok || !data.result) {
    return [];
  }
  
  // Filter zones by account ID and Enterprise plan
  const accountZones = data.result.filter(zone => 
    zone.account && zone.account.id === accountId
  );
  
  const enterpriseZones = accountZones.filter(zone => 
    zone.plan?.legacy_id === 'enterprise' || 
    zone.plan?.name?.toLowerCase().includes('enterprise')
  );

  return enterpriseZones;
}

/**
 * Fetch Bot Management metrics for specific zones
 * Returns Likely Human requests (likely human traffic with bot score > 30)
 */
async function fetchBotManagementMetrics(apiKey, zoneId, dateStart, dateEnd) {
  const query = {
    operationName: 'GetBotTimeseries',
    variables: {
      zoneTag: zoneId,
      automatedFilter: {
        AND: [
          { requestSource: 'eyeball' },
          { botScore: 1 },
          { datetime_geq: dateStart },
          { datetime_leq: dateEnd },
          { botManagementDecision_neq: 'other' },
          { botScoreSrcName_neq: 'verified_bot' },
        ],
      },
      likelyAutomatedFilter: {
        AND: [
          { requestSource: 'eyeball' },
          { botScore_geq: 2, botScore_leq: 29 },
          { datetime_geq: dateStart },
          { datetime_leq: dateEnd },
          { botManagementDecision_neq: 'other' },
        ],
      },
      likelyHumanFilter: {
        AND: [
          { requestSource: 'eyeball' },
          { botScore_geq: 30, botScore_leq: 99 },
          { datetime_geq: dateStart },
          { datetime_leq: dateEnd },
          { botManagementDecision_neq: 'other' },
          { securitySource_neq: 'l7ddos' },
          { securityAction_neq: 'block' },
          { securityAction_neq: 'challenge_failed' },
          { securityAction_neq: 'jschallenge_failed' },
          { securityAction_neq: 'managed_challenge_failed' },
        ],
      },
      verifiedBotFilter: {
        AND: [
          { requestSource: 'eyeball' },
          { datetime_geq: dateStart },
          { datetime_leq: dateEnd },
          { botManagementDecision_neq: 'other' },
          { botScoreSrcName: 'verified_bot' },
        ],
      },
    },
    query: `query GetBotTimeseries($zoneTag: string, $automatedFilter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject, $likelyAutomatedFilter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject, $likelyHumanFilter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject, $verifiedBotFilter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject) {
      viewer {
        scope: zones(filter: {zoneTag: $zoneTag}) {
          likely_human_total: httpRequestsAdaptiveGroups(filter: {AND: [{botManagementDecision_neq: "verified_bot"}, $likelyHumanFilter]}, limit: 1) {
            count
            confidence(level: 0.95) {
              count {
                estimate
                lower
                upper
                sampleSize
              }
            }
          }
          automated: httpRequestsAdaptiveGroups(filter: {AND: [{botManagementDecision_neq: "verified_bot"}, $automatedFilter]}, limit: 10000) {
            dimensions {
              ts: date
              __typename
            }
            count
            avg {
              sampleInterval
              __typename
            }
            __typename
          }
          likely_automated: httpRequestsAdaptiveGroups(filter: {AND: [{botManagementDecision_neq: "verified_bot"}, $likelyAutomatedFilter]}, limit: 10000) {
            dimensions {
              ts: date
              __typename
            }
            count
            avg {
              sampleInterval
              __typename
            }
            __typename
          }
          likely_human: httpRequestsAdaptiveGroups(filter: {AND: [{botManagementDecision_neq: "verified_bot"}, $likelyHumanFilter]}, limit: 10000) {
            dimensions {
              ts: date
              __typename
            }
            count
            avg {
              sampleInterval
              __typename
            }
            confidence(level: 0.95) {
              count {
                estimate
                lower
                upper
                sampleSize
              }
            }
            __typename
          }
          verified_bot: httpRequestsAdaptiveGroups(filter: {AND: [{botManagementDecision: "verified_bot"}, $verifiedBotFilter]}, limit: 10000) {
            dimensions {
              ts: date
              __typename
            }
            count
            avg {
              sampleInterval
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
    }`,
  };

  const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
    },
    body: JSON.stringify(query),
  });

  const data = await response.json();

  if (!response.ok || data.errors) {
    console.error(`Failed to fetch bot management metrics for zone ${zoneId}:`, data.errors || data);
    return null;
  }

  // Extract Likely Human requests (likely_human)
  const scope = data.data?.viewer?.scope?.[0];
  if (!scope) {
    return null;
  }

  // Sum up all likely_human requests (bot score > 30 = Likely Human requests)
  const likelyHumanData = scope.likely_human || [];
  const likelyHuman = likelyHumanData.reduce((total, entry) => {
    return total + (entry.count || 0);
  }, 0);
  
  // Get confidence from aggregated total (not from time series)
  let confidence = null;
  const totalData = scope.likely_human_total?.[0];
  if (totalData?.confidence?.count) {
    confidence = {
      estimate: totalData.confidence.count.estimate || likelyHuman,
      lower: totalData.confidence.count.lower || likelyHuman,
      upper: totalData.confidence.count.upper || likelyHuman,
      sampleSize: totalData.confidence.count.sampleSize || 0
    };
  }

  return {
    zoneId,
    likelyHuman,
    confidence,
    automated: scope.automated?.reduce((total, entry) => total + (entry.count || 0), 0) || 0,
    likelyAutomated: scope.likely_automated?.reduce((total, entry) => total + (entry.count || 0), 0) || 0,
    verifiedBot: scope.verified_bot?.reduce((total, entry) => total + (entry.count || 0), 0) || 0,
  };
}

/**
 * Aggregate Bot Management metrics across multiple zones
 */
async function fetchBotManagementForAccount(apiKey, accountId, botManagementConfig, env) {
  if (!botManagementConfig || !botManagementConfig.enabled || !botManagementConfig.zones || botManagementConfig.zones.length === 0) {
    return null;
  }

  // Calculate date ranges
  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthEnd = now;
  
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59);

  // Date strings in ISO format
  const currentMonthStartISO = currentMonthStart.toISOString();
  const currentMonthEndISO = currentMonthEnd.toISOString();
  const previousMonthStartISO = previousMonthStart.toISOString();
  const previousMonthEndISO = previousMonthEnd.toISOString();

  // Get all enterprise zones to map IDs to names
  const allZones = await fetchEnterpriseZones(apiKey, accountId);
  const zoneMap = {};
  const accountZoneIds = new Set();
  allZones.forEach(zone => {
    zoneMap[zone.id] = zone.name;
    accountZoneIds.add(zone.id);
  });

  // Filter configured zones to only those that belong to this account
  const accountBotZones = botManagementConfig.zones.filter(zoneId => accountZoneIds.has(zoneId));
  
  // If no zones belong to this account, return null
  if (accountBotZones.length === 0) {
    return null;
  }

  // Fetch current month metrics for each configured zone IN THIS ACCOUNT
  const currentMonthPromises = accountBotZones.map(zoneId =>
    fetchBotManagementMetrics(apiKey, zoneId, currentMonthStartISO, currentMonthEndISO)
  );

  const currentMonthResults = await Promise.allSettled(currentMonthPromises);
  const currentMonthData = currentMonthResults
    .filter(result => result.status === 'fulfilled' && result.value)
    .map(result => result.value);

  // Fetch previous month metrics
  const previousMonthPromises = accountBotZones.map(zoneId =>
    fetchBotManagementMetrics(apiKey, zoneId, previousMonthStartISO, previousMonthEndISO)
  );

  const previousMonthResults = await Promise.allSettled(previousMonthPromises);
  const previousMonthData = previousMonthResults
    .filter(result => result.status === 'fulfilled' && result.value)
    .map(result => result.value);

  // Aggregate results
  const currentTotal = currentMonthData.reduce((sum, zone) => sum + zone.likelyHuman, 0);
  const previousTotal = previousMonthData.reduce((sum, zone) => sum + zone.likelyHuman, 0);
  const currentBotTraffic = currentMonthData.reduce((sum, zone) => sum + zone.automated + zone.likelyAutomated + zone.verifiedBot, 0);
  const currentTotalTraffic = currentMonthData.reduce((sum, zone) => sum + zone.likelyHuman + zone.automated + zone.likelyAutomated + zone.verifiedBot, 0);
  
  // Aggregate confidence from all zones
  const confidenceData = {
    estimates: [],
    lowers: [],
    uppers: [],
    sampleSizes: []
  };
  
  currentMonthData.forEach(zone => {
    if (zone.confidence) {
      confidenceData.estimates.push(zone.confidence.estimate);
      confidenceData.lowers.push(zone.confidence.lower);
      confidenceData.uppers.push(zone.confidence.upper);
      confidenceData.sampleSizes.push(zone.confidence.sampleSize);
    }
  });
  
  let aggregatedConfidence = null;
  if (confidenceData.estimates.length > 0) {
    aggregatedConfidence = {
      estimate: confidenceData.estimates.reduce((a, b) => a + b, 0),
      lower: confidenceData.lowers.reduce((a, b) => a + b, 0),
      upper: confidenceData.uppers.reduce((a, b) => a + b, 0),
      sampleSize: confidenceData.sampleSizes.reduce((a, b) => a + b, 0)
    };
  }

  // Build zone breakdown
  const zoneBreakdown = currentMonthData.map(zone => ({
    zoneId: zone.zoneId,
    zoneName: zoneMap[zone.zoneId] || zone.zoneId,
    likelyHuman: zone.likelyHuman,
    automated: zone.automated,
    likelyAutomated: zone.likelyAutomated,
    verifiedBot: zone.verifiedBot,
  }));
  const previousZoneBreakdown = previousMonthData.map(zone => ({
    zoneId: zone.zoneId,
    zoneName: zoneMap[zone.zoneId] || zone.zoneId,
    likelyHuman: zone.likelyHuman,
    automated: zone.automated,
    likelyAutomated: zone.likelyAutomated,
    verifiedBot: zone.verifiedBot,
  }));

  // Store previous month data in KV if we're past day 2 of current month
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;
  if (now.getDate() >= 2) {
    try {
      await env.CONFIG_KV.put(
        `monthly-bot-stats:${accountId}:${previousMonthKey}`,
        JSON.stringify({
          likelyHuman: previousTotal,
          zones: previousZoneBreakdown,
        }),
        { expirationTtl: 31536000 } // 1 year
      );
      console.log(`Stored Bot Management stats for ${previousMonthKey}`);
    } catch (error) {
      console.error('Failed to store Bot Management monthly stats:', error);
    }
  }

  // Get historical Bot Management data
  const historicalBotData = await getHistoricalBotManagementData(env, accountId);
  
  // Build timeSeries
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const timeSeriesData = [
    ...historicalBotData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      likelyHuman: currentTotal,
    }
  ];

  // Calculate confidence percentage
  const calculateConfidencePercentage = (confidence) => {
    if (!confidence || !confidence.estimate) return null;
    const estimate = confidence.estimate;
    const lower = confidence.lower || estimate;
    const upper = confidence.upper || estimate;
    const intervalWidth = upper - lower;
    const relativeWidth = intervalWidth / (2 * estimate);
    const confidencePercent = Math.max(0, Math.min(100, 100 * (1 - relativeWidth)));
    return {
      percent: Math.round(confidencePercent * 10) / 10,
      sampleSize: confidence.sampleSize,
      estimate: confidence.estimate,
      lower: confidence.lower,
      upper: confidence.upper
    };
  };

  return {
    enabled: true,
    threshold: botManagementConfig.threshold || null,
    current: {
      likelyHuman: currentTotal,
      botTraffic: currentBotTraffic,
      totalTraffic: currentTotalTraffic,
      zones: zoneBreakdown,
      confidence: aggregatedConfidence ? calculateConfidencePercentage(aggregatedConfidence) : null,
    },
    previous: {
      likelyHuman: previousTotal,
      zones: previousZoneBreakdown,
    },
    timeSeries: timeSeriesData,
  };
}

/**
 * Get all historical Bot Management data from KV (cached for 6 hours)
 */
async function getHistoricalBotManagementData(env, accountId) {
  // Check cache first (6 hour TTL)
  const cacheKey = `historical-bot-data:${accountId}`;
  const cached = await env.CONFIG_KV.get(cacheKey, 'json');
  
  if (cached && cached.cachedAt) {
    const cacheAge = Date.now() - cached.cachedAt;
    if (cacheAge < 6 * 60 * 60 * 1000) { // 6 hours
      console.log(`Using cached historical Bot Management data for account ${accountId} (age: ${Math.floor(cacheAge / 3600000)}h)`);
      return cached.data;
    }
  }
  
  const historicalData = [];
  
  // List all monthly-bot-stats keys for this account
  const listResult = await env.CONFIG_KV.list({ prefix: `monthly-bot-stats:${accountId}:` });
  
  for (const key of listResult.keys) {
    const data = await env.CONFIG_KV.get(key.name, 'json');
    if (data) {
      // Extract month from key: monthly-bot-stats:{accountId}:YYYY-MM
      const month = key.name.split(':')[2];
      const [year, monthNum] = month.split('-');
      const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
      
      historicalData.push({
        month,
        timestamp,
        likelyHuman: data.likelyHuman || 0,
      });
    }
  }
  
  // Cache the historical data (6 hour TTL)
  try {
    await env.CONFIG_KV.put(
      cacheKey,
      JSON.stringify({
        cachedAt: Date.now(),
        data: historicalData
      }),
      { expirationTtl: 21600 } // 6 hours
    );
    console.log(`Cached historical Bot Management data for account ${accountId}`);
  } catch (cacheError) {
    console.error('Failed to cache historical Bot Management data:', cacheError);
  }
  
  return historicalData;
}

/**
 * Fetch Zero Trust Seats for an account
 * Returns current seat count (account-level metric, no zones involved)
 */
async function fetchZeroTrustSeatsForAccount(apiKey, accountId, seatsConfig, env) {
  if (!seatsConfig || !seatsConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  // Fetch current seat count from Access Users API
  let currentSeats = 0;
  try {
    const response = await fetch(
      `https://api.cloudflare.com/client/v4/accounts/${accountId}/access/users?seat_type=any&per_page=1`,
      {
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
      }
    );

    if (response.ok) {
      const data = await response.json();
      currentSeats = data.result_info?.total_count || 0;
      console.log(`Zero Trust Seats for account ${accountId}: ${currentSeats}`);
    } else {
      console.error(`Failed to fetch Zero Trust seats for account ${accountId}: ${response.status}`);
      return null;
    }
  } catch (error) {
    console.error(`Error fetching Zero Trust seats for account ${accountId}:`, error);
    return null;
  }

  // Get cached previous month data
  const previousMonthCacheKey = `monthly-zt-seats:${accountId}:${previousMonthKey}`;
  let previousSeats = 0;
  
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');
  if (cachedPreviousMonth) {
    previousSeats = cachedPreviousMonth.seats || 0;
  }

  // Cache current month snapshot at end of month (day >= 28)
  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-zt-seats:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ seats: currentSeats, cachedAt: Date.now() }),
          { expirationTtl: 31536000 } // 1 year
        );
        console.log(`Cached Zero Trust seats snapshot for ${currentMonthKey}: ${currentSeats}`);
      } catch (cacheError) {
        console.error('Failed to cache Zero Trust seats snapshot:', cacheError);
      }
    }
  }

  // Load historical data for time series
  const historicalData = await getHistoricalZeroTrustSeatsData(env, accountId);

  // Build time series
  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: new Date(now.getFullYear(), now.getMonth(), 1).toISOString(),
      seats: currentSeats,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  // Deduplicate by month (keep latest)
  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { seats: currentSeats },
    previous: { seats: previousSeats },
    timeSeries: deduplicatedTimeSeries,
  };
}

/**
 * Get historical Zero Trust seats data from KV
 */
async function getHistoricalZeroTrustSeatsData(env, accountId) {
  const historicalData = [];
  
  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-zt-seats:${accountId}:` });
    
    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
        
        historicalData.push({
          month,
          timestamp,
          seats: data.seats || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical Zero Trust seats data:', error);
  }
  
  return historicalData;
}

/**
 * Fetch Workers & Pages metrics for an account
 * Returns requests and CPU time (account-level metrics)
 */
async function fetchWorkersPagesForAccount(apiKey, accountId, workersPagesConfig, env) {
  if (!workersPagesConfig || !workersPagesConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  // GraphQL query for Workers & Pages metrics
  const query = `
    query getWorkersAndPagesMetrics($accountTag: string!, $monthlyFilter: AccountWorkersInvocationsAdaptiveFilter_InputObject, $monthlyOverviewFilter: AccountWorkersOverviewRequestsAdaptiveGroupsFilter_InputObject) {
      viewer {
        accounts(filter: {accountTag: $accountTag}) {
          monthlyPagesFunctionsInvocationsAdaptiveGroups: pagesFunctionsInvocationsAdaptiveGroups(limit: 1000, filter: $monthlyFilter) {
            sum {
              requests
            }
            dimensions {
              usageModel
            }
          }
          monthlyWorkersInvocationsAdaptive: workersInvocationsAdaptive(limit: 10000, filter: $monthlyFilter) {
            sum {
              requests
            }
            dimensions {
              usageModel
            }
            confidence(level: 0.95) {
              sum {
                requests {
                  estimate
                  lower
                  upper
                  sampleSize
                }
              }
            }
          }
          monthlyWorkersOverviewRequestsAdaptiveGroups: workersOverviewRequestsAdaptiveGroups(limit: 1000, filter: $monthlyOverviewFilter) {
            sum {
              cpuTimeUs
            }
            dimensions {
              usageModel
            }
          }
        }
      }
    }
  `;

  let currentRequests = 0;
  let currentCpuTimeMs = 0;

  try {
    // Format dates for current month
    const monthlyFilter = {
      date_geq: currentMonthStart.toISOString().split('T')[0],
      date_leq: now.toISOString().split('T')[0],
    };
    const monthlyOverviewFilter = {
      datetime_geq: currentMonthStart.toISOString(),
      datetime_leq: now.toISOString(),
    };

    const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query,
        variables: {
          accountTag: accountId,
          monthlyFilter,
          monthlyOverviewFilter,
        },
      }),
    });

    if (!response.ok) {
      console.error(`Workers & Pages GraphQL failed for account ${accountId}: ${response.status}`);
      return null;
    }

    const data = await response.json();
    
    if (data.errors) {
      console.error(`Workers & Pages GraphQL errors for account ${accountId}:`, data.errors);
      return null;
    }

    const account = data.data?.viewer?.accounts?.[0];
    if (!account) {
      console.log(`No Workers & Pages data for account ${accountId}`);
      return { current: { requests: 0, cpuTimeMs: 0 }, previous: { requests: 0, cpuTimeMs: 0 }, timeSeries: [] };
    }

    // Sum requests from Workers and Pages Functions
    const workersRequests = account.monthlyWorkersInvocationsAdaptive?.reduce(
      (sum, entry) => sum + (entry.sum?.requests || 0), 0
    ) || 0;
    const pagesRequests = account.monthlyPagesFunctionsInvocationsAdaptiveGroups?.reduce(
      (sum, entry) => sum + (entry.sum?.requests || 0), 0
    ) || 0;
    currentRequests = workersRequests + pagesRequests;

    // Sum CPU time (convert from microseconds to milliseconds)
    const cpuTimeUs = account.monthlyWorkersOverviewRequestsAdaptiveGroups?.reduce(
      (sum, entry) => sum + (entry.sum?.cpuTimeUs || 0), 0
    ) || 0;
    currentCpuTimeMs = cpuTimeUs / 1000; // Convert to milliseconds

    const workersConfData = account.monthlyWorkersInvocationsAdaptive?.reduce((agg, entry) => {
      const conf = entry.confidence?.sum?.requests;
      if (conf) {
        agg.estimate += conf.estimate || 0;
        agg.lower += conf.lower || 0;
        agg.upper += conf.upper || 0;
        agg.sampleSize += conf.sampleSize || 0;
      }
      return agg;
    }, { estimate: 0, lower: 0, upper: 0, sampleSize: 0 });

    var currentConfidence = null;
    if (workersConfData && workersConfData.estimate > 0) {
      const intervalWidth = workersConfData.upper - workersConfData.lower;
      const relativeWidth = intervalWidth / (2 * workersConfData.estimate);
      const confidencePercent = Math.max(0, Math.min(100, 100 * (1 - relativeWidth)));
      currentConfidence = {
        percent: Math.round(confidencePercent * 10) / 10,
        sampleSize: workersConfData.sampleSize,
        estimate: workersConfData.estimate,
        lower: workersConfData.lower,
        upper: workersConfData.upper,
      };
    }

    console.log(`Workers & Pages for account ${accountId}: ${currentRequests.toLocaleString()} requests, ${currentCpuTimeMs.toLocaleString()} ms CPU time${currentConfidence ? `, confidence: ${currentConfidence.percent}%` : ''}`);

  } catch (error) {
    console.error(`Error fetching Workers & Pages for account ${accountId}:`, error);
    return null;
  }

  // Get cached previous month data
  const previousMonthCacheKey = `monthly-workers-pages:${accountId}:${previousMonthKey}`;
  let previousRequests = 0;
  let previousCpuTimeMs = 0;
  
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');
  if (cachedPreviousMonth) {
    previousRequests = cachedPreviousMonth.requests || 0;
    previousCpuTimeMs = cachedPreviousMonth.cpuTimeMs || 0;
    console.log(`Workers & Pages previous month from cache: ${previousRequests.toLocaleString()} requests, ${previousCpuTimeMs.toLocaleString()} ms`);
  } else if (now.getDate() >= 2) {
    // Fetch previous month data from API
    try {
      const prevMonthlyFilter = {
        date_geq: previousMonthStart.toISOString().split('T')[0],
        date_leq: previousMonthEnd.toISOString().split('T')[0],
      };
      const prevMonthlyOverviewFilter = {
        datetime_geq: previousMonthStart.toISOString(),
        datetime_leq: previousMonthEnd.toISOString(),
      };

      const prevResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query,
          variables: {
            accountTag: accountId,
            monthlyFilter: prevMonthlyFilter,
            monthlyOverviewFilter: prevMonthlyOverviewFilter,
          },
        }),
      });

      if (prevResponse.ok) {
        const prevData = await prevResponse.json();
        const prevAccount = prevData.data?.viewer?.accounts?.[0];
        
        if (prevAccount) {
          const prevWorkersRequests = prevAccount.monthlyWorkersInvocationsAdaptive?.reduce(
            (sum, entry) => sum + (entry.sum?.requests || 0), 0
          ) || 0;
          const prevPagesRequests = prevAccount.monthlyPagesFunctionsInvocationsAdaptiveGroups?.reduce(
            (sum, entry) => sum + (entry.sum?.requests || 0), 0
          ) || 0;
          previousRequests = prevWorkersRequests + prevPagesRequests;

          const prevCpuTimeUs = prevAccount.monthlyWorkersOverviewRequestsAdaptiveGroups?.reduce(
            (sum, entry) => sum + (entry.sum?.cpuTimeUs || 0), 0
          ) || 0;
          previousCpuTimeMs = prevCpuTimeUs / 1000;

          // Cache the previous month data
          await env.CONFIG_KV.put(
            previousMonthCacheKey,
            JSON.stringify({ requests: previousRequests, cpuTimeMs: previousCpuTimeMs, cachedAt: Date.now() }),
            { expirationTtl: 31536000 } // 1 year
          );
          console.log(`Workers & Pages previous month cached: ${previousRequests.toLocaleString()} requests`);
        }
      }
    } catch (prevError) {
      console.error(`Failed to fetch previous month Workers & Pages:`, prevError);
    }
  }

  // Cache current month snapshot at end of month (day >= 28)
  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-workers-pages:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ requests: currentRequests, cpuTimeMs: currentCpuTimeMs, cachedAt: Date.now() }),
          { expirationTtl: 31536000 } // 1 year
        );
        console.log(`Cached Workers & Pages snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache Workers & Pages snapshot:', cacheError);
      }
    }
  }

  // Load historical data for time series
  const historicalData = await getHistoricalWorkersPagesData(env, accountId);

  // Build time series
  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      requests: currentRequests,
      cpuTimeMs: currentCpuTimeMs,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  // Deduplicate by month (keep latest)
  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { requests: currentRequests, cpuTimeMs: currentCpuTimeMs, confidence: currentConfidence },
    previous: { requests: previousRequests, cpuTimeMs: previousCpuTimeMs },
    timeSeries: deduplicatedTimeSeries,
  };
}

/**
 * Get historical Workers & Pages data from KV
 */
async function getHistoricalWorkersPagesData(env, accountId) {
  const historicalData = [];
  
  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-workers-pages:${accountId}:` });
    
    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
        
        historicalData.push({
          month,
          timestamp,
          requests: data.requests || 0,
          cpuTimeMs: data.cpuTimeMs || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical Workers & Pages data:', error);
  }
  
  return historicalData;
}

/**
 * Fetch R2 Storage metrics for an account
 * Returns Class A ops, Class B ops, and total storage (account-level metrics)
 */
async function fetchR2StorageForAccount(apiKey, accountId, r2Config, env) {
  if (!r2Config || !r2Config.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const query = `
    query getR2Storage($accountTag: string!, $storageFilter: AccountR2StorageAdaptiveGroupsFilter_InputObject, $classAOpsFilter: AccountR2OperationsAdaptiveGroupsFilter_InputObject, $classBOpsFilter: AccountR2OperationsAdaptiveGroupsFilter_InputObject) {
      viewer {
        accounts(filter: {accountTag: $accountTag}) {
          r2StorageAdaptiveGroups(limit: 10000, orderBy: [date_DESC], filter: $storageFilter) {
            max {
              payloadSize
              metadataSize
            }
            dimensions {
              date
            }
          }
          classAOps: r2OperationsAdaptiveGroups(limit: 10000, filter: $classAOpsFilter) {
            sum {
              requests
            }
            dimensions {
              date
            }
          }
          classBOps: r2OperationsAdaptiveGroups(limit: 10000, filter: $classBOpsFilter) {
            sum {
              requests
            }
            dimensions {
              date
            }
          }
        }
      }
    }
  `;

  let currentClassAOps = 0;
  let currentClassBOps = 0;
  let currentStorageBytes = 0;

  try {
    const dateStart = currentMonthStart.toISOString().split('T')[0];
    const dateEnd = now.toISOString().split('T')[0];

    const storageFilter = {
      date_geq: dateStart,
      date_leq: dateEnd,
    };
    const classAOpsFilter = {
      date_geq: dateStart,
      date_leq: dateEnd,
      actionType_in: ['ListBuckets', 'PutBucket', 'ListObjects', 'PutObject', 'CopyObject', 'CompleteMultipartUpload', 'CreateMultipartUpload', 'UploadPart', 'UploadPartCopy', 'PutBucketEncryption', 'PutBucketCors', 'PutBucketLifecycleConfiguration'],
    };
    const classBOpsFilter = {
      date_geq: dateStart,
      date_leq: dateEnd,
      actionType_in: ['HeadBucket', 'HeadObject', 'GetObject', 'ReportUsageSummary', 'GetBucketEncryption', 'GetBucketLocation', 'GetBucketCors', 'GetBucketLifecycleConfiguration'],
    };

    const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query,
        variables: {
          accountTag: accountId,
          storageFilter,
          classAOpsFilter,
          classBOpsFilter,
        },
      }),
    });

    if (!response.ok) {
      console.error(`R2 Storage GraphQL failed for account ${accountId}: ${response.status}`);
      return null;
    }

    const data = await response.json();
    
    if (data.errors) {
      console.error(`R2 Storage GraphQL errors for account ${accountId}:`, data.errors);
      return null;
    }

    const account = data.data?.viewer?.accounts?.[0];
    if (!account) {
      console.log(`No R2 Storage data for account ${accountId}`);
      return { current: { classAOps: 0, classBOps: 0, storageGB: 0 }, previous: { classAOps: 0, classBOps: 0, storageGB: 0 }, timeSeries: [] };
    }

    currentClassAOps = account.classAOps?.reduce(
      (sum, entry) => sum + (entry.sum?.requests || 0), 0
    ) || 0;

    currentClassBOps = account.classBOps?.reduce(
      (sum, entry) => sum + (entry.sum?.requests || 0), 0
    ) || 0;

    const storageData = account.r2StorageAdaptiveGroups || [];
    if (storageData.length > 0) {
      const latestStorage = storageData[0];
      currentStorageBytes = (latestStorage.max?.payloadSize || 0) + (latestStorage.max?.metadataSize || 0);
    }

    const currentStorageGB = currentStorageBytes / (1024 * 1024 * 1024);

    console.log(`R2 Storage for account ${accountId}: ${currentClassAOps.toLocaleString()} Class A ops, ${currentClassBOps.toLocaleString()} Class B ops, ${currentStorageGB.toFixed(2)} GB`);

  } catch (error) {
    console.error(`Error fetching R2 Storage for account ${accountId}:`, error);
    return null;
  }

  const currentStorageGB = currentStorageBytes / (1024 * 1024 * 1024);

  let previousClassAOps = 0;
  let previousClassBOps = 0;
  let previousStorageGB = 0;
  
  const previousMonthCacheKey = `monthly-r2-storage:${accountId}:${previousMonthKey}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');
  
  if (cachedPreviousMonth) {
    previousClassAOps = cachedPreviousMonth.classAOps || 0;
    previousClassBOps = cachedPreviousMonth.classBOps || 0;
    previousStorageGB = cachedPreviousMonth.storageGB || 0;
    console.log(`R2 Storage previous month from cache: ${previousClassAOps.toLocaleString()} Class A, ${previousClassBOps.toLocaleString()} Class B, ${previousStorageGB.toFixed(2)} GB`);
  } else if (now.getDate() >= 2) {
    try {
      const prevDateStart = previousMonthStart.toISOString().split('T')[0];
      const prevDateEnd = previousMonthEnd.toISOString().split('T')[0];

      const prevStorageFilter = {
        date_geq: prevDateStart,
        date_leq: prevDateEnd,
      };
      const prevClassAOpsFilter = {
        date_geq: prevDateStart,
        date_leq: prevDateEnd,
        actionType_in: ['ListBuckets', 'PutBucket', 'ListObjects', 'PutObject', 'CopyObject', 'CompleteMultipartUpload', 'CreateMultipartUpload', 'UploadPart', 'UploadPartCopy', 'PutBucketEncryption', 'PutBucketCors', 'PutBucketLifecycleConfiguration'],
      };
      const prevClassBOpsFilter = {
        date_geq: prevDateStart,
        date_leq: prevDateEnd,
        actionType_in: ['HeadBucket', 'HeadObject', 'GetObject', 'ReportUsageSummary', 'GetBucketEncryption', 'GetBucketLocation', 'GetBucketCors', 'GetBucketLifecycleConfiguration'],
      };

      const prevResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query,
          variables: {
            accountTag: accountId,
            storageFilter: prevStorageFilter,
            classAOpsFilter: prevClassAOpsFilter,
            classBOpsFilter: prevClassBOpsFilter,
          },
        }),
      });

      if (prevResponse.ok) {
        const prevData = await prevResponse.json();
        const prevAccount = prevData.data?.viewer?.accounts?.[0];
        
        if (prevAccount) {
          previousClassAOps = prevAccount.classAOps?.reduce(
            (sum, entry) => sum + (entry.sum?.requests || 0), 0
          ) || 0;
          previousClassBOps = prevAccount.classBOps?.reduce(
            (sum, entry) => sum + (entry.sum?.requests || 0), 0
          ) || 0;
          
          const prevStorageData = prevAccount.r2StorageAdaptiveGroups || [];
          if (prevStorageData.length > 0) {
            const prevLatestStorage = prevStorageData[0];
            const prevStorageBytes = (prevLatestStorage.max?.payloadSize || 0) + (prevLatestStorage.max?.metadataSize || 0);
            previousStorageGB = prevStorageBytes / (1024 * 1024 * 1024);
          }

          await env.CONFIG_KV.put(
            previousMonthCacheKey,
            JSON.stringify({ classAOps: previousClassAOps, classBOps: previousClassBOps, storageGB: previousStorageGB, cachedAt: Date.now() }),
            { expirationTtl: 31536000 }
          );
          console.log(`R2 Storage previous month cached: ${previousClassAOps.toLocaleString()} Class A ops`);
        }
      }
    } catch (prevError) {
      console.error(`Failed to fetch previous month R2 Storage:`, prevError);
    }
  }

  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-r2-storage:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ classAOps: currentClassAOps, classBOps: currentClassBOps, storageGB: currentStorageGB, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached R2 Storage snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache R2 Storage snapshot:', cacheError);
      }
    }
  }

  const historicalData = await getHistoricalR2StorageData(env, accountId);

  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      classAOps: currentClassAOps,
      classBOps: currentClassBOps,
      storageGB: currentStorageGB,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { classAOps: currentClassAOps, classBOps: currentClassBOps, storageGB: currentStorageGB },
    previous: { classAOps: previousClassAOps, classBOps: previousClassBOps, storageGB: previousStorageGB },
    timeSeries: deduplicatedTimeSeries,
  };
}

async function getHistoricalR2StorageData(env, accountId) {
  const historicalData = [];
  
  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-r2-storage:${accountId}:` });
    
    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
        
        historicalData.push({
          month,
          timestamp,
          classAOps: data.classAOps || 0,
          classBOps: data.classBOps || 0,
          storageGB: data.storageGB || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical R2 Storage data:', error);
  }
  
  return historicalData;
}

/**
 * Fetch D1 Database metrics for an account
 * Returns rows read, rows written, and storage (account-level metrics)
 */
async function fetchD1ForAccount(apiKey, accountId, d1Config, env) {
  if (!d1Config || !d1Config.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const analyticsQuery = `
    query getD1Analytics($accountTag: string!, $filter: AccountD1AnalyticsAdaptiveGroupsFilter_InputObject) {
      viewer {
        accounts(filter: {accountTag: $accountTag}) {
          d1AnalyticsAdaptiveGroups(limit: 10000, filter: $filter) {
            sum {
              rowsRead
              rowsWritten
            }
          }
        }
      }
    }
  `;

  const storageQuery = `
    query getD1Storage($accountTag: string!, $filter: AccountD1StorageAdaptiveGroupsFilter_InputObject) {
      viewer {
        accounts(filter: {accountTag: $accountTag}) {
          d1StorageAdaptiveGroups(limit: 10000, filter: $filter) {
            max {
              databaseSizeBytes
            }
            dimensions {
              databaseId
            }
          }
        }
      }
    }
  `;

  let currentRowsRead = 0;
  let currentRowsWritten = 0;
  let currentStorageMB = 0;

  try {
    const dateStart = currentMonthStart.toISOString().split('T')[0];
    const dateEnd = now.toISOString().split('T')[0];

    const analyticsFilter = {
      date_geq: dateStart,
      date_leq: dateEnd,
    };

    const [analyticsResponse, storageResponse] = await Promise.all([
      fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: analyticsQuery,
          variables: { accountTag: accountId, filter: analyticsFilter },
        }),
      }),
      fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: storageQuery,
          variables: { accountTag: accountId, filter: { date: dateEnd } },
        }),
      }),
    ]);

    if (!analyticsResponse.ok || !storageResponse.ok) {
      console.error(`D1 GraphQL failed for account ${accountId}`);
      return null;
    }

    const [analyticsData, storageData] = await Promise.all([
      analyticsResponse.json(),
      storageResponse.json(),
    ]);

    if (analyticsData.errors) {
      console.error(`D1 Analytics GraphQL errors for account ${accountId}:`, analyticsData.errors);
    }
    if (storageData.errors) {
      console.error(`D1 Storage GraphQL errors for account ${accountId}:`, storageData.errors);
    }

    const analyticsAccount = analyticsData.data?.viewer?.accounts?.[0];
    if (analyticsAccount?.d1AnalyticsAdaptiveGroups) {
      for (const entry of analyticsAccount.d1AnalyticsAdaptiveGroups) {
        currentRowsRead += entry.sum?.rowsRead || 0;
        currentRowsWritten += entry.sum?.rowsWritten || 0;
      }
    }

    const storageAccount = storageData.data?.viewer?.accounts?.[0];
    if (storageAccount?.d1StorageAdaptiveGroups) {
      let totalStorageBytes = 0;
      for (const entry of storageAccount.d1StorageAdaptiveGroups) {
        totalStorageBytes += entry.max?.databaseSizeBytes || 0;
      }
      currentStorageMB = totalStorageBytes / 1000000;
    }

    console.log(`D1 for account ${accountId}: ${currentRowsRead.toLocaleString()} rows read, ${currentRowsWritten.toLocaleString()} rows written, ${currentStorageMB.toFixed(2)} MB`);

  } catch (error) {
    console.error(`Error fetching D1 for account ${accountId}:`, error);
    return null;
  }

  let previousRowsRead = 0;
  let previousRowsWritten = 0;
  let previousStorageMB = 0;

  const previousMonthCacheKey = `monthly-d1:${accountId}:${previousMonthKey}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');

  if (cachedPreviousMonth) {
    previousRowsRead = cachedPreviousMonth.rowsRead || 0;
    previousRowsWritten = cachedPreviousMonth.rowsWritten || 0;
    previousStorageMB = cachedPreviousMonth.storageMB || 0;
    console.log(`D1 previous month from cache: ${previousRowsRead.toLocaleString()} rows read`);
  } else if (now.getDate() >= 2) {
    try {
      const prevDateStart = previousMonthStart.toISOString().split('T')[0];
      const prevDateEnd = previousMonthEnd.toISOString().split('T')[0];

      const prevAnalyticsFilter = {
        date_geq: prevDateStart,
        date_leq: prevDateEnd,
      };

      const prevResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: analyticsQuery,
          variables: { accountTag: accountId, filter: prevAnalyticsFilter },
        }),
      });

      if (prevResponse.ok) {
        const prevData = await prevResponse.json();
        const prevAccount = prevData.data?.viewer?.accounts?.[0];

        if (prevAccount?.d1AnalyticsAdaptiveGroups) {
          for (const entry of prevAccount.d1AnalyticsAdaptiveGroups) {
            previousRowsRead += entry.sum?.rowsRead || 0;
            previousRowsWritten += entry.sum?.rowsWritten || 0;
          }

          await env.CONFIG_KV.put(
            previousMonthCacheKey,
            JSON.stringify({ rowsRead: previousRowsRead, rowsWritten: previousRowsWritten, storageMB: previousStorageMB, cachedAt: Date.now() }),
            { expirationTtl: 31536000 }
          );
          console.log(`D1 previous month cached: ${previousRowsRead.toLocaleString()} rows read`);
        }
      }
    } catch (prevError) {
      console.error(`Failed to fetch previous month D1:`, prevError);
    }
  }

  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-d1:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ rowsRead: currentRowsRead, rowsWritten: currentRowsWritten, storageMB: currentStorageMB, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached D1 snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache D1 snapshot:', cacheError);
      }
    }
  }

  const historicalData = await getHistoricalD1Data(env, accountId);

  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      rowsRead: currentRowsRead,
      rowsWritten: currentRowsWritten,
      storageMB: currentStorageMB,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { rowsRead: currentRowsRead, rowsWritten: currentRowsWritten, storageMB: currentStorageMB },
    previous: { rowsRead: previousRowsRead, rowsWritten: previousRowsWritten, storageMB: previousStorageMB },
    timeSeries: deduplicatedTimeSeries,
  };
}

async function getHistoricalD1Data(env, accountId) {
  const historicalData = [];

  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-d1:${accountId}:` });

    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();

        historicalData.push({
          month,
          timestamp,
          rowsRead: data.rowsRead || 0,
          rowsWritten: data.rowsWritten || 0,
          storageMB: data.storageMB || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical D1 data:', error);
  }

  return historicalData;
}

/**
 * Fetch KV metrics for an account
 * Returns keys read, keys written, keys deleted, list requests, and storage (account-level metrics)
 */
async function fetchKVForAccount(apiKey, accountId, kvConfig, env) {
  if (!kvConfig || !kvConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const operationsQuery = `
    query getKVOperations($accountTag: string!, $start: Date, $end: Date) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          kvOperationsAdaptiveGroups(
            filter: { date_geq: $start, date_leq: $end }
            limit: 10000
          ) {
            sum { requests }
            dimensions { actionType }
          }
        }
      }
    }
  `;

  const storageQuery = `
    query getKVStorage($accountTag: string!, $start: Date, $end: Date) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          kvStorageAdaptiveGroups(
            filter: { date_geq: $start, date_leq: $end }
            limit: 10000
            orderBy: [date_DESC]
          ) {
            max { keyCount byteCount }
            dimensions { date namespaceId }
          }
        }
      }
    }
  `;

  let currentReads = 0;
  let currentWrites = 0;
  let currentDeletes = 0;
  let currentLists = 0;
  let currentStorageMB = 0;

  try {
    const dateStart = currentMonthStart.toISOString().split('T')[0];
    const dateEnd = now.toISOString().split('T')[0];

    const [opsResponse, storageResponse] = await Promise.all([
      fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: operationsQuery,
          variables: { accountTag: accountId, start: dateStart, end: dateEnd },
        }),
      }),
      fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: storageQuery,
          variables: { accountTag: accountId, start: dateEnd, end: dateEnd },
        }),
      }),
    ]);

    if (!opsResponse.ok || !storageResponse.ok) {
      console.error(`KV GraphQL failed for account ${accountId}`);
      return null;
    }

    const [opsData, storageData] = await Promise.all([
      opsResponse.json(),
      storageResponse.json(),
    ]);

    if (opsData.errors) {
      console.error(`KV Operations GraphQL errors for account ${accountId}:`, opsData.errors);
    }
    if (storageData.errors) {
      console.error(`KV Storage GraphQL errors for account ${accountId}:`, storageData.errors);
    }

    const opsAccount = opsData.data?.viewer?.accounts?.[0];
    if (opsAccount?.kvOperationsAdaptiveGroups) {
      for (const entry of opsAccount.kvOperationsAdaptiveGroups) {
        const actionType = entry.dimensions?.actionType;
        const requests = entry.sum?.requests || 0;
        if (actionType === 'read') currentReads += requests;
        else if (actionType === 'write') currentWrites += requests;
        else if (actionType === 'delete') currentDeletes += requests;
        else if (actionType === 'list') currentLists += requests;
      }
    }

    const storageAccount = storageData.data?.viewer?.accounts?.[0];
    if (storageAccount?.kvStorageAdaptiveGroups && storageAccount.kvStorageAdaptiveGroups.length > 0) {
      let totalBytes = 0;
      for (const entry of storageAccount.kvStorageAdaptiveGroups) {
        totalBytes += entry.max?.byteCount || 0;
      }
      currentStorageMB = totalBytes / 1000000;
    }

    console.log(`KV for account ${accountId}: ${currentReads.toLocaleString()} reads, ${currentWrites.toLocaleString()} writes, ${currentDeletes.toLocaleString()} deletes, ${currentLists.toLocaleString()} lists, ${currentStorageMB.toFixed(2)} MB`);

  } catch (error) {
    console.error(`Error fetching KV for account ${accountId}:`, error);
    return null;
  }

  let previousReads = 0;
  let previousWrites = 0;
  let previousDeletes = 0;
  let previousLists = 0;
  let previousStorageMB = 0;

  const previousMonthCacheKey = `monthly-kv:${accountId}:${previousMonthKey}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');

  if (cachedPreviousMonth) {
    previousReads = cachedPreviousMonth.reads || 0;
    previousWrites = cachedPreviousMonth.writes || 0;
    previousDeletes = cachedPreviousMonth.deletes || 0;
    previousLists = cachedPreviousMonth.lists || 0;
    previousStorageMB = cachedPreviousMonth.storageMB || 0;
    console.log(`KV previous month from cache: ${previousReads.toLocaleString()} reads`);
  } else if (now.getDate() >= 2) {
    try {
      const prevDateStart = previousMonthStart.toISOString().split('T')[0];
      const prevDateEnd = previousMonthEnd.toISOString().split('T')[0];

      const prevResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: operationsQuery,
          variables: { accountTag: accountId, start: prevDateStart, end: prevDateEnd },
        }),
      });

      if (prevResponse.ok) {
        const prevData = await prevResponse.json();
        const prevAccount = prevData.data?.viewer?.accounts?.[0];

        if (prevAccount?.kvOperationsAdaptiveGroups) {
          for (const entry of prevAccount.kvOperationsAdaptiveGroups) {
            const actionType = entry.dimensions?.actionType;
            const requests = entry.sum?.requests || 0;
            if (actionType === 'read') previousReads += requests;
            else if (actionType === 'write') previousWrites += requests;
            else if (actionType === 'delete') previousDeletes += requests;
            else if (actionType === 'list') previousLists += requests;
          }

          await env.CONFIG_KV.put(
            previousMonthCacheKey,
            JSON.stringify({ reads: previousReads, writes: previousWrites, deletes: previousDeletes, lists: previousLists, storageMB: previousStorageMB, cachedAt: Date.now() }),
            { expirationTtl: 31536000 }
          );
          console.log(`KV previous month cached: ${previousReads.toLocaleString()} reads`);
        }
      }
    } catch (prevError) {
      console.error(`Failed to fetch previous month KV:`, prevError);
    }
  }

  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-kv:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ reads: currentReads, writes: currentWrites, deletes: currentDeletes, lists: currentLists, storageMB: currentStorageMB, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached KV snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache KV snapshot:', cacheError);
      }
    }
  }

  const historicalData = await getHistoricalKVData(env, accountId);

  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      reads: currentReads,
      writes: currentWrites,
      deletes: currentDeletes,
      lists: currentLists,
      storageMB: currentStorageMB,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { reads: currentReads, writes: currentWrites, deletes: currentDeletes, lists: currentLists, storageMB: currentStorageMB },
    previous: { reads: previousReads, writes: previousWrites, deletes: previousDeletes, lists: previousLists, storageMB: previousStorageMB },
    timeSeries: deduplicatedTimeSeries,
  };
}

async function getHistoricalKVData(env, accountId) {
  const historicalData = [];

  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-kv:${accountId}:` });

    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();

        historicalData.push({
          month,
          timestamp,
          reads: data.reads || 0,
          writes: data.writes || 0,
          deletes: data.deletes || 0,
          lists: data.lists || 0,
          storageMB: data.storageMB || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical KV data:', error);
  }

  return historicalData;
}

/**
 * Fetch Stream metrics for an account
 * Returns minutes of video stored (REST API) and minutes of video delivered (GraphQL)
 */
async function fetchStreamForAccount(apiKey, accountId, streamConfig, env) {
  if (!streamConfig || !streamConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const minutesDeliveredQuery = `
    query getStreamMinutesDelivered($accountTag: string!, $start: Date, $end: Date) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          streamMinutesViewedAdaptiveGroups(
            filter: { date_geq: $start, date_lt: $end }
            limit: 10000
          ) {
            sum { minutesViewed }
          }
        }
      }
    }
  `;

  let currentMinutesStored = 0;
  let currentMinutesDelivered = 0;

  try {
    const dateStart = currentMonthStart.toISOString().split('T')[0];
    const dateEnd = now.toISOString().split('T')[0];

    const [storageResponse, deliveredResponse] = await Promise.all([
      fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/stream/storage-usage`, {
        headers: { 'Authorization': `Bearer ${apiKey}` },
      }),
      fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: minutesDeliveredQuery,
          variables: { accountTag: accountId, start: dateStart, end: dateEnd },
        }),
      }),
    ]);

    if (storageResponse.ok) {
      const storageData = await storageResponse.json();
      if (storageData.success && storageData.result) {
        currentMinutesStored = storageData.result.totalStorageMinutes || 0;
      }
    } else {
      console.error(`Stream storage API failed for account ${accountId}: ${storageResponse.status}`);
    }

    if (deliveredResponse.ok) {
      const deliveredData = await deliveredResponse.json();
      if (deliveredData.errors) {
        console.error(`Stream GraphQL errors for account ${accountId}:`, deliveredData.errors);
      }
      const deliveredAccount = deliveredData.data?.viewer?.accounts?.[0];
      if (deliveredAccount?.streamMinutesViewedAdaptiveGroups) {
        for (const entry of deliveredAccount.streamMinutesViewedAdaptiveGroups) {
          currentMinutesDelivered += entry.sum?.minutesViewed || 0;
        }
      }
    } else {
      console.error(`Stream GraphQL failed for account ${accountId}: ${deliveredResponse.status}`);
    }

    console.log(`Stream for account ${accountId}: ${currentMinutesStored.toLocaleString()} min stored, ${currentMinutesDelivered.toLocaleString()} min delivered`);

  } catch (error) {
    console.error(`Error fetching Stream for account ${accountId}:`, error);
    return null;
  }

  let previousMinutesStored = 0;
  let previousMinutesDelivered = 0;

  const previousMonthCacheKey = `monthly-stream:${accountId}:${previousMonthKey}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');

  if (cachedPreviousMonth) {
    previousMinutesStored = cachedPreviousMonth.minutesStored || 0;
    previousMinutesDelivered = cachedPreviousMonth.minutesDelivered || 0;
    console.log(`Stream previous month from cache: ${previousMinutesDelivered.toLocaleString()} min delivered`);
  } else if (now.getDate() >= 2) {
    try {
      const prevDateStart = previousMonthStart.toISOString().split('T')[0];
      const prevDateEnd = previousMonthEnd.toISOString().split('T')[0];

      const prevResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: minutesDeliveredQuery,
          variables: { accountTag: accountId, start: prevDateStart, end: prevDateEnd },
        }),
      });

      if (prevResponse.ok) {
        const prevData = await prevResponse.json();
        const prevAccount = prevData.data?.viewer?.accounts?.[0];
        if (prevAccount?.streamMinutesViewedAdaptiveGroups) {
          for (const entry of prevAccount.streamMinutesViewedAdaptiveGroups) {
            previousMinutesDelivered += entry.sum?.minutesViewed || 0;
          }
        }

        await env.CONFIG_KV.put(
          previousMonthCacheKey,
          JSON.stringify({ minutesStored: previousMinutesStored, minutesDelivered: previousMinutesDelivered, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Stream previous month cached: ${previousMinutesDelivered.toLocaleString()} min delivered`);
      }
    } catch (prevError) {
      console.error(`Failed to fetch previous month Stream:`, prevError);
    }
  }

  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-stream:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ minutesStored: currentMinutesStored, minutesDelivered: currentMinutesDelivered, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached Stream snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache Stream snapshot:', cacheError);
      }
    }
  }

  const historicalData = await getHistoricalStreamData(env, accountId);

  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      minutesStored: currentMinutesStored,
      minutesDelivered: currentMinutesDelivered,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { minutesStored: currentMinutesStored, minutesDelivered: currentMinutesDelivered },
    previous: { minutesStored: previousMinutesStored, minutesDelivered: previousMinutesDelivered },
    timeSeries: deduplicatedTimeSeries,
  };
}

async function getHistoricalStreamData(env, accountId) {
  const historicalData = [];

  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-stream:${accountId}:` });

    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();

        historicalData.push({
          month, timestamp,
          minutesStored: data.minutesStored || 0,
          minutesDelivered: data.minutesDelivered || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical Stream data:', error);
  }

  return historicalData;
}

/**
 * Fetch Images metrics for an account
 * Returns images stored (REST API) and images delivered (GraphQL)
 */
async function fetchImagesForAccount(apiKey, accountId, imagesConfig, env) {
  if (!imagesConfig || !imagesConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const imagesDeliveredQuery = `
    query getImagesDelivered($accountTag: string!, $start: Date, $end: Date) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          imagesRequestsAdaptiveGroups(
            filter: { date_geq: $start, date_lt: $end }
            limit: 10000
          ) {
            sum { requests }
          }
        }
      }
    }
  `;

  let currentImagesStored = 0;
  let currentImagesDelivered = 0;

  try {
    const dateStart = currentMonthStart.toISOString().split('T')[0];
    const dateEnd = now.toISOString().split('T')[0];

    const [storageResponse, deliveredResponse] = await Promise.all([
      fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/images/v1/stats`, {
        headers: { 'Authorization': `Bearer ${apiKey}` },
      }),
      fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: imagesDeliveredQuery,
          variables: { accountTag: accountId, start: dateStart, end: dateEnd },
        }),
      }),
    ]);

    if (storageResponse.ok) {
      const storageData = await storageResponse.json();
      if (storageData.success && storageData.result) {
        currentImagesStored = storageData.result.count?.current || 0;
      }
    } else {
      console.error(`Images stats API failed for account ${accountId}: ${storageResponse.status}`);
    }

    if (deliveredResponse.ok) {
      const deliveredData = await deliveredResponse.json();
      if (deliveredData.errors) {
        console.error(`Images GraphQL errors for account ${accountId}:`, deliveredData.errors);
      }
      const deliveredAccount = deliveredData.data?.viewer?.accounts?.[0];
      if (deliveredAccount?.imagesRequestsAdaptiveGroups) {
        for (const entry of deliveredAccount.imagesRequestsAdaptiveGroups) {
          currentImagesDelivered += entry.sum?.requests || 0;
        }
      }
    } else {
      console.error(`Images GraphQL failed for account ${accountId}: ${deliveredResponse.status}`);
    }

    console.log(`Images for account ${accountId}: ${currentImagesStored.toLocaleString()} stored, ${currentImagesDelivered.toLocaleString()} delivered`);

  } catch (error) {
    console.error(`Error fetching Images for account ${accountId}:`, error);
    return null;
  }

  let previousImagesStored = 0;
  let previousImagesDelivered = 0;

  const previousMonthCacheKey = `monthly-images:${accountId}:${previousMonthKey}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');

  if (cachedPreviousMonth) {
    previousImagesStored = cachedPreviousMonth.imagesStored || 0;
    previousImagesDelivered = cachedPreviousMonth.imagesDelivered || 0;
    console.log(`Images previous month from cache: ${previousImagesDelivered.toLocaleString()} delivered`);
  } else if (now.getDate() >= 2) {
    try {
      const prevDateStart = previousMonthStart.toISOString().split('T')[0];
      const prevDateEnd = previousMonthEnd.toISOString().split('T')[0];

      const prevResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: imagesDeliveredQuery,
          variables: { accountTag: accountId, start: prevDateStart, end: prevDateEnd },
        }),
      });

      if (prevResponse.ok) {
        const prevData = await prevResponse.json();
        const prevAccount = prevData.data?.viewer?.accounts?.[0];
        if (prevAccount?.imagesRequestsAdaptiveGroups) {
          for (const entry of prevAccount.imagesRequestsAdaptiveGroups) {
            previousImagesDelivered += entry.sum?.requests || 0;
          }
        }

        await env.CONFIG_KV.put(
          previousMonthCacheKey,
          JSON.stringify({ imagesStored: previousImagesStored, imagesDelivered: previousImagesDelivered, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Images previous month cached: ${previousImagesDelivered.toLocaleString()} delivered`);
      }
    } catch (prevError) {
      console.error(`Failed to fetch previous month Images:`, prevError);
    }
  }

  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-images:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ imagesStored: currentImagesStored, imagesDelivered: currentImagesDelivered, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached Images snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache Images snapshot:', cacheError);
      }
    }
  }

  const historicalData = await getHistoricalImagesData(env, accountId);

  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      imagesStored: currentImagesStored,
      imagesDelivered: currentImagesDelivered,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { imagesStored: currentImagesStored, imagesDelivered: currentImagesDelivered },
    previous: { imagesStored: previousImagesStored, imagesDelivered: previousImagesDelivered },
    timeSeries: deduplicatedTimeSeries,
  };
}

async function getHistoricalImagesData(env, accountId) {
  const historicalData = [];

  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-images:${accountId}:` });

    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();

        historicalData.push({
          month, timestamp,
          imagesStored: data.imagesStored || 0,
          imagesDelivered: data.imagesDelivered || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical Images data:', error);
  }

  return historicalData;
}

/**
 * Fetch Workers AI metrics for an account
 * Returns neurons used (GraphQL aiInferenceAdaptiveGroups)
 */
async function fetchWorkersAIForAccount(apiKey, accountId, waiConfig, env) {
  if (!waiConfig || !waiConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const neuronsQuery = `
    query getWorkersAINeurons($accountTag: string!, $start: Date, $end: Date) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          aiInferenceAdaptiveGroups(
            filter: { date_geq: $start, date_leq: $end }
            limit: 1
          ) {
            sum { neurons: totalNeurons }
          }
        }
      }
    }
  `;

  let currentNeurons = 0;

  try {
    const dateStart = currentMonthStart.toISOString().split('T')[0];
    const dateEnd = now.toISOString().split('T')[0];

    const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: neuronsQuery,
        variables: { accountTag: accountId, start: dateStart, end: dateEnd },
      }),
    });

    if (response.ok) {
      const data = await response.json();
      if (data.errors) {
        console.error(`Workers AI GraphQL errors for account ${accountId}:`, data.errors);
      }
      const account = data.data?.viewer?.accounts?.[0];
      if (account?.aiInferenceAdaptiveGroups?.[0]) {
        currentNeurons = account.aiInferenceAdaptiveGroups[0].sum?.neurons || 0;
      }
    } else {
      console.error(`Workers AI GraphQL failed for account ${accountId}: ${response.status}`);
    }

    console.log(`Workers AI for account ${accountId}: ${currentNeurons.toLocaleString()} neurons`);

  } catch (error) {
    console.error(`Error fetching Workers AI for account ${accountId}:`, error);
    return null;
  }

  let previousNeurons = 0;

  const previousMonthCacheKey = `monthly-workersai:${accountId}:${previousMonthKey}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');

  if (cachedPreviousMonth) {
    previousNeurons = cachedPreviousMonth.neurons || 0;
    console.log(`Workers AI previous month from cache: ${previousNeurons.toLocaleString()} neurons`);
  } else if (now.getDate() >= 2) {
    try {
      const prevDateStart = previousMonthStart.toISOString().split('T')[0];
      const prevDateEnd = previousMonthEnd.toISOString().split('T')[0];

      const prevResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: neuronsQuery,
          variables: { accountTag: accountId, start: prevDateStart, end: prevDateEnd },
        }),
      });

      if (prevResponse.ok) {
        const prevData = await prevResponse.json();
        const prevAccount = prevData.data?.viewer?.accounts?.[0];
        if (prevAccount?.aiInferenceAdaptiveGroups?.[0]) {
          previousNeurons = prevAccount.aiInferenceAdaptiveGroups[0].sum?.neurons || 0;
        }

        await env.CONFIG_KV.put(
          previousMonthCacheKey,
          JSON.stringify({ neurons: previousNeurons, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Workers AI previous month cached: ${previousNeurons.toLocaleString()} neurons`);
      }
    } catch (prevError) {
      console.error(`Failed to fetch previous month Workers AI:`, prevError);
    }
  }

  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-workersai:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ neurons: currentNeurons, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached Workers AI snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache Workers AI snapshot:', cacheError);
      }
    }
  }

  const historicalData = await getHistoricalWorkersAIData(env, accountId);

  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      neurons: currentNeurons,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { neurons: currentNeurons },
    previous: { neurons: previousNeurons },
    timeSeries: deduplicatedTimeSeries,
  };
}

/**
 * Fetch Queues metrics for an account
 * Returns billable operations (GraphQL queueMessageOperationsAdaptiveGroups)
 */
async function fetchQueuesForAccount(apiKey, accountId, queuesConfig, env) {
  if (!queuesConfig || !queuesConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const opsQuery = `
    query getQueuesOperations($accountTag: string!, $start: Date!, $end: Date!) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          queueMessageOperationsAdaptiveGroups(
            filter: { date_geq: $start, date_leq: $end }
            limit: 1
          ) {
            sum { billableOperations }
          }
        }
      }
    }
  `;

  let currentOps = 0;

  try {
    const dateStart = currentMonthStart.toISOString().split('T')[0];
    const dateEnd = now.toISOString().split('T')[0];

    const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: opsQuery,
        variables: { accountTag: accountId, start: dateStart, end: dateEnd },
      }),
    });

    if (response.ok) {
      const data = await response.json();
      if (data.errors) {
        console.error(`Queues GraphQL errors for account ${accountId}:`, data.errors);
      }
      const account = data.data?.viewer?.accounts?.[0];
      if (account?.queueMessageOperationsAdaptiveGroups?.[0]) {
        currentOps = account.queueMessageOperationsAdaptiveGroups[0].sum?.billableOperations || 0;
      }
    } else {
      console.error(`Queues GraphQL failed for account ${accountId}: ${response.status}`);
    }

    console.log(`Queues for account ${accountId}: ${currentOps.toLocaleString()} operations`);

  } catch (error) {
    console.error(`Error fetching Queues for account ${accountId}:`, error);
    return null;
  }

  let previousOps = 0;

  const previousMonthCacheKey = `monthly-queues:${accountId}:${previousMonthKey}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');

  if (cachedPreviousMonth) {
    previousOps = cachedPreviousMonth.operations || 0;
    console.log(`Queues previous month from cache: ${previousOps.toLocaleString()} operations`);
  } else if (now.getDate() >= 2) {
    try {
      const prevDateStart = previousMonthStart.toISOString().split('T')[0];
      const prevDateEnd = previousMonthEnd.toISOString().split('T')[0];

      const prevResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: opsQuery,
          variables: { accountTag: accountId, start: prevDateStart, end: prevDateEnd },
        }),
      });

      if (prevResponse.ok) {
        const prevData = await prevResponse.json();
        const prevAccount = prevData.data?.viewer?.accounts?.[0];
        if (prevAccount?.queueMessageOperationsAdaptiveGroups?.[0]) {
          previousOps = prevAccount.queueMessageOperationsAdaptiveGroups[0].sum?.billableOperations || 0;
        }

        await env.CONFIG_KV.put(
          previousMonthCacheKey,
          JSON.stringify({ operations: previousOps, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Queues previous month cached: ${previousOps.toLocaleString()} operations`);
      }
    } catch (prevError) {
      console.error(`Failed to fetch previous month Queues:`, prevError);
    }
  }

  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-queues:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ operations: currentOps, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached Queues snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache Queues snapshot:', cacheError);
      }
    }
  }

  const historicalData = await getHistoricalQueuesData(env, accountId);

  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      operations: currentOps,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { operations: currentOps },
    previous: { operations: previousOps },
    timeSeries: deduplicatedTimeSeries,
  };
}

async function pollSpectrumConcurrent(env) {
  try {
    const configData = await env.CONFIG_KV.get('config:default', 'json');
    if (!configData?.networkServices?.spectrum?.enabled) {
      return;
    }

    const specZones = configData.networkServices.spectrum.zones || [];
    if (specZones.length === 0) return;

    const apiKey = env.CLOUDFLARE_API_TOKEN;
    if (!apiKey) return;

    const now = new Date();
    const monthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

    for (const zoneId of specZones) {
      try {
        const response = await fetch(
          `https://api.cloudflare.com/client/v4/zones/${zoneId}/spectrum/analytics/aggregate/current`,
          { headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' } }
        );

        if (!response.ok) continue;

        const data = await response.json();
        if (!data.success || !data.result) continue;

        const concurrent = data.result.reduce((sum, app) => sum + (app.connections || 0), 0);

        const kvKey = `spectrum-concurrent-samples:${zoneId}:${monthKey}`;
        const stored = await env.CONFIG_KV.get(kvKey, 'json');
        const samples = stored?.samples || [];
        samples.push(concurrent);

        await env.CONFIG_KV.put(kvKey, JSON.stringify({
          samples,
          lastUpdated: now.toISOString(),
        }), { expirationTtl: 31536000 });
      } catch (err) {
        console.error(`Spectrum poll error for zone ${zoneId}:`, err);
      }
    }
  } catch (err) {
    console.error('Spectrum concurrent polling failed:', err);
  }
}

function computeP95ZeroFilled(samples, monthStart) {
  if (!samples || samples.length === 0) return 0;
  const now = new Date();
  const start = monthStart || new Date(now.getFullYear(), now.getMonth(), 1);
  const totalMinutes = Math.max(samples.length, Math.floor((now - start) / 60000));
  const zeroFilled = [...samples];
  while (zeroFilled.length < totalMinutes) zeroFilled.push(0);
  zeroFilled.sort((a, b) => a - b);
  const index = Math.ceil(zeroFilled.length * 0.95) - 1;
  return zeroFilled[Math.max(0, index)];
}

async function fetchSpectrumForZone(apiKey, zoneId, specConfig, env) {
  if (!specConfig || !specConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  let currentDataTransfer = 0;

  const SPECTRUM_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
  const currentTransferCacheKey = `spectrum-transfer:${zoneId}:${currentMonthKey}`;
  const cachedTransfer = await env.CONFIG_KV.get(currentTransferCacheKey, 'json');

  if (cachedTransfer && (Date.now() - cachedTransfer.cachedAt) < SPECTRUM_CACHE_TTL_MS) {
    currentDataTransfer = cachedTransfer.bytes || 0;
    console.log(`Spectrum data transfer from cache for zone ${zoneId}: ${(currentDataTransfer / 1e9).toFixed(2)} GB (age: ${Math.round((Date.now() - cachedTransfer.cachedAt) / 60000)}min)`);
  } else {
    try {
      const summaryResponse = await fetch(
        `https://api.cloudflare.com/client/v4/zones/${zoneId}/spectrum/analytics/events/summary?since=${currentMonthStart.toISOString()}&until=${now.toISOString()}&metrics=bytesIngress,bytesEgress`,
        {
          headers: {
            'Authorization': `Bearer ${apiKey}`,
            'Content-Type': 'application/json',
          },
        }
      );

      if (summaryResponse.ok) {
        const data = await summaryResponse.json();
        if (data.success && data.result?.totals) {
          const ingress = data.result.totals.bytesIngress || 0;
          const egress = data.result.totals.bytesEgress || 0;
          currentDataTransfer = ingress + egress;
        }
        if (currentDataTransfer > 0) {
          await env.CONFIG_KV.put(currentTransferCacheKey, JSON.stringify({ bytes: currentDataTransfer, cachedAt: Date.now() }), { expirationTtl: 86400 });
        }
      } else {
        console.error(`Spectrum events/summary failed for zone ${zoneId}: ${summaryResponse.status}`);
        if (cachedTransfer) currentDataTransfer = cachedTransfer.bytes || 0;
      }
    } catch (error) {
      console.error(`Error fetching Spectrum for zone ${zoneId}:`, error);
      if (cachedTransfer) { currentDataTransfer = cachedTransfer.bytes || 0; } else { return null; }
    }
  }

  const concurrentKvKey = `spectrum-concurrent-samples:${zoneId}:${currentMonthKey}`;
  const concurrentData = await env.CONFIG_KV.get(concurrentKvKey, 'json');
  const currentSamples = concurrentData?.samples || [];
  const currentP95Concurrent = computeP95ZeroFilled(currentSamples, currentMonthStart);

  console.log(`Spectrum for zone ${zoneId}: ${(currentDataTransfer / (1024*1024*1024)).toFixed(2)} GB transfer, P95 concurrent: ${currentP95Concurrent} (${currentSamples.length} samples, zero-filled to ${Math.floor((now - currentMonthStart) / 60000)} min)`);

  let previousDataTransfer = 0;
  let previousP95Concurrent = 0;

  const previousMonthCacheKey = `monthly-spectrum:${zoneId}:${previousMonthKey}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');

  if (cachedPreviousMonth) {
    previousDataTransfer = cachedPreviousMonth.dataTransfer || 0;
    previousP95Concurrent = cachedPreviousMonth.p95Concurrent || cachedPreviousMonth.p99Concurrent || cachedPreviousMonth.maxConcurrent || 0;
  } else if (now.getDate() >= 2) {
    try {
      const prevResponse = await fetch(
        `https://api.cloudflare.com/client/v4/zones/${zoneId}/spectrum/analytics/events/summary?since=${previousMonthStart.toISOString()}&until=${previousMonthEnd.toISOString()}&metrics=bytesIngress,bytesEgress`,
        {
          headers: {
            'Authorization': `Bearer ${apiKey}`,
            'Content-Type': 'application/json',
          },
        }
      );

      if (prevResponse.ok) {
        const prevData = await prevResponse.json();
        if (prevData.success && prevData.result?.totals) {
          previousDataTransfer = (prevData.result.totals.bytesIngress || 0) + (prevData.result.totals.bytesEgress || 0);
        }
      }

      const prevSamplesData = await env.CONFIG_KV.get(`spectrum-concurrent-samples:${zoneId}:${previousMonthKey}`, 'json');
      previousP95Concurrent = computeP95ZeroFilled(prevSamplesData?.samples, previousMonthStart);

      await env.CONFIG_KV.put(
        previousMonthCacheKey,
        JSON.stringify({ dataTransfer: previousDataTransfer, p95Concurrent: previousP95Concurrent, cachedAt: Date.now() }),
        { expirationTtl: 31536000 }
      );
      console.log(`Cached Spectrum previous month for zone ${zoneId}`);
    } catch (cacheError) {
      console.error('Failed to fetch/cache Spectrum previous month:', cacheError);
    }
  }

  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-spectrum:${zoneId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ dataTransfer: currentDataTransfer, p95Concurrent: currentP95Concurrent, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached Spectrum snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache Spectrum snapshot:', cacheError);
      }
    }
  }

  const historicalData = await getHistoricalSpectrumData(env, zoneId);

  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      dataTransfer: currentDataTransfer,
      p95Concurrent: currentP95Concurrent,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { dataTransfer: currentDataTransfer, p95Concurrent: currentP95Concurrent },
    previous: { dataTransfer: previousDataTransfer, p95Concurrent: previousP95Concurrent },
    timeSeries: deduplicatedTimeSeries,
  };
}

async function getHistoricalSpectrumData(env, zoneId) {
  const historicalData = [];

  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-spectrum:${zoneId}:` });

    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();

        historicalData.push({
          month, timestamp,
          dataTransfer: data.dataTransfer || 0,
          p95Concurrent: data.p95Concurrent || data.p99Concurrent || data.maxConcurrent || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical Spectrum data:', error);
  }

  return historicalData;
}

async function fetchLoadBalancingForAccount(apiKey, accountId, env) {
  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  try {
    const response = await fetch(
      `https://api.cloudflare.com/client/v4/accounts/${accountId}/load_balancers/usage`,
      { headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' } }
    );

    if (!response.ok) {
      console.error(`Load Balancing API failed for account ${accountId}: ${response.status}`);
      return null;
    }

    const data = await response.json();
    if (!data.success || !data.result) {
      console.error(`Load Balancing API error for account ${accountId}:`, data.errors);
      return null;
    }

    const currentEndpoints = data.result.origins || 0;
    console.log(`Load Balancing for account ${accountId}: ${currentEndpoints} endpoints`);

    const previousMonthCacheKey = `monthly-load-balancing:${accountId}:${previousMonthKey}`;
    let previousEndpoints = 0;
    const cachedPrevious = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');
    if (cachedPrevious) {
      previousEndpoints = cachedPrevious.endpoints || 0;
    }

    if (now.getDate() >= 28) {
      const currentCacheKey = `monthly-load-balancing:${accountId}:${currentMonthKey}`;
      const existingCurrentCache = await env.CONFIG_KV.get(currentCacheKey, 'json');
      if (!existingCurrentCache) {
        try {
          await env.CONFIG_KV.put(
            currentCacheKey,
            JSON.stringify({ endpoints: currentEndpoints, cachedAt: Date.now() }),
            { expirationTtl: 31536000 }
          );
        } catch (cacheError) {
          console.error('Failed to cache Load Balancing data:', cacheError);
        }
      }
    }

    const historicalData = [];
    try {
      const listResult = await env.CONFIG_KV.list({ prefix: `monthly-load-balancing:${accountId}:` });
      for (const key of listResult.keys) {
        const cached = await env.CONFIG_KV.get(key.name, 'json');
        if (cached) {
          const month = key.name.split(':')[2];
          const [year, monthNum] = month.split('-');
          const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
          historicalData.push({ month, timestamp, endpoints: cached.endpoints || 0 });
        }
      }
    } catch (e) {
      console.error('Error loading historical Load Balancing data:', e);
    }

    const timeSeries = [
      ...historicalData,
      { month: currentMonthKey, timestamp: currentMonthStart.toISOString(), endpoints: currentEndpoints },
    ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    const timeSeriesMap = new Map();
    timeSeries.forEach(entry => timeSeriesMap.set(entry.month, entry));
    const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
      .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    return {
      current: { endpoints: currentEndpoints },
      previous: { endpoints: previousEndpoints },
      timeSeries: deduplicatedTimeSeries,
    };
  } catch (error) {
    console.error(`Error fetching Load Balancing for account ${accountId}:`, error);
    return null;
  }
}

async function fetchCustomHostnamesForAccount(apiKey, accountId, env) {
  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  try {
    const zonesResponse = await fetch(
      `https://api.cloudflare.com/client/v4/zones?account.id=${accountId}&per_page=1&status=active`,
      { headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' } }
    );

    if (!zonesResponse.ok) {
      console.error(`Custom Hostnames: Failed to list zones for account ${accountId}: ${zonesResponse.status}`);
      return null;
    }

    const zonesData = await zonesResponse.json();
    if (!zonesData.success || !zonesData.result || zonesData.result.length === 0) {
      console.error(`Custom Hostnames: No zones found for account ${accountId}`);
      return null;
    }

    const zoneId = zonesData.result[0].id;

    const quotaResponse = await fetch(
      `https://api.cloudflare.com/client/v4/zones/${zoneId}/custom_hostnames/quota?ui=true`,
      { headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' } }
    );

    if (!quotaResponse.ok) {
      console.error(`Custom Hostnames API failed for account ${accountId}: ${quotaResponse.status}`);
      return null;
    }

    const quotaData = await quotaResponse.json();
    if (!quotaData.success || !quotaData.result) {
      console.error(`Custom Hostnames API error for account ${accountId}:`, quotaData.errors);
      return null;
    }

    const currentHostnames = quotaData.result.used || 0;
    console.log(`Custom Hostnames for account ${accountId}: ${currentHostnames} used`);

    const previousMonthCacheKey = `monthly-custom-hostnames:${accountId}:${previousMonthKey}`;
    let previousHostnames = 0;
    const cachedPrevious = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');
    if (cachedPrevious) {
      previousHostnames = cachedPrevious.hostnames || 0;
    }

    if (now.getDate() >= 28) {
      const currentCacheKey = `monthly-custom-hostnames:${accountId}:${currentMonthKey}`;
      const existingCurrentCache = await env.CONFIG_KV.get(currentCacheKey, 'json');
      if (!existingCurrentCache) {
        try {
          await env.CONFIG_KV.put(
            currentCacheKey,
            JSON.stringify({ hostnames: currentHostnames, cachedAt: Date.now() }),
            { expirationTtl: 31536000 }
          );
        } catch (cacheError) {
          console.error('Failed to cache Custom Hostnames data:', cacheError);
        }
      }
    }

    const historicalData = [];
    try {
      const listResult = await env.CONFIG_KV.list({ prefix: `monthly-custom-hostnames:${accountId}:` });
      for (const key of listResult.keys) {
        const cached = await env.CONFIG_KV.get(key.name, 'json');
        if (cached) {
          const month = key.name.split(':')[2];
          const [year, monthNum] = month.split('-');
          const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
          historicalData.push({ month, timestamp, hostnames: cached.hostnames || 0 });
        }
      }
    } catch (e) {
      console.error('Error loading historical Custom Hostnames data:', e);
    }

    const timeSeries = [
      ...historicalData,
      { month: currentMonthKey, timestamp: currentMonthStart.toISOString(), hostnames: currentHostnames },
    ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    const timeSeriesMap = new Map();
    timeSeries.forEach(entry => timeSeriesMap.set(entry.month, entry));
    const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
      .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    return {
      current: { hostnames: currentHostnames },
      previous: { hostnames: previousHostnames },
      timeSeries: deduplicatedTimeSeries,
    };
  } catch (error) {
    console.error(`Error fetching Custom Hostnames for account ${accountId}:`, error);
    return null;
  }
}

async function fetchLogExplorerForAccount(apiKey, accountId, env) {
  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const currentDateStart = currentMonthStart.toISOString();

  try {
    const currentResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: `{ viewer { accounts(filter: {accountTag: $accountId}) { logExplorerIngestionAdaptiveGroups(limit: 100, filter: {datetimeMinute_geq: $dateFrom}) { sum { billableBytes } } } } }`,
        variables: { accountId, dateFrom: currentDateStart },
      }),
    });

    if (!currentResponse.ok) {
      console.error(`Log Explorer API failed for account ${accountId}: ${currentResponse.status}`);
      return null;
    }

    const currentData = await currentResponse.json();
    const currentAccount = currentData.data?.viewer?.accounts?.[0];
    let currentBillableGB = 0;
    if (currentAccount?.logExplorerIngestionAdaptiveGroups?.[0]) {
      const billableBytes = currentAccount.logExplorerIngestionAdaptiveGroups[0].sum?.billableBytes || 0;
      currentBillableGB = billableBytes / (1000 ** 3);
    }
    console.log(`Log Explorer for account ${accountId}: ${currentBillableGB.toFixed(2)} GB`);

    const previousMonthCacheKey = `monthly-log-explorer:${accountId}:${previousMonthKey}`;
    let previousBillableGB = 0;
    const cachedPrevious = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');
    if (cachedPrevious) {
      previousBillableGB = cachedPrevious.billableGB || 0;
    } else if (now.getDate() >= 2) {
      try {
        const prevDateStart = previousMonthStart.toISOString();
        const prevDateEnd = new Date(now.getFullYear(), now.getMonth(), 1).toISOString();
        const prevResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            query: `{ viewer { accounts(filter: {accountTag: $accountId}) { logExplorerIngestionAdaptiveGroups(limit: 100, filter: {datetimeMinute_geq: $dateFrom, datetimeMinute_lt: $dateTo}) { sum { billableBytes } } } } }`,
            variables: { accountId, dateFrom: prevDateStart, dateTo: prevDateEnd },
          }),
        });

        if (prevResponse.ok) {
          const prevData = await prevResponse.json();
          const prevAccount = prevData.data?.viewer?.accounts?.[0];
          if (prevAccount?.logExplorerIngestionAdaptiveGroups?.[0]) {
            previousBillableGB = (prevAccount.logExplorerIngestionAdaptiveGroups[0].sum?.billableBytes || 0) / (1000 ** 3);
          }

          await env.CONFIG_KV.put(
            previousMonthCacheKey,
            JSON.stringify({ billableGB: previousBillableGB, cachedAt: Date.now() }),
            { expirationTtl: 31536000 }
          );
          console.log(`Log Explorer previous month cached: ${previousBillableGB.toFixed(2)} GB`);
        }
      } catch (prevError) {
        console.error(`Failed to fetch previous month Log Explorer:`, prevError);
      }
    }

    if (now.getDate() >= 28) {
      const currentCacheKey = `monthly-log-explorer:${accountId}:${currentMonthKey}`;
      const existingCurrentCache = await env.CONFIG_KV.get(currentCacheKey, 'json');
      if (!existingCurrentCache) {
        try {
          await env.CONFIG_KV.put(
            currentCacheKey,
            JSON.stringify({ billableGB: currentBillableGB, cachedAt: Date.now() }),
            { expirationTtl: 31536000 }
          );
          console.log(`Cached Log Explorer snapshot for ${currentMonthKey}`);
        } catch (cacheError) {
          console.error('Failed to cache Log Explorer snapshot:', cacheError);
        }
      }
    }

    const historicalData = await getHistoricalLogExplorerData(env, accountId);

    const timeSeries = [
      ...historicalData,
      { month: currentMonthKey, timestamp: currentMonthStart.toISOString(), billableGB: currentBillableGB },
    ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    const timeSeriesMap = new Map();
    timeSeries.forEach(entry => timeSeriesMap.set(entry.month, entry));
    const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
      .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    return {
      current: { billableGB: currentBillableGB },
      previous: { billableGB: previousBillableGB },
      timeSeries: deduplicatedTimeSeries,
    };
  } catch (error) {
    console.error(`Error fetching Log Explorer for account ${accountId}:`, error);
    return null;
  }
}

async function getHistoricalLogExplorerData(env, accountId) {
  const historicalData = [];
  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-log-explorer:${accountId}:` });
    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
        historicalData.push({ month, timestamp, billableGB: data.billableGB || 0 });
      }
    }
  } catch (error) {
    console.error('Error loading historical Log Explorer data:', error);
  }
  return historicalData;
}

async function fetchCacheReserveForZone(apiKey, zoneId, zoneName, env) {
  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const currentDateStart = currentMonthStart.toISOString().split('T')[0];
  const currentDateEnd = now.toISOString().split('T')[0];
  const previousDateStart = previousMonthStart.toISOString().split('T')[0];
  const previousDateEnd = previousMonthEnd.toISOString().split('T')[0];

  try {
    const discoverResp = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: `query discoverBucket($zoneTag: string, $filter: ZoneCacheReserveStorageAdaptiveGroupsFilter_InputObject) {
          viewer { zones(filter: {zoneTag: $zoneTag}) {
            cacheReserveStorageAdaptiveGroups(limit: 1, filter: $filter) { dimensions { bucketName } }
          } }
        }`,
        variables: { zoneTag: zoneId, filter: { date_geq: currentDateStart, date_leq: currentDateEnd } },
      }),
    });
    const discoverData = await discoverResp.json();
    const firstBucket = discoverData.data?.viewer?.zones?.[0]?.cacheReserveStorageAdaptiveGroups?.[0]?.dimensions?.bucketName;
    if (!firstBucket) {
      console.log(`No Cache Reserve bucket found for zone ${zoneId} - skipping`);
      return null;
    }
    const bucketPrefix = firstBucket.replace(/-[^-]+$/, '-%');
    console.log(`Cache Reserve zone ${zoneName || zoneId}: discovered bucket prefix ${bucketPrefix}`);

    const query = `
      query getCacheReserveMetrics(
        $zoneTag: string,
        $currentStorageFilter: ZoneCacheReserveStorageAdaptiveGroupsFilter_InputObject,
        $previousStorageFilter: ZoneCacheReserveStorageAdaptiveGroupsFilter_InputObject,
        $currentOpsFilter: ZoneCacheReserveOperationsAdaptiveGroupsFilter_InputObject,
        $previousOpsFilter: ZoneCacheReserveOperationsAdaptiveGroupsFilter_InputObject
      ) {
        viewer {
          zones(filter: {zoneTag: $zoneTag}) {
            currentStorage: cacheReserveStorageAdaptiveGroups(limit: 10000, filter: $currentStorageFilter) {
              max { storedBytes }
              dimensions { date bucketHash }
            }
            previousStorage: cacheReserveStorageAdaptiveGroups(limit: 10000, filter: $previousStorageFilter) {
              max { storedBytes }
              dimensions { date bucketHash }
            }
            currentOps: cacheReserveOperationsAdaptiveGroups(limit: 10000, filter: $currentOpsFilter) {
              sum { requests }
              dimensions { operationClass }
            }
            previousOps: cacheReserveOperationsAdaptiveGroups(limit: 10000, filter: $previousOpsFilter) {
              sum { requests }
              dimensions { operationClass }
            }
          }
        }
      }
    `;

    const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query,
        variables: {
          zoneTag: zoneId,
          currentStorageFilter: { date_geq: currentDateStart, date_leq: currentDateEnd, bucketName_like: bucketPrefix },
          previousStorageFilter: { date_geq: previousDateStart, date_leq: previousDateEnd, bucketName_like: bucketPrefix },
          currentOpsFilter: { actionStatus_in: ["success", "userError"], date_geq: currentDateStart, date_leq: currentDateEnd, bucketName_like: bucketPrefix },
          previousOpsFilter: { actionStatus_in: ["success", "userError"], date_geq: previousDateStart, date_leq: previousDateEnd, bucketName_like: bucketPrefix },
        },
      }),
    });

    if (!response.ok) {
      console.error(`Cache Reserve GraphQL failed for zone ${zoneId}: ${response.status}`);
      return null;
    }

    const data = await response.json();
    if (data.errors) {
      console.error(`Cache Reserve GraphQL errors for zone ${zoneId}:`, data.errors);
      return null;
    }

    const zone = data.data?.viewer?.zones?.[0];
    if (!zone) {
      console.log(`No Cache Reserve data for zone ${zoneId}`);
      return null;
    }

    const calcGBDays = (storageEntries) => {
      if (!storageEntries || storageEntries.length === 0) return 0;
      const dailyTotals = new Map();
      storageEntries.forEach(entry => {
        const date = entry.dimensions?.date;
        const bytes = entry.max?.storedBytes || 0;
        dailyTotals.set(date, (dailyTotals.get(date) || 0) + bytes);
      });
      let gbDays = 0;
      for (const bytes of dailyTotals.values()) {
        gbDays += bytes / 1e9;
      }
      return gbDays;
    };

    const classifyOps = (opsEntries) => {
      let classA = 0;
      let classB = 0;
      if (!opsEntries) return { classA, classB };
      opsEntries.forEach(entry => {
        const opClass = (entry.dimensions?.operationClass || '').toUpperCase();
        const requests = entry.sum?.requests || 0;
        if (opClass === 'A') {
          classA += requests;
        } else if (opClass === 'B') {
          classB += requests;
        }
      });
      return { classA, classB };
    };

    const currentGBDays = calcGBDays(zone.currentStorage);
    const previousGBDays = calcGBDays(zone.previousStorage);
    const currentOps = classifyOps(zone.currentOps);
    const previousOps = classifyOps(zone.previousOps);

    console.log(`Cache Reserve zone ${zoneName || zoneId}: storage=${currentGBDays.toFixed(4)} GB-days, classA=${currentOps.classA}, classB=${currentOps.classB}`);

    const previousMonthCacheKey = `monthly-cache-reserve:${zoneId}:${previousMonthKey}`;
    if (now.getDate() >= 2 && (previousGBDays > 0 || previousOps.classA > 0 || previousOps.classB > 0)) {
      try {
        await env.CONFIG_KV.put(
          previousMonthCacheKey,
          JSON.stringify({ storageGBDays: previousGBDays, classAOps: previousOps.classA, classBOps: previousOps.classB }),
          { expirationTtl: 31536000 }
        );
      } catch (cacheError) {
        console.error('Failed to cache Cache Reserve previous month:', cacheError);
      }
    }

    const historicalData = await getHistoricalCacheReserveData(env, zoneId);

    const timeSeries = [
      ...historicalData,
      {
        month: previousMonthKey,
        timestamp: previousMonthStart.toISOString(),
        storageGBDays: previousGBDays,
        classAOps: previousOps.classA,
        classBOps: previousOps.classB,
      },
      {
        month: currentMonthKey,
        timestamp: currentMonthStart.toISOString(),
        storageGBDays: currentGBDays,
        classAOps: currentOps.classA,
        classBOps: currentOps.classB,
      }
    ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    const timeSeriesMap = new Map();
    timeSeries.forEach(entry => {
      timeSeriesMap.set(entry.month, entry);
    });
    const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
      .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    return {
      zoneId,
      zoneName: zoneName || zoneId,
      current: { storageGBDays: currentGBDays, classAOps: currentOps.classA, classBOps: currentOps.classB },
      previous: { storageGBDays: previousGBDays, classAOps: previousOps.classA, classBOps: previousOps.classB },
      timeSeries: deduplicatedTimeSeries,
    };
  } catch (error) {
    console.error(`Error fetching Cache Reserve for zone ${zoneId}:`, error);
    return null;
  }
}

async function getHistoricalCacheReserveData(env, zoneId) {
  const historicalData = [];
  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-cache-reserve:${zoneId}:` });
    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
        historicalData.push({
          month, timestamp,
          storageGBDays: data.storageGBDays || data.storageMBDays || data.storageBytes || 0,
          classAOps: data.classAOps || 0,
          classBOps: data.classBOps || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical Cache Reserve data:', error);
  }
  return historicalData;
}

async function fetchWorkersLogsTracesForAccount(apiKey, accountId, wltConfig, env) {
  if (!wltConfig || !wltConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  let currentEvents = 0;

  try {
    const response = await fetch(
      `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/observability/usage-statuses`,
      {
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
      }
    );

    if (response.ok) {
      const data = await response.json();
      if (data.success && data.result) {
        currentEvents = data.result.numEventsMonthly || 0;
      }
    } else {
      console.error(`Workers Logs & Traces API failed for account ${accountId}: ${response.status}`);
    }

    console.log(`Workers Logs & Traces for account ${accountId}: ${currentEvents.toLocaleString()} events`);

  } catch (error) {
    console.error(`Error fetching Workers Logs & Traces for account ${accountId}:`, error);
    return null;
  }

  let previousEvents = 0;

  const previousMonthCacheKey = `monthly-wlt:${accountId}:${previousMonthKey}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');

  if (cachedPreviousMonth) {
    previousEvents = cachedPreviousMonth.events || 0;
    console.log(`Workers Logs & Traces previous month from cache: ${previousEvents.toLocaleString()} events`);
  }

  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-wlt:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ events: currentEvents, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached Workers Logs & Traces snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache Workers Logs & Traces snapshot:', cacheError);
      }
    }
  }

  const historicalData = await getHistoricalWorkersLogsTracesData(env, accountId);

  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      events: currentEvents,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { events: currentEvents },
    previous: { events: previousEvents },
    timeSeries: deduplicatedTimeSeries,
  };
}

async function getHistoricalWorkersLogsTracesData(env, accountId) {
  const historicalData = [];

  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-wlt:${accountId}:` });

    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();

        historicalData.push({
          month, timestamp,
          events: data.events || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical Workers Logs & Traces data:', error);
  }

  return historicalData;
}

async function getHistoricalQueuesData(env, accountId) {
  const historicalData = [];

  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-queues:${accountId}:` });

    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();

        historicalData.push({
          month, timestamp,
          operations: data.operations || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical Queues data:', error);
  }

  return historicalData;
}

async function getHistoricalWorkersAIData(env, accountId) {
  const historicalData = [];

  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-workersai:${accountId}:` });

    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();

        historicalData.push({
          month, timestamp,
          neurons: data.neurons || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical Workers AI data:', error);
  }

  return historicalData;
}

/**
 * Fetch Durable Objects metrics for an account
 * Returns requests, duration (GB-s), SQLite rows read/written, KV read/write/delete units, and storage
 */
async function fetchDurableObjectsForAccount(apiKey, accountId, doConfig, env) {
  if (!doConfig || !doConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const invocationsQuery = `
    query getDOInvocations($accountTag: string!, $filter: AccountDurableObjectsInvocationsAdaptiveGroupsFilter_InputObject) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          durableObjectsInvocationsAdaptiveGroups(filter: $filter, limit: 10000) {
            sum { requests }
          }
        }
      }
    }
  `;

  const periodicQuery = `
    query getDOPeriodic($accountTag: string!, $filter: AccountDurableObjectsPeriodicGroupsFilter_InputObject) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          durableObjectsPeriodicGroups(filter: $filter, limit: 10000) {
            sum { activeTime rowsRead rowsWritten storageReadUnits storageWriteUnits storageDeletes }
          }
        }
      }
    }
  `;

  const storageQuery = `
    query getDOStorage($accountTag: string!, $filter: AccountDurableObjectsStorageGroupsFilter_InputObject) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          durableObjectsStorageGroups(filter: $filter, limit: 10000, orderBy: [date_DESC]) {
            max { storedBytes }
            dimensions { date }
          }
        }
      }
    }
  `;

  let currentRequests = 0;
  let currentDurationGBs = 0;
  let currentSqliteRowsRead = 0;
  let currentSqliteRowsWritten = 0;
  let currentKvReadUnits = 0;
  let currentKvWriteUnits = 0;
  let currentKvDeletes = 0;
  let currentStorageMB = 0;

  try {
    const dateStart = currentMonthStart.toISOString().split('T')[0];
    const dateEnd = now.toISOString().split('T')[0];
    const dateFilter = { date_geq: dateStart, date_leq: dateEnd };

    const [invResponse, periodicResponse, storageResponse] = await Promise.all([
      fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: invocationsQuery, variables: { accountTag: accountId, filter: dateFilter } }),
      }),
      fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: periodicQuery, variables: { accountTag: accountId, filter: dateFilter } }),
      }),
      fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: storageQuery, variables: { accountTag: accountId, filter: { date: dateEnd } } }),
      }),
    ]);

    if (!invResponse.ok || !periodicResponse.ok || !storageResponse.ok) {
      console.error(`DO GraphQL failed for account ${accountId}`);
      return null;
    }

    const [invData, periodicData, storageData] = await Promise.all([
      invResponse.json(), periodicResponse.json(), storageResponse.json(),
    ]);

    if (invData.errors) console.error(`DO Invocations errors for ${accountId}:`, invData.errors);
    if (periodicData.errors) console.error(`DO Periodic errors for ${accountId}:`, periodicData.errors);
    if (storageData.errors) console.error(`DO Storage errors for ${accountId}:`, storageData.errors);

    const invAccount = invData.data?.viewer?.accounts?.[0];
    if (invAccount?.durableObjectsInvocationsAdaptiveGroups) {
      for (const entry of invAccount.durableObjectsInvocationsAdaptiveGroups) {
        currentRequests += entry.sum?.requests || 0;
      }
    }

    const periodicAccount = periodicData.data?.viewer?.accounts?.[0];
    if (periodicAccount?.durableObjectsPeriodicGroups) {
      for (const entry of periodicAccount.durableObjectsPeriodicGroups) {
        const activeTimeUs = entry.sum?.activeTime || 0;
        currentDurationGBs += activeTimeUs / 7812500;
        currentSqliteRowsRead += entry.sum?.rowsRead || 0;
        currentSqliteRowsWritten += entry.sum?.rowsWritten || 0;
        currentKvReadUnits += entry.sum?.storageReadUnits || 0;
        currentKvWriteUnits += entry.sum?.storageWriteUnits || 0;
        currentKvDeletes += entry.sum?.storageDeletes || 0;
      }
    }

    const storageAccount = storageData.data?.viewer?.accounts?.[0];
    if (storageAccount?.durableObjectsStorageGroups && storageAccount.durableObjectsStorageGroups.length > 0) {
      const latestStorage = storageAccount.durableObjectsStorageGroups[0];
      currentStorageMB = (latestStorage.max?.storedBytes || 0) / 1000000;
    }

    console.log(`DO for account ${accountId}: ${currentRequests.toLocaleString()} requests, ${currentDurationGBs.toFixed(2)} GB-s, ${currentSqliteRowsRead.toLocaleString()} sqlite reads, ${currentStorageMB.toFixed(4)} MB`);

  } catch (error) {
    console.error(`Error fetching DO for account ${accountId}:`, error);
    return null;
  }

  let previousRequests = 0;
  let previousDurationGBs = 0;
  let previousSqliteRowsRead = 0;
  let previousSqliteRowsWritten = 0;
  let previousKvReadUnits = 0;
  let previousKvWriteUnits = 0;
  let previousKvDeletes = 0;
  let previousStorageMB = 0;

  const previousMonthCacheKey = `monthly-do:${accountId}:${previousMonthKey}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');

  if (cachedPreviousMonth) {
    previousRequests = cachedPreviousMonth.requests || 0;
    previousDurationGBs = cachedPreviousMonth.durationGBs || 0;
    previousSqliteRowsRead = cachedPreviousMonth.sqliteRowsRead || 0;
    previousSqliteRowsWritten = cachedPreviousMonth.sqliteRowsWritten || 0;
    previousKvReadUnits = cachedPreviousMonth.kvReadUnits || 0;
    previousKvWriteUnits = cachedPreviousMonth.kvWriteUnits || 0;
    previousKvDeletes = cachedPreviousMonth.kvDeletes || 0;
    previousStorageMB = cachedPreviousMonth.storageMB || 0;
    console.log(`DO previous month from cache: ${previousRequests.toLocaleString()} requests`);
  } else if (now.getDate() >= 2) {
    try {
      const prevDateStart = previousMonthStart.toISOString().split('T')[0];
      const prevDateEnd = previousMonthEnd.toISOString().split('T')[0];
      const prevFilter = { date_geq: prevDateStart, date_leq: prevDateEnd };

      const [prevInvResponse, prevPeriodicResponse] = await Promise.all([
        fetch('https://api.cloudflare.com/client/v4/graphql', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: invocationsQuery, variables: { accountTag: accountId, filter: prevFilter } }),
        }),
        fetch('https://api.cloudflare.com/client/v4/graphql', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: periodicQuery, variables: { accountTag: accountId, filter: prevFilter } }),
        }),
      ]);

      if (prevInvResponse.ok && prevPeriodicResponse.ok) {
        const [prevInvData, prevPeriodicData] = await Promise.all([
          prevInvResponse.json(), prevPeriodicResponse.json(),
        ]);

        const prevInvAccount = prevInvData.data?.viewer?.accounts?.[0];
        if (prevInvAccount?.durableObjectsInvocationsAdaptiveGroups) {
          for (const entry of prevInvAccount.durableObjectsInvocationsAdaptiveGroups) {
            previousRequests += entry.sum?.requests || 0;
          }
        }

        const prevPeriodicAccount = prevPeriodicData.data?.viewer?.accounts?.[0];
        if (prevPeriodicAccount?.durableObjectsPeriodicGroups) {
          for (const entry of prevPeriodicAccount.durableObjectsPeriodicGroups) {
            previousDurationGBs += (entry.sum?.activeTime || 0) / 7812500;
            previousSqliteRowsRead += entry.sum?.rowsRead || 0;
            previousSqliteRowsWritten += entry.sum?.rowsWritten || 0;
            previousKvReadUnits += entry.sum?.storageReadUnits || 0;
            previousKvWriteUnits += entry.sum?.storageWriteUnits || 0;
            previousKvDeletes += entry.sum?.storageDeletes || 0;
          }
        }

        await env.CONFIG_KV.put(
          previousMonthCacheKey,
          JSON.stringify({ requests: previousRequests, durationGBs: previousDurationGBs, sqliteRowsRead: previousSqliteRowsRead, sqliteRowsWritten: previousSqliteRowsWritten, kvReadUnits: previousKvReadUnits, kvWriteUnits: previousKvWriteUnits, kvDeletes: previousKvDeletes, storageMB: previousStorageMB, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`DO previous month cached: ${previousRequests.toLocaleString()} requests`);
      }
    } catch (prevError) {
      console.error(`Failed to fetch previous month DO:`, prevError);
    }
  }

  if (now.getDate() >= 28) {
    const currentMonthCacheKey = `monthly-do:${accountId}:${currentMonthKey}`;
    const existingCurrentCache = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
    if (!existingCurrentCache) {
      try {
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify({ requests: currentRequests, durationGBs: currentDurationGBs, sqliteRowsRead: currentSqliteRowsRead, sqliteRowsWritten: currentSqliteRowsWritten, kvReadUnits: currentKvReadUnits, kvWriteUnits: currentKvWriteUnits, kvDeletes: currentKvDeletes, storageMB: currentStorageMB, cachedAt: Date.now() }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached DO snapshot for ${currentMonthKey}`);
      } catch (cacheError) {
        console.error('Failed to cache DO snapshot:', cacheError);
      }
    }
  }

  const historicalData = await getHistoricalDOData(env, accountId);

  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      requests: currentRequests,
      durationGBs: currentDurationGBs,
      sqliteRowsRead: currentSqliteRowsRead,
      sqliteRowsWritten: currentSqliteRowsWritten,
      kvReadUnits: currentKvReadUnits,
      kvWriteUnits: currentKvWriteUnits,
      kvDeletes: currentKvDeletes,
      storageMB: currentStorageMB,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => { timeSeriesMap.set(entry.month, entry); });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  return {
    current: { requests: currentRequests, durationGBs: currentDurationGBs, sqliteRowsRead: currentSqliteRowsRead, sqliteRowsWritten: currentSqliteRowsWritten, kvReadUnits: currentKvReadUnits, kvWriteUnits: currentKvWriteUnits, kvDeletes: currentKvDeletes, storageMB: currentStorageMB },
    previous: { requests: previousRequests, durationGBs: previousDurationGBs, sqliteRowsRead: previousSqliteRowsRead, sqliteRowsWritten: previousSqliteRowsWritten, kvReadUnits: previousKvReadUnits, kvWriteUnits: previousKvWriteUnits, kvDeletes: previousKvDeletes, storageMB: previousStorageMB },
    timeSeries: deduplicatedTimeSeries,
  };
}

async function getHistoricalDOData(env, accountId) {
  const historicalData = [];
  try {
    const listResult = await env.CONFIG_KV.list({ prefix: `monthly-do:${accountId}:` });
    for (const key of listResult.keys) {
      const data = await env.CONFIG_KV.get(key.name, 'json');
      if (data) {
        const month = key.name.split(':')[2];
        const [year, monthNum] = month.split('-');
        const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
        historicalData.push({
          month, timestamp,
          requests: data.requests || 0,
          durationGBs: data.durationGBs || 0,
          sqliteRowsRead: data.sqliteRowsRead || 0,
          sqliteRowsWritten: data.sqliteRowsWritten || 0,
          kvReadUnits: data.kvReadUnits || 0,
          kvWriteUnits: data.kvWriteUnits || 0,
          kvDeletes: data.kvDeletes || 0,
          storageMB: data.storageMB || 0,
        });
      }
    }
  } catch (error) {
    console.error('Error loading historical DO data:', error);
  }
  return historicalData;
}

/**
 * Check if an IP address is private (RFC1918)
 */
function isPrivateIP(ip) {
  if (!ip) return false;
  // 10.0.0.0/8
  if (ip.startsWith('10.')) return true;
  // 192.168.0.0/16
  if (ip.startsWith('192.168.')) return true;
  // 172.16.0.0/12 (172.16.x.x - 172.31.x.x)
  if (ip.startsWith('172.')) {
    const secondOctet = parseInt(ip.split('.')[1], 10);
    if (secondOctet >= 16 && secondOctet <= 31) return true;
  }
  return false;
}

/**
 * Classify tunnels as Magic Transit or Magic WAN based on IP addresses
 * Returns a Map of tunnelName -> 'magicTransit' | 'magicWan'
 * Logic: If any IP (source or dest) is private -> Magic WAN, else -> Magic Transit
 */
async function classifyTunnelsByIP(apiKey, accountId, env) {
  const cacheKey = `tunnel-classification:${accountId}`;
  
  // Check cache first (valid for 24 hours)
  const cached = await env.CONFIG_KV.get(cacheKey, 'json');
  if (cached && (Date.now() - cached.cachedAt) < 24 * 60 * 60 * 1000) {
    console.log(`Tunnel classification from cache for ${accountId}: ${Object.keys(cached.tunnels).length} tunnels`);
    return new Map(Object.entries(cached.tunnels));
  }
  
  // Query recent IP data to classify tunnels
  const now = new Date();
  const startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); // Last 7 days
  
  const query = `
    query ClassifyTunnels($accountTag: String!, $datetimeStart: Date!, $datetimeEnd: Date!) {
      viewer {
        accounts(filter: {accountTag: $accountTag}) {
          magicTransitNetworkAnalyticsAdaptiveGroups(
            limit: 1000,
            filter: {
              datetime_geq: $datetimeStart,
              datetime_lt: $datetimeEnd
            }
          ) {
            dimensions {
              ipSourceAddress
              ipDestinationAddress
              ingressTunnelName
              egressTunnelName
            }
          }
        }
      }
    }
  `;
  
  try {
    const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query,
        variables: {
          accountTag: accountId,
          datetimeStart: startDate.toISOString(),
          datetimeEnd: now.toISOString(),
        },
      }),
    });
    
    if (!response.ok) {
      console.error(`Failed to fetch tunnel classification data: ${response.status}`);
      return new Map();
    }
    
    const data = await response.json();
    const entries = data?.data?.viewer?.accounts?.[0]?.magicTransitNetworkAnalyticsAdaptiveGroups || [];
    
    // Track which tunnels have private IPs
    const tunnelHasPrivateIP = new Map();
    
    for (const entry of entries) {
      const srcIP = entry.dimensions?.ipSourceAddress;
      const dstIP = entry.dimensions?.ipDestinationAddress;
      const ingressTunnel = entry.dimensions?.ingressTunnelName;
      const egressTunnel = entry.dimensions?.egressTunnelName;
      
      const hasPrivate = isPrivateIP(srcIP) || isPrivateIP(dstIP);
      
      // Mark tunnels that have any private IP traffic as Magic WAN
      if (ingressTunnel) {
        if (hasPrivate || !tunnelHasPrivateIP.has(ingressTunnel)) {
          tunnelHasPrivateIP.set(ingressTunnel, tunnelHasPrivateIP.get(ingressTunnel) || hasPrivate);
        }
      }
      if (egressTunnel) {
        if (hasPrivate || !tunnelHasPrivateIP.has(egressTunnel)) {
          tunnelHasPrivateIP.set(egressTunnel, tunnelHasPrivateIP.get(egressTunnel) || hasPrivate);
        }
      }
    }
    
    // Convert to tunnel -> serviceType map
    const tunnelClassification = new Map();
    for (const [tunnelName, hasPrivate] of tunnelHasPrivateIP) {
      if (tunnelName) { // Skip empty tunnel names
        tunnelClassification.set(tunnelName, hasPrivate ? 'magicWan' : 'magicTransit');
      }
    }
    
    // Log classification results
    const mtTunnels = [...tunnelClassification.entries()].filter(([_, type]) => type === 'magicTransit').map(([name]) => name);
    const mwanTunnels = [...tunnelClassification.entries()].filter(([_, type]) => type === 'magicWan').map(([name]) => name);
    console.log(`Tunnel classification for ${accountId}: MT=[${mtTunnels.join(', ')}] MWAN=[${mwanTunnels.join(', ')}]`);
    
    // Cache the classification
    await env.CONFIG_KV.put(
      cacheKey,
      JSON.stringify({ tunnels: Object.fromEntries(tunnelClassification), cachedAt: Date.now() }),
      { expirationTtl: 7 * 24 * 60 * 60 } // 7 days
    );
    
    return tunnelClassification;
  } catch (error) {
    console.error('Error classifying tunnels:', error);
    return new Map();
  }
}

/**
 * Fetch Magic Transit/WAN bandwidth for an account using GraphQL
 * Returns P95th bandwidth in Mbps (account-level metric)
 * @param {string} serviceType - 'magicTransit' or 'magicWan'
 */
async function fetchMagicBandwidthForAccount(apiKey, accountId, serviceConfig, env, serviceType) {
  if (!serviceConfig || !serviceConfig.enabled) {
    return null;
  }

  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  // Define time ranges for current and previous month
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 1);
  
  // Round end time to last completed HOUR for stable P95 calculation
  // This ensures the same data is returned regardless of when during the hour you refresh
  const currentMonthEnd = new Date(now);
  currentMonthEnd.setMinutes(0, 0, 0);
  // If we're at exactly the top of the hour, use previous hour to ensure data is complete
  if (now.getMinutes() < 5) {
    currentMonthEnd.setHours(currentMonthEnd.getHours() - 1);
  }

  // Billing-aligned filter configs per service type (matching internal billing SQL)
  // MT: uses direction filter; WAN: no direction filter, uses onRamp/offRamp
  const BILLING_FILTERS = {
    magicTransit: {
      ingress: 'direction: "ingress", offRamp_in: ["GRE", "IPsec", "CNI"]',
      egress: 'direction: "egress", onRamp_in: ["GRE", "IPsec", "CNI"]',
    },
    magicWan: {
      ingress: 'onRamp_in: ["GRE", "IPsec", "CNI"], offRamp_neq: "WARP"',
      egress: 'egressTunnelName_neq: "", ingressTunnelName_neq: "", onRamp_neq: "WARP", offRamp_neq: "WARP"',
    },
  };

  const buildBillingQuery = (extraFilters) => `
    query GetTunnelBandwidth($accountTag: String!, $datetimeStart: Date!, $datetimeEnd: Date!) {
      viewer {
        accounts(filter: {accountTag: $accountTag}) {
          magicTransitTunnelTrafficAdaptiveGroups(
            limit: 10000,
            filter: {
              datetime_geq: $datetimeStart,
              datetime_lt: $datetimeEnd,
              ${extraFilters}
            }
          ) {
            avg {
              bitRateFiveMinutes
            }
            dimensions {
              datetimeFiveMinutes
              tunnelName
            }
          }
        }
      }
    }
  `;

  const WINDOW_DAYS = 4;
  const fetchWindowedData = async (filterStr, periodStart, periodEnd) => {
    const windowMs = WINDOW_DAYS * 24 * 60 * 60 * 1000;
    const windows = [];
    let windowStart = new Date(periodStart.getTime());
    while (windowStart < periodEnd) {
      const windowEnd = new Date(Math.min(windowStart.getTime() + windowMs, periodEnd.getTime()));
      windows.push({ start: windowStart, end: windowEnd });
      windowStart = windowEnd;
    }
    
    const billingQuery = buildBillingQuery(filterStr);
    const results = await Promise.all(windows.map(async (w) => {
      try {
        const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${apiKey}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            query: billingQuery,
            variables: {
              accountTag: accountId,
              datetimeStart: w.start.toISOString(),
              datetimeEnd: w.end.toISOString(),
            },
          }),
        });
        if (response.ok) {
          const data = await response.json();
          return data?.data?.viewer?.accounts?.[0]?.magicTransitTunnelTrafficAdaptiveGroups || [];
        }
        return [];
      } catch (err) {
        console.error(`${serviceType} window fetch error:`, err);
        return [];
      }
    }));
    
    const allEntries = results.flat();
    console.log(`${serviceType} fetched ${allEntries.length} entries in ${windows.length} parallel windowed queries`);
    return allEntries;
  };

  const calcAccountLevelP95 = (entries, periodStart, periodEnd) => {
    const totalIntervals = Math.floor((periodEnd.getTime() - periodStart.getTime()) / (5 * 60 * 1000));
    const intervals = {};
    const tunnelNames = new Set();
    for (const entry of entries) {
      const tunnelName = entry.dimensions?.tunnelName;
      if (!tunnelName) continue;
      const classifiedType = tunnelClassification.get(tunnelName);
      if (classifiedType && classifiedType !== serviceType) continue;
      const time = entry.dimensions?.datetimeFiveMinutes;
      const bitRate = entry.avg?.bitRateFiveMinutes || 0;
      intervals[time] = (intervals[time] || 0) + bitRate;
      tunnelNames.add(tunnelName);
    }
    const samples = [];
    for (let i = 0; i < totalIntervals; i++) {
      const intervalTime = new Date(periodStart.getTime() + i * 5 * 60 * 1000)
        .toISOString()
        .replace('.000Z', 'Z');
      samples.push(intervals[intervalTime] || 0);
    }
    samples.sort((a, b) => a - b);
    const p95Index = Math.floor(samples.length * 0.95);
    const p95Val = samples.length > 0 ? samples[Math.min(p95Index, samples.length - 1)] : 0;
    return { p95: p95Val, tunnelCount: tunnelNames.size };
  };

  // Classify tunnels by IP to separate Magic Transit vs Magic WAN
  const tunnelClassification = await classifyTunnelsByIP(apiKey, accountId, env);
  
  // Fetch current month data - use short-term cache to avoid API inconsistency
  let currentP95Mbps = 0;
  let currentIngressP95Mbps = 0;
  let currentEgressP95Mbps = 0;
  // v13 cache key - windowed pagination + parallel fetches
  const currentMonthCacheKey = `current-v13-${serviceType}:${accountId}:${currentMonthKey}`;
  const cachedCurrentMonth = await env.CONFIG_KV.get(currentMonthCacheKey, 'json');
  
  const CACHE_TTL_MS = 6 * 60 * 60 * 1000; // 6 hours
  
  if (cachedCurrentMonth && cachedCurrentMonth.p95Mbps > 0 && (Date.now() - cachedCurrentMonth.cachedAt) < CACHE_TTL_MS) {
    currentP95Mbps = cachedCurrentMonth.p95Mbps;
    currentIngressP95Mbps = cachedCurrentMonth.ingressP95Mbps || 0;
    currentEgressP95Mbps = cachedCurrentMonth.egressP95Mbps || 0;
    console.log(`${serviceType} current month from cache: ${currentP95Mbps.toFixed(6)} Mbps (age: ${Math.round((Date.now() - cachedCurrentMonth.cachedAt) / 1000)}s)`);
  } else {
    // Fetch fresh data using billing-aligned filters with time-windowed pagination
    // Account-level P95: sum all matching tunnel traffic per 5-min interval, then P95
    // MT main = ingress P95; WAN main = max(ingress P95, egress P95)
    try {
      const filters = BILLING_FILTERS[serviceType];
      
      const [ingressData, egressData] = await Promise.all([
        fetchWindowedData(filters.ingress, currentMonthStart, currentMonthEnd),
        fetchWindowedData(filters.egress, currentMonthStart, currentMonthEnd),
      ]);
      
      const ingressResult = calcAccountLevelP95(ingressData, currentMonthStart, currentMonthEnd);
      const egressResult = calcAccountLevelP95(egressData, currentMonthStart, currentMonthEnd);
      
      currentIngressP95Mbps = ingressResult.p95 / 1e6;
      currentEgressP95Mbps = egressResult.p95 / 1e6;
      if (serviceType === 'magicTransit') {
        currentP95Mbps = currentIngressP95Mbps;
      } else {
        currentP95Mbps = Math.max(currentIngressP95Mbps, currentEgressP95Mbps);
      }
      const tunnelCount = Math.max(ingressResult.tunnelCount, egressResult.tunnelCount);
      const dataIntervalCount = ingressData.length + egressData.length;
      console.log(`${serviceType} FINAL: p95=${currentP95Mbps.toFixed(4)} Mbps (ingress=${currentIngressP95Mbps.toFixed(4)}, egress=${currentEgressP95Mbps.toFixed(4)}, tunnels=${tunnelCount})`);
      
      // Cache the result - but prefer cached non-zero over fresh zero
      const freshGotZero = tunnelCount === 0 || currentP95Mbps === 0;
      const cachedHasValue = cachedCurrentMonth && cachedCurrentMonth.p95Mbps > 0;
      
      if (freshGotZero && cachedHasValue) {
        // Fresh fetch returned 0 but we have cached non-zero - keep cached value
        currentP95Mbps = cachedCurrentMonth.p95Mbps;
        currentIngressP95Mbps = cachedCurrentMonth.ingressP95Mbps || 0;
        currentEgressP95Mbps = cachedCurrentMonth.egressP95Mbps || 0;
        console.log(`${serviceType} fresh fetch returned 0, keeping cached: ${currentP95Mbps.toFixed(6)} Mbps (cached ${Math.round((Date.now() - cachedCurrentMonth.cachedAt) / 1000)}s ago)`);
      } else if (tunnelCount > 0) {
        // Got valid data - cache it
        const cacheData = { 
          p95Mbps: currentP95Mbps, 
          ingressP95Mbps: currentIngressP95Mbps,
          egressP95Mbps: currentEgressP95Mbps,
          intervalCount: dataIntervalCount, 
          tunnelCount, 
          cachedAt: Date.now() 
        };
        await env.CONFIG_KV.put(
          currentMonthCacheKey,
          JSON.stringify(cacheData),
          { expirationTtl: 3600 }
        );
        console.log(`${serviceType} cached: ${currentP95Mbps.toFixed(6)} Mbps (${tunnelCount} tunnels)`);
      } else {
        console.log(`${serviceType} no tunnels found and no cache available`);
      }
    } catch (fetchError) {
      console.error(`${serviceType} fetch error for account ${accountId}:`, fetchError);
      if (cachedCurrentMonth && cachedCurrentMonth.p95Mbps > 0) {
        currentP95Mbps = cachedCurrentMonth.p95Mbps;
        currentIngressP95Mbps = cachedCurrentMonth.ingressP95Mbps || 0;
        currentEgressP95Mbps = cachedCurrentMonth.egressP95Mbps || 0;
        console.log(`${serviceType} fetch failed, falling back to stale cache: ${currentP95Mbps.toFixed(6)} Mbps`);
      } else {
        return null;
      }
    }
  }

  // Get previous month data - first try cache, then fetch from API
  // v6 cache key - windowed pagination + parallel fetches
  const previousMonthCacheKey = `monthly-v6-${serviceType}:${accountId}:${previousMonthKey}`;
  let previousP95Mbps = 0;
  let previousIngressP95Mbps = 0;
  let previousEgressP95Mbps = 0;
  
  const cachedPreviousMonth = await env.CONFIG_KV.get(previousMonthCacheKey, 'json');
  if (cachedPreviousMonth) {
    previousP95Mbps = cachedPreviousMonth.p95Mbps || 0;
    previousIngressP95Mbps = cachedPreviousMonth.ingressP95Mbps || 0;
    previousEgressP95Mbps = cachedPreviousMonth.egressP95Mbps || 0;
    console.log(`${serviceType} previous month from cache: ${previousP95Mbps} Mbps (ingress: ${previousIngressP95Mbps}, egress: ${previousEgressP95Mbps})`);
  } else {
    console.log(`${serviceType} fetching previous month from API for ${previousMonthKey}`);
    try {
      const filters = BILLING_FILTERS[serviceType];
      
      const [prevIngressData, prevEgressData] = await Promise.all([
        fetchWindowedData(filters.ingress, previousMonthStart, previousMonthEnd),
        fetchWindowedData(filters.egress, previousMonthStart, previousMonthEnd),
      ]);
      
      const prevIngressResult = calcAccountLevelP95(prevIngressData, previousMonthStart, previousMonthEnd);
      const prevEgressResult = calcAccountLevelP95(prevEgressData, previousMonthStart, previousMonthEnd);
      
      previousIngressP95Mbps = prevIngressResult.p95 / 1e6;
      previousEgressP95Mbps = prevEgressResult.p95 / 1e6;
      if (serviceType === 'magicTransit') {
        previousP95Mbps = previousIngressP95Mbps;
      } else {
        previousP95Mbps = Math.max(previousIngressP95Mbps, previousEgressP95Mbps);
      }
      const prevTunnelCount = Math.max(prevIngressResult.tunnelCount, prevEgressResult.tunnelCount);
      console.log(`${serviceType} previous month: p95=${previousP95Mbps.toFixed(4)} Mbps (ingress=${previousIngressP95Mbps.toFixed(4)}, egress=${previousEgressP95Mbps.toFixed(4)}, tunnels=${prevTunnelCount})`);
      
      if (prevTunnelCount > 0) {
        await env.CONFIG_KV.put(
          previousMonthCacheKey,
          JSON.stringify({ 
            p95Mbps: previousP95Mbps, 
            ingressP95Mbps: previousIngressP95Mbps,
            egressP95Mbps: previousEgressP95Mbps,
            tunnelCount: prevTunnelCount, 
            cachedAt: Date.now() 
          }),
          { expirationTtl: 31536000 }
        );
      }
    } catch (prevError) {
      console.error(`${serviceType} failed to fetch previous month:`, prevError);
    }
  }

  // Cache current month snapshot at end of month (day >= 28)
  if (now.getDate() >= 28) {
    const snapshotKey = `monthly-v6-${serviceType}:${accountId}:${currentMonthKey}`;
    const existingSnapshot = await env.CONFIG_KV.get(snapshotKey, 'json');
    if (!existingSnapshot) {
      try {
        await env.CONFIG_KV.put(
          snapshotKey,
          JSON.stringify({
            p95Mbps: currentP95Mbps,
            ingressP95Mbps: currentIngressP95Mbps,
            egressP95Mbps: currentEgressP95Mbps,
            cachedAt: Date.now()
          }),
          { expirationTtl: 31536000 }
        );
        console.log(`Cached ${serviceType} snapshot for ${currentMonthKey}: p95=${currentP95Mbps}, ingress=${currentIngressP95Mbps}, egress=${currentEgressP95Mbps}`);
      } catch (cacheError) {
        console.error(`Failed to cache ${serviceType} bandwidth snapshot:`, cacheError);
      }
    }
  }

  // Load historical data for time series
  const historicalData = await getHistoricalMagicBandwidthData(env, accountId, serviceType);

  // Build time series
  const timeSeries = [
    ...historicalData,
    {
      month: currentMonthKey,
      timestamp: new Date(now.getFullYear(), now.getMonth(), 1).toISOString(),
      p95Mbps: currentP95Mbps,
      ingressP95Mbps: currentIngressP95Mbps,
      egressP95Mbps: currentEgressP95Mbps,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  // Deduplicate by month (keep latest)
  const timeSeriesMap = new Map();
  timeSeries.forEach(entry => {
    timeSeriesMap.set(entry.month, entry);
  });
  const deduplicatedTimeSeries = Array.from(timeSeriesMap.values())
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  // Build return object - always include ingress/egress breakdown (frontend decides what to show)
  console.log(`${serviceType} RETURN DATA - current: p95=${currentP95Mbps}, ingress=${currentIngressP95Mbps}, egress=${currentEgressP95Mbps}`);
  console.log(`${serviceType} RETURN DATA - previous: p95=${previousP95Mbps}, ingress=${previousIngressP95Mbps}, egress=${previousEgressP95Mbps}`);
  return {
    current: { 
      p95Mbps: currentP95Mbps,
      ingressP95Mbps: currentIngressP95Mbps,
      egressP95Mbps: currentEgressP95Mbps,
    },
    previous: { 
      p95Mbps: previousP95Mbps,
      ingressP95Mbps: previousIngressP95Mbps,
      egressP95Mbps: previousEgressP95Mbps,
    },
    timeSeries: deduplicatedTimeSeries,
  };
}

/**
 * Get historical Magic Transit/WAN bandwidth data from KV
 */
async function getHistoricalMagicBandwidthData(env, accountId, serviceType) {
  const historicalData = [];
  
  try {
    const prefixes = [
      `monthly-v6-${serviceType}:${accountId}:`,
      `monthly-v5-${serviceType}:${accountId}:`,
    ];
    
    const seen = new Set();
    for (const prefix of prefixes) {
      const listResult = await env.CONFIG_KV.list({ prefix });
      for (const key of listResult.keys) {
        const data = await env.CONFIG_KV.get(key.name, 'json');
        if (data) {
          const month = key.name.split(':')[2];
          if (seen.has(month)) continue;
          seen.add(month);
          const [year, monthNum] = month.split('-');
          const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
          historicalData.push({
            month,
            timestamp,
            p95Mbps: data.p95Mbps || 0,
            ingressP95Mbps: data.ingressP95Mbps || data.p95Mbps || 0,
            egressP95Mbps: data.egressP95Mbps || 0,
          });
        }
      }
    }
  } catch (error) {
    console.error(`Error loading historical ${serviceType} bandwidth data:`, error);
  }
  
  return historicalData;
}

/**
 * Calculate add-on metrics from existing zone data (API Shield, Page Shield, Advanced Rate Limiting)
 * These add-ons use HTTP request data we already have - just filter by configured zones!
 */
async function calculateZoneBasedAddonForAccount(accountData, addonConfig, env, addonType) {
  if (!addonConfig || !addonConfig.enabled) {
    return null;
  }

  if (!addonConfig.zones || addonConfig.zones.length === 0) {
    console.log(`${addonType}: No zones configured for account ${accountData.accountId}, skipping`);
    return null;
  }

  const configuredZones = new Set(addonConfig.zones);
  
  // Filter current month zones to only those configured for this add-on
  const currentZones = (accountData.zoneBreakdown?.zones || [])
    .filter(zone => configuredZones.has(zone.zoneTag))
    .map(zone => ({
      zoneId: zone.zoneTag,
      zoneName: zone.zoneName || zone.zoneTag,
      requests: zone.requests || 0,
    }));
  
  // If no configured zones belong to this account, return null
  if (currentZones.length === 0) {
    console.log(`${addonType}: No configured zones found in account ${accountData.accountId}, skipping`);
    return null;
  }
  
  // Filter previous month zones
  const previousZones = (accountData.previousMonthZoneBreakdown?.zones || [])
    .filter(zone => configuredZones.has(zone.zoneTag))
    .map(zone => ({
      zoneId: zone.zoneTag,
      zoneName: zone.zoneName || zone.zoneTag,
      requests: zone.requests || 0,
    }));
  
  // Sum up requests for configured zones
  const currentTotal = currentZones.reduce((sum, zone) => sum + (zone.requests || 0), 0);
  const previousTotal = previousZones.reduce((sum, zone) => sum + (zone.requests || 0), 0);
  
  // Zone-based SKUs inherit confidence from HTTP request data
  // Since these are just HTTP requests filtered by zone, use the account's overall HTTP request confidence
  // This is appropriate because:
  // 1. These are HTTP requests (same data source as core HTTP metrics)
  // 2. Sampling applies equally to all zones
  // 3. The confidence represents the accuracy of the request counts
  const confidence = accountData.current?.confidence?.requests || null;
  
  // Load historical data from KV
  const historicalData = await getHistoricalAddonData(env, accountData.accountId, addonType);
  
  // Build timeSeries (include both previous and current month!)
  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;
  
  const timeSeries = [
    ...historicalData,
    // ✅ Add previous month (we have this data!)
    {
      month: previousMonthKey,
      timestamp: previousMonthStart.toISOString(),
      requests: previousTotal,
    },
    // ✅ Add current month
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      requests: currentTotal,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  // Store previous month data in KV if we're past day 2 of current month
  const kvPrefix = `monthly-${addonType.toLowerCase().replace(/\s+/g, '-')}-stats`;
  
  if (now.getDate() >= 2 && previousTotal > 0) {
    try {
      await env.CONFIG_KV.put(
        `${kvPrefix}:${accountData.accountId}:${previousMonthKey}`,
        JSON.stringify({
          requests: previousTotal,
          zones: previousZones,
        }),
        { expirationTtl: 31536000 } // 1 year
      );
      console.log(`Stored ${addonType} stats for ${previousMonthKey}`);
    } catch (error) {
      console.error(`Failed to store ${addonType} monthly stats:`, error);
    }
  }

  return {
    current: {
      requests: currentTotal,
      zones: currentZones,
      confidence: confidence,
    },
    previous: {
      requests: previousTotal,
      zones: previousZones,
    },
    timeSeries,
  };
}

async function calculateArgoForAccount(accountData, argoConfig, env) {
  if (!argoConfig || !argoConfig.enabled) {
    return null;
  }

  if (!argoConfig.zones || argoConfig.zones.length === 0) {
    console.log(`argo: No zones configured for account ${accountData.accountId}, skipping`);
    return null;
  }

  const configuredZones = new Set(argoConfig.zones);

  const currentZones = (accountData.zoneBreakdown?.zones || [])
    .filter(zone => configuredZones.has(zone.zoneTag))
    .map(zone => ({
      zoneId: zone.zoneTag,
      zoneName: zone.zoneName || zone.zoneTag,
      bytes: zone.bytes || 0,
    }));

  if (currentZones.length === 0) {
    console.log(`argo: No configured zones found in account ${accountData.accountId}, skipping`);
    return null;
  }

  const previousZones = (accountData.previousMonthZoneBreakdown?.zones || [])
    .filter(zone => configuredZones.has(zone.zoneTag))
    .map(zone => ({
      zoneId: zone.zoneTag,
      zoneName: zone.zoneName || zone.zoneTag,
      bytes: zone.bytes || 0,
    }));

  const currentTotal = currentZones.reduce((sum, zone) => sum + (zone.bytes || 0), 0);
  const previousTotal = previousZones.reduce((sum, zone) => sum + (zone.bytes || 0), 0);

  const confidence = accountData.current?.confidence?.bytes || null;

  const historicalData = await getHistoricalAddonData(env, accountData.accountId, 'argo');

  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;

  const timeSeries = [
    ...historicalData,
    {
      month: previousMonthKey,
      timestamp: previousMonthStart.toISOString(),
      bytes: previousTotal,
    },
    {
      month: currentMonthKey,
      timestamp: currentMonthStart.toISOString(),
      bytes: currentTotal,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  const kvPrefix = 'monthly-argo-stats';
  if (now.getDate() >= 2 && previousTotal > 0) {
    try {
      await env.CONFIG_KV.put(
        `${kvPrefix}:${accountData.accountId}:${previousMonthKey}`,
        JSON.stringify({
          bytes: previousTotal,
          zones: previousZones,
        }),
        { expirationTtl: 31536000 }
      );
      console.log(`Stored argo stats for ${previousMonthKey}`);
    } catch (error) {
      console.error('Failed to store argo monthly stats:', error);
    }
  }

  return {
    current: {
      bytes: currentTotal,
      zones: currentZones,
      confidence: confidence,
    },
    previous: {
      bytes: previousTotal,
      zones: previousZones,
    },
    timeSeries,
  };
}

/**
 * Get historical addon data from KV (cached for 6 hours)
 */
async function getHistoricalAddonData(env, accountId, addonType) {
  const kvPrefix = `monthly-${addonType.toLowerCase().replace(/\s+/g, '-')}-stats`;
  const cacheKey = `historical-${addonType.toLowerCase().replace(/\s+/g, '-')}-data:${accountId}`;
  const cached = await env.CONFIG_KV.get(cacheKey, 'json');
  
  if (cached && cached.cachedAt) {
    const cacheAge = Date.now() - cached.cachedAt;
    if (cacheAge < 6 * 60 * 60 * 1000) { // 6 hours
      console.log(`Using cached historical ${addonType} data for account ${accountId}`);
      return cached.data;
    }
  }
  
  const historicalData = [];
  const listResult = await env.CONFIG_KV.list({ prefix: `${kvPrefix}:${accountId}:` });
  
  for (const key of listResult.keys) {
    const data = await env.CONFIG_KV.get(key.name, 'json');
    if (data) {
      const month = key.name.split(':')[2];
      const [year, monthNum] = month.split('-');
      const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
      
      historicalData.push({
        month,
        timestamp,
        requests: data.requests || 0,
        bytes: data.bytes || 0,
      });
    }
  }
  
  // Cache the historical data
  try {
    await env.CONFIG_KV.put(
      cacheKey,
      JSON.stringify({
        cachedAt: Date.now(),
        data: historicalData
      }),
      { expirationTtl: 21600 } // 6 hours
    );
  } catch (cacheError) {
    console.error(`Failed to cache historical ${addonType} data:`, cacheError);
  }
  
  return historicalData;
}

/**
 * Get all historical monthly data from KV (cached for 6 hours)
 */
async function getHistoricalMonthlyData(env, accountId) {
  // Check cache first (6 hour TTL)
  const cacheKey = `historical-data:${accountId}`;
  const cached = await env.CONFIG_KV.get(cacheKey, 'json');
  
  if (cached && cached.cachedAt) {
    const cacheAge = Date.now() - cached.cachedAt;
    if (cacheAge < 6 * 60 * 60 * 1000) { // 6 hours
      console.log(`Using cached historical data for account ${accountId} (age: ${Math.floor(cacheAge / 3600000)}h)`);
      return cached.data;
    }
  }
  
  const historicalData = [];
  
  // List all monthly-stats keys for this account
  const listResult = await env.CONFIG_KV.list({ prefix: `monthly-stats:${accountId}:` });
  
  for (const key of listResult.keys) {
    const data = await env.CONFIG_KV.get(key.name, 'json');
    if (data) {
      // Extract month from key: monthly-stats:{accountId}:YYYY-MM
      const month = key.name.split(':')[2];
      const [year, monthNum] = month.split('-');
      const timestamp = new Date(parseInt(year), parseInt(monthNum) - 1, 1).toISOString();
      
      historicalData.push({
        month,
        timestamp,
        requests: data.requests || 0,
        bytes: data.bytes || 0,
        dnsQueries: data.dnsQueries || 0,
      });
    }
  }
  
  // Cache the historical data (6 hour TTL)
  try {
    await env.CONFIG_KV.put(
      cacheKey,
      JSON.stringify({
        cachedAt: Date.now(),
        data: historicalData
      }),
      { expirationTtl: 21600 } // 6 hours
    );
    console.log(`Cached historical data for account ${accountId}`);
  } catch (cacheError) {
    console.error('Failed to cache historical data:', cacheError);
  }
  
  return historicalData;
}

/**
 * Test firewall query to debug the correct syntax
 */
async function testFirewallQuery(request, env, corsHeaders) {
  const body = await request.json();
  const apiKey = env.CLOUDFLARE_API_TOKEN;
  const accountId = body.accountId;  // From request body/KV
  
  if (!apiKey) {
    return new Response(JSON.stringify({ error: 'API token not configured. Set it using: npx wrangler secret put CLOUDFLARE_API_TOKEN' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  if (!accountId) {
    return new Response(JSON.stringify({ error: 'Account ID required' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  // Get Enterprise zones
  const zonesResponse = await fetch(`https://api.cloudflare.com/client/v4/zones?per_page=1000`, {
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
  });

  const zonesData = await zonesResponse.json();
  const zones = zonesData.result || [];
  const enterpriseZones = zones.filter(zone => zone.plan?.legacy_id === 'enterprise' || zone.plan?.name === 'Enterprise Website');
  const zoneIds = enterpriseZones.map(z => z.id);

  if (zoneIds.length === 0) {
    return new Response(JSON.stringify({ error: 'No enterprise zones found' }), {
      status: 404,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  // Get date range (current month for testing)
  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59, 999);
  
  const dateStart = currentMonthStart.toISOString().split('T')[0];
  const dateEnd = currentMonthEnd.toISOString().split('T')[0];
  const datetimeStart = currentMonthStart.toISOString();
  const datetimeEnd = currentMonthEnd.toISOString();

  // Try different query variations
  const queries = [
    {
      name: 'firewallEventsAdaptiveGroups with date',
      query: `query TestFirewall($zoneIds: [String!]!, $dateStart: String!, $dateEnd: String!) {
        viewer {
          zones(filter: {zoneTag_in: $zoneIds}) {
            zoneTag
            firewallEventsAdaptiveGroups(
              filter: { date_geq: $dateStart, date_leq: $dateEnd },
              limit: 10
            ) {
              count
              dimensions { action source }
            }
          }
        }
      }`,
      variables: { zoneIds, dateStart, dateEnd }
    },
    {
      name: 'firewallEventsAdaptiveGroups with datetime',
      query: `query TestFirewall($zoneIds: [String!]!, $datetimeStart: String!, $datetimeEnd: String!) {
        viewer {
          zones(filter: {zoneTag_in: $zoneIds}) {
            zoneTag
            firewallEventsAdaptiveGroups(
              filter: { datetime_geq: $datetimeStart, datetime_leq: $datetimeEnd },
              limit: 10
            ) {
              count
              dimensions { action source }
            }
          }
        }
      }`,
      variables: { zoneIds, datetimeStart, datetimeEnd }
    },
    {
      name: 'firewallEventsAdaptive (no Groups)',
      query: `query TestFirewall($zoneIds: [String!]!, $datetimeStart: String!, $datetimeEnd: String!) {
        viewer {
          zones(filter: {zoneTag_in: $zoneIds}) {
            zoneTag
            firewallEventsAdaptive(
              filter: { datetime_geq: $datetimeStart, datetime_leq: $datetimeEnd },
              limit: 10
            ) {
              action
              source
            }
          }
        }
      }`,
      variables: { zoneIds, datetimeStart, datetimeEnd }
    }
  ];

  const results = [];

  for (const testQuery of queries) {
    try {
      const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
          query: testQuery.query,
          variables: testQuery.variables,
          operationName: 'TestFirewall'
        }),
      });

      const data = await response.json();
      
      results.push({
        name: testQuery.name,
        success: response.ok && !data.errors,
        status: response.status,
        data: data,
        sampleData: data.data?.viewer?.zones?.[0]
      });
    } catch (error) {
      results.push({
        name: testQuery.name,
        success: false,
        error: error.message
      });
    }
  }

  return new Response(
    JSON.stringify({
      message: 'Tested multiple firewall query variations',
      dateRange: { dateStart, dateEnd, datetimeStart, datetimeEnd },
      enterpriseZones: zoneIds.length,
      results: results
    }, null, 2),
    {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    }
  );
}

/**
 * Manual trigger for cache pre-warming (API endpoint)
 */
async function triggerPrewarm(request, env, corsHeaders) {
  try {
    console.log('🔥 Manual cache pre-warm triggered via API');
    
    // Run pre-warm in background
    const startTime = Date.now();
    await preWarmCache(env);
    const duration = Date.now() - startTime;
    
    return new Response(
      JSON.stringify({ 
        success: true,
        message: 'Cache pre-warming completed successfully',
        duration: `${(duration / 1000).toFixed(2)}s`
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error('Pre-warm trigger error:', error);
    return new Response(
      JSON.stringify({ 
        success: false,
        error: error.message 
      }),
      { 
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      }
    );
  }
}

/**
 * Pre-warm cache (triggered by Cron every 6 hours)
 * Fetches and caches all dashboard data so subsequent loads are instant
 * This is the SECRET SAUCE for scaling to many SKUs! 🚀
 */
async function preWarmCache(env) {
  try {
    console.log('🔥 Pre-warming cache started...');
    
    // Get configuration to know which accounts to fetch
    const configData = await env.CONFIG_KV.get('config:default');
    if (!configData) {
      console.log('Pre-warm: No configuration found, skipping');
      return;
    }

    const config = JSON.parse(configData);
    const apiKey = env.CLOUDFLARE_API_TOKEN;
    const accountIds = config.accountIds || (config.accountId ? [config.accountId] : []);
    
    if (!apiKey) {
      console.log('Pre-warm: API token not configured, skipping');
      return;
    }

    if (accountIds.length === 0) {
      console.log('Pre-warm: No account IDs configured, skipping');
      return;
    }

    const startTime = Date.now();
    console.log(`Pre-warm: Fetching data for ${accountIds.length} account(s)...`);
    const prewarmData = await fetchAllMetrics(apiKey, accountIds, config, env);
    const prewarmCacheKey = `pre-warmed:${accountIds.join(',')}`;
    await env.CONFIG_KV.put(prewarmCacheKey, JSON.stringify({ timestamp: Date.now(), data: prewarmData }), { expirationTtl: 6 * 60 * 60 });
    const prewarmDuration = Date.now() - startTime;
    console.log(`✅ Pre-warm complete! Cached in ${(prewarmDuration / 1000).toFixed(1)}s. Next dashboard load will be INSTANT! ⚡`);
    return;
  } catch (error) {
    console.error('Pre-warm cache error:', error);
  }
}

/**
 * Run scheduled threshold check (triggered by Cron)
 * Checks thresholds automatically every 6 hours without dashboard being open
 */
async function runScheduledThresholdCheck(env) {
  try {
    // Get configuration for default user
    const configData = await env.CONFIG_KV.get('config:default');
    if (!configData) {
      console.log('Scheduled check: No configuration found');
      return;
    }

    const config = JSON.parse(configData);
    
    // Only run if alerts are enabled and Slack webhook is configured
    if (!config.alertsEnabled || !config.slackWebhook) {
      console.log('Scheduled check: Alerts not enabled or no Slack webhook configured');
      return;
    }

    // API Token: Read from wrangler secret (secure storage)
    const apiKey = env.CLOUDFLARE_API_TOKEN;
    const accountIds = config.accountIds || (config.accountId ? [config.accountId] : []);
    
    if (!apiKey) {
      console.log('Scheduled check: API token not configured');
      return;
    }

    if (accountIds.length === 0) {
      console.log('Scheduled check: No account IDs configured');
      return;
    }

    console.log(`Scheduled check: Running for ${accountIds.length} account(s)`);

    const cacheKey = `pre-warmed:${accountIds.join(',')}`;
    const cachedData = await env.CONFIG_KV.get(cacheKey, 'json');

    if (!cachedData || !cachedData.data) {
      console.log('Scheduled check: No pre-warmed cache available, skipping');
      return;
    }

    const m = cachedData.data;
    const skuMetrics = [];
    const fN = (v) => { if (v >= 1e9) return `${(v/1e9).toFixed(2)}B`; if (v >= 1e6) return `${(v/1e6).toFixed(2)}M`; if (v >= 1e3) return `${(v/1e3).toFixed(1)}K`; return String(Math.round(v)); };
    const fB = (b) => { if (b >= 1e12) return `${(b/1e12).toFixed(2)} TB`; if (b >= 1e9) return `${(b/1e9).toFixed(2)} GB`; if (b >= 1e6) return `${(b/1e6).toFixed(2)} MB`; return `${b} B`; };
    const fG = (gb) => gb >= 1000 ? `${(gb/1000).toFixed(2)} TB` : `${gb.toFixed(2)} GB`;
    const fMB = (mb) => mb >= 1000 ? fG(mb/1000) : `${mb.toFixed(2)} MB`;
    const fMn = (v) => v >= 1e6 ? `${(v/1e6).toFixed(2)}M min` : v >= 1e3 ? `${(v/1e3).toFixed(1)}K min` : `${Math.round(v)} min`;
    const fBw = (v) => v >= 1000 ? `${(v/1000).toFixed(2)} Gbps` : `${v.toFixed(2)} Mbps`;
    const a = (key, name, cat, cur, thr, fmt, thrFmt) => {
      if (cur !== undefined && cur !== null) {
        skuMetrics.push({ key, name, category: cat, current: cur, threshold: thr || null, formatted: fmt, thresholdFormatted: thrFmt || '', percentage: thr ? (cur / thr) * 100 : 0 });
      }
    };

    const appCfg = config.applicationServices || {};
    const zc = m.zonesCount || 0; const zt = appCfg.core?.thresholdZones || config.thresholdZones;
    a('zones', 'Enterprise Zones', 'Zones & Traffic', zc, zt, `${zc} zones`, zt ? `${zt} zones` : '');
    const rq = m.current?.requests || 0; const rqt = appCfg.core?.thresholdRequests || config.thresholdRequests;
    a('requests', 'HTTP Requests', 'Zones & Traffic', rq, rqt, fN(rq), rqt ? fN(rqt) : '');
    const bw = m.current?.bytes || 0; const bwt = appCfg.core?.thresholdBandwidth || config.thresholdBandwidth;
    a('bandwidth', 'Data Transfer', 'Zones & Traffic', bw, bwt, fB(bw), bwt ? fB(bwt) : '');
    const dns = m.current?.dnsQueries || 0; const dnst = appCfg.core?.thresholdDnsQueries || config.thresholdDnsQueries;
    a('dnsQueries', 'DNS Queries', 'Zones & Traffic', dns, dnst, fN(dns), dnst ? fN(dnst) : '');

    if (m.botManagement?.enabled) { const c = m.botManagement.current?.likelyHuman||0, t = appCfg.botManagement?.threshold; a('botManagement', 'Bot Management', 'Application Security', c, t, fN(c), t?fN(t):''); }
    if (m.apiShield?.enabled) { const c = m.apiShield.current?.requests||0, t = appCfg.apiShield?.threshold; a('apiShield', 'API Shield', 'Application Security', c, t, fN(c), t?fN(t):''); }
    if (m.pageShield?.enabled) { const c = m.pageShield.current?.requests||0, t = appCfg.pageShield?.threshold; a('pageShield', 'Page Shield', 'Application Security', c, t, fN(c), t?fN(t):''); }
    if (m.advancedRateLimiting?.enabled) { const c = m.advancedRateLimiting.current?.requests||0, t = appCfg.advancedRateLimiting?.threshold; a('advancedRateLimiting', 'Adv. Rate Limiting', 'Application Security', c, t, fN(c), t?fN(t):''); }
    if (m.argo?.enabled) { const c = m.argo.current?.bytes||0, t = appCfg.argo?.threshold; a('argo', 'Argo Smart Routing', 'Delivery & Performance', c, t, fB(c), t?fB(t):''); }
    if (m.cacheReserve?.enabled) {
      const cr = m.cacheReserve, cfg = appCfg.cacheReserve||{};
      const crGB = cr.current?.storageGBDays||0; const crThr = cfg.storageThreshold?cfg.storageThreshold*1000:null;
      a('cacheReserve-storage', 'Cache Reserve — Storage', 'Delivery & Performance', crGB, crThr, crGB>=1000?`${(crGB/1000).toFixed(2)} TB`:`${crGB.toFixed(2)} GB`, crThr?(crThr>=1000?`${(crThr/1000).toFixed(2)} TB`:`${crThr.toFixed(2)} GB`):'');
      a('cacheReserve-classA', 'Cache Reserve — Class A Ops', 'Delivery & Performance', cr.current?.classAOps||0, cfg.classAOpsThreshold, fN(cr.current?.classAOps||0), cfg.classAOpsThreshold?fN(cfg.classAOpsThreshold):'');
      a('cacheReserve-classB', 'Cache Reserve — Class B Ops', 'Delivery & Performance', cr.current?.classBOps||0, cfg.classBOpsThreshold, fN(cr.current?.classBOps||0), cfg.classBOpsThreshold?fN(cfg.classBOpsThreshold):'');
    }
    if (m.loadBalancing?.enabled) { const c = m.loadBalancing.current?.endpoints||0, t = appCfg.loadBalancing?.threshold; a('loadBalancing', 'Load Balancing', 'Delivery & Performance', c, t, `${c} endpoints`, t?`${t} endpoints`:''); }
    if (m.customHostnames?.enabled) { const c = m.customHostnames.current?.hostnames||0, t = appCfg.customHostnames?.threshold; a('customHostnames', 'Custom Hostnames', 'Delivery & Performance', c, t, `${c} hostnames`, t?`${t} hostnames`:''); }
    if (m.logExplorer?.enabled) { const c = m.logExplorer.current?.billableGB||0, t = appCfg.logExplorer?.threshold; a('logExplorer', 'Log Explorer', 'Logs & Analytics', c, t, fG(c), t?fG(t):''); }

    const ztCfg = config.zeroTrust || {};
    if (m.zeroTrustSeats?.enabled) { const c = m.zeroTrustSeats.current?.seats||0, t = ztCfg.seats?.threshold; a('zeroTrustSeats', 'Zero Trust Seats', 'Cloudflare One', c, t, `${c} seats`, t?`${t} seats`:''); }

    const netCfg = config.networkServices || {};
    if (m.magicTransit?.enabled) {
      const c = m.magicTransit.current?.ingressP95Mbps||0, t = netCfg.magicTransit?.threshold;
      a('magicTransit', 'Magic Transit — Ingress', 'Network Services', c, t, fBw(c), t?fBw(t):'');
      if (netCfg.magicTransit?.egressEnabled) { const ec = m.magicTransit.current?.egressP95Mbps||0, et = netCfg.magicTransit?.egressThreshold; a('magicTransit-egress', 'Magic Transit — Egress', 'Network Services', ec, et, fBw(ec), et?fBw(et):''); }
    }
    if (m.magicWan?.enabled) { const c = m.magicWan.current?.p95Mbps||0, t = netCfg.magicWan?.threshold; a('magicWan', 'Magic WAN', 'Cloudflare One', c, t, fBw(c), t?fBw(t):''); }
    if (m.spectrum?.enabled) {
      const sp = m.spectrum, cfg = netCfg.spectrum||{};
      const spdt = cfg.dataTransferThreshold ? cfg.dataTransferThreshold*1e12 : null;
      a('spectrum-transfer', 'Spectrum — Data Transfer', 'Network Services', sp.current?.dataTransfer||0, spdt, fB(sp.current?.dataTransfer||0), spdt?fB(spdt):'');
      a('spectrum-conns', 'Spectrum — Connections', 'Network Services', sp.current?.p95Concurrent||0, cfg.connectionsThreshold, fN(sp.current?.p95Concurrent||0), cfg.connectionsThreshold?fN(cfg.connectionsThreshold):'');
    }

    const devCfg = config.developerServices || {};
    if (m.workersPages?.enabled) {
      const wp = m.workersPages, cfg = devCfg.workersPages||{};
      const wpt = cfg.requestsThreshold?cfg.requestsThreshold*1e6:null;
      const wpct = cfg.cpuTimeThreshold?cfg.cpuTimeThreshold*1e6:null;
      a('workersPages-req', 'Workers & Pages — Requests', 'Developer Platform', wp.current?.requests||0, wpt, fN(wp.current?.requests||0), wpt?fN(wpt):'');
      a('workersPages-cpu', 'Workers & Pages — CPU Time', 'Developer Platform', wp.current?.cpuTimeMs||0, wpct, fN(wp.current?.cpuTimeMs||0)+' ms', wpct?fN(wpct)+' ms':'');
    }
    if (m.r2Storage?.enabled) {
      const r2 = m.r2Storage, cfg = devCfg.r2Storage||{};
      const r2at = cfg.classAOpsThreshold?cfg.classAOpsThreshold*1e6:null;
      const r2bt = cfg.classBOpsThreshold?cfg.classBOpsThreshold*1e6:null;
      const r2st = cfg.storageThreshold?cfg.storageThreshold*1000:null;
      a('r2-storage', 'R2 — Storage', 'Developer Platform', r2.current?.storageGB||0, r2st, fG(r2.current?.storageGB||0), r2st?fG(r2st):'');
      a('r2-classA', 'R2 — Class A Ops', 'Developer Platform', r2.current?.classAOps||0, r2at, fN(r2.current?.classAOps||0), r2at?fN(r2at):'');
      a('r2-classB', 'R2 — Class B Ops', 'Developer Platform', r2.current?.classBOps||0, r2bt, fN(r2.current?.classBOps||0), r2bt?fN(r2bt):'');
    }
    if (m.d1?.enabled) {
      const d = m.d1, cfg = devCfg.d1||{};
      const d1rr = cfg.rowsReadThreshold?cfg.rowsReadThreshold*1e6:null;
      const d1rw = cfg.rowsWrittenThreshold?cfg.rowsWrittenThreshold*1e6:null;
      const d1mb = d.current?.storageMB||0; const d1st = cfg.storageThreshold?cfg.storageThreshold*1000:null;
      a('d1-storage', 'D1 — Storage', 'Developer Platform', d1mb, d1st, fMB(d1mb), d1st?fMB(d1st):'');
      a('d1-rowsRead', 'D1 — Rows Read', 'Developer Platform', d.current?.rowsRead||0, d1rr, fN(d.current?.rowsRead||0), d1rr?fN(d1rr):'');
      a('d1-rowsWritten', 'D1 — Rows Written', 'Developer Platform', d.current?.rowsWritten||0, d1rw, fN(d.current?.rowsWritten||0), d1rw?fN(d1rw):'');
    }
    if (m.kv?.enabled) {
      const k = m.kv, cfg = devCfg.kv||{};
      const kvr = cfg.readsThreshold?cfg.readsThreshold*1e6:null;
      const kvw = cfg.writesThreshold?cfg.writesThreshold*1e6:null;
      const kvd = cfg.deletesThreshold?cfg.deletesThreshold*1e6:null;
      const kvl = cfg.listsThreshold?cfg.listsThreshold*1e6:null;
      a('kv-reads', 'KV — Reads', 'Developer Platform', k.current?.reads||0, kvr, fN(k.current?.reads||0), kvr?fN(kvr):'');
      a('kv-writes', 'KV — Writes', 'Developer Platform', k.current?.writes||0, kvw, fN(k.current?.writes||0), kvw?fN(kvw):'');
      a('kv-deletes', 'KV — Deletes', 'Developer Platform', k.current?.deletes||0, kvd, fN(k.current?.deletes||0), kvd?fN(kvd):'');
      a('kv-lists', 'KV — Lists', 'Developer Platform', k.current?.lists||0, kvl, fN(k.current?.lists||0), kvl?fN(kvl):'');
      const kvst = cfg.storageThreshold?cfg.storageThreshold*1000:null;
      a('kv-storage', 'KV — Storage', 'Developer Platform', k.current?.storageMB||0, kvst, fMB(k.current?.storageMB||0), kvst?fMB(kvst):'');
    }
    if (m.stream?.enabled) {
      const s = m.stream, cfg = devCfg.stream||{};
      const sst = cfg.minutesStoredThreshold?cfg.minutesStoredThreshold*1e3:null;
      const sdt = cfg.minutesDeliveredThreshold?cfg.minutesDeliveredThreshold*1e3:null;
      a('stream-stored', 'Stream — Min Stored', 'Developer Platform', s.current?.minutesStored||0, sst, fMn(s.current?.minutesStored||0), sst?fMn(sst):'');
      a('stream-delivered', 'Stream — Min Delivered', 'Developer Platform', s.current?.minutesDelivered||0, sdt, fMn(s.current?.minutesDelivered||0), sdt?fMn(sdt):'');
    }
    if (m.images?.enabled) {
      const im = m.images, cfg = devCfg.images||{};
      const ist = cfg.imagesStoredThreshold?cfg.imagesStoredThreshold*1e3:null;
      const idt = cfg.imagesDeliveredThreshold?cfg.imagesDeliveredThreshold*1e3:null;
      a('images-stored', 'Images — Stored', 'Developer Platform', im.current?.imagesStored||0, ist, fN(im.current?.imagesStored||0), ist?fN(ist):'');
      a('images-delivered', 'Images — Delivered', 'Developer Platform', im.current?.imagesDelivered||0, idt, fN(im.current?.imagesDelivered||0), idt?fN(idt):'');
    }
    if (m.workersAI?.enabled) { const c = m.workersAI.current?.neurons||0, t = devCfg.workersAI?.neuronsThreshold?devCfg.workersAI.neuronsThreshold*1e6:null; a('workersAI', 'Workers AI', 'Developer Platform', c, t, fN(c)+' neurons', t?fN(t)+' neurons':''); }
    if (m.queues?.enabled) { const c = m.queues.current?.operations||0, t = devCfg.queues?.operationsThreshold?devCfg.queues.operationsThreshold*1e6:null; a('queues', 'Queues', 'Developer Platform', c, t, fN(c)+' ops', t?fN(t)+' ops':''); }
    if (m.workersLogsTraces?.enabled) { const c = m.workersLogsTraces.current?.events||0, t = devCfg.workersLogsTraces?.eventsThreshold?devCfg.workersLogsTraces.eventsThreshold*1e6:null; a('workersLogsTraces', 'Workers Observability', 'Developer Platform', c, t, fN(c)+' events', t?fN(t)+' events':''); }
    if (m.durableObjects?.enabled) {
      const d = m.durableObjects, cfg = devCfg.durableObjects||{};
      const dort = cfg.requestsThreshold?cfg.requestsThreshold*1e6:null;
      const dodt = cfg.durationThreshold?cfg.durationThreshold*1e3:null;
      const fGBs = (v) => v>=1e6?`${(v/1e6).toFixed(2)}M GB-s`:v>=1e3?`${(v/1e3).toFixed(2)}K GB-s`:`${v.toFixed(2)} GB-s`;
      a('do-requests', 'Durable Objects — Requests', 'Developer Platform', d.current?.requests||0, dort, fN(d.current?.requests||0), dort?fN(dort):'');
      a('do-duration', 'Durable Objects — Duration', 'Developer Platform', d.current?.durationGBs||0, cfg.durationThreshold, fGBs(d.current?.durationGBs||0), cfg.durationThreshold?fGBs(cfg.durationThreshold):'');
      const domb = d.current?.storageMB||0; const dost = cfg.storageThreshold?cfg.storageThreshold*1000:null;
      a('do-storage', 'Durable Objects — Storage', 'Developer Platform', domb, dost, fMB(domb), dost?fMB(dost):'');
    }

    const alerts = skuMetrics.filter(s => s.threshold && s.percentage >= 90);
    console.log(`Scheduled check: ${skuMetrics.length} SKUs evaluated, ${alerts.length} at ≥90%`);

    if (alerts.length === 0) {
      console.log('Scheduled check: All metrics within thresholds');
      return;
    }

    const now = new Date();
    const freq = config.alertFrequency || 'monthly';
    let periodKey;
    if (freq === 'weekly') {
      const startOfYear = new Date(now.getFullYear(), 0, 1);
      const weekNum = Math.ceil(((now - startOfYear) / 86400000 + startOfYear.getDay() + 1) / 7);
      periodKey = `${now.getFullYear()}-W${String(weekNum).padStart(2, '0')}`;
    } else {
      periodKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    }

    const accountsKey = accountIds.sort().join('-');
    const newAlerts = [];
    for (const alert of alerts) {
      const alertKey = `alert-sent:${accountsKey}:${alert.key}:${periodKey}`;
      const alreadySent = await env.CONFIG_KV.get(alertKey);
      if (!alreadySent) {
        newAlerts.push(alert);
        await env.CONFIG_KV.put(alertKey, 'true', { expirationTtl: 3888000 });
      }
    }

    if (newAlerts.length === 0) {
      console.log(`Scheduled check: All ${alerts.length} alert(s) already sent ${freq === 'weekly' ? 'this week' : 'this month'}`);
      return;
    }

    const dashboardUrl = 'https://enterprise-usage-dashboard.felipe-cloudflare.workers.dev';
    try {
      await sendSlackAlert(newAlerts, config.slackWebhook, dashboardUrl);
      console.log(`Scheduled check: Sent ${newAlerts.length} alert(s) to Slack`);
    } catch (slackErr) {
      console.error('Scheduled check: Failed to send Slack alert:', slackErr.message);
    }
  } catch (error) {
    console.error('Scheduled check error:', error);
  }
}

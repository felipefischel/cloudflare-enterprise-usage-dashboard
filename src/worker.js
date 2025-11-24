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
    // Run automatic threshold checks AND pre-warm cache every 6 hours
    ctx.waitUntil(Promise.all([
      runScheduledThresholdCheck(env),
      preWarmCache(env)
    ]));
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

    // Phase 3: Full data including historical (SLOW - 10s)
    // Only fetch metrics that are ENABLED in config
    const configData = await env.CONFIG_KV.get('config:default');
    const config = configData ? JSON.parse(configData) : {};
    
    let coreMetrics = null;
    let botManagementData = null;
    let successfulMetrics = []; // ✅ Declare outside if block so add-ons can use it!
    
    // Fetch App Services Core if enabled
    if (config?.applicationServices?.core?.enabled !== false) {
      // Default to enabled for backward compatibility
      console.log('📊 Fetching App Services Core metrics...');
      const accountMetricsPromises = accountIds.map(accountId => 
        fetchAccountMetrics(apiKey, accountId, env)
      );
      
      const accountMetricsResults = await Promise.allSettled(accountMetricsPromises);
      successfulMetrics = accountMetricsResults
        .filter(result => result.status === 'fulfilled')
        .map(result => result.value);
      
      if (successfulMetrics.length > 0) {
        coreMetrics = aggregateAccountMetrics(successfulMetrics);
      } else {
        console.warn('⚠️ Failed to fetch core metrics from any account');
      }
    } else {
      console.log('⏭️ App Services Core disabled - skipping fetch');
    }
    
    // Fetch Bot Management if enabled
    if (config?.applicationServices?.botManagement?.enabled && accountIds.length > 0) {
      console.log('🤖 Fetching Bot Management metrics...');
      const botManagementConfig = config.applicationServices.botManagement;
      
      const botMgmtPromises = accountIds.map(accountId =>
        fetchBotManagementForAccount(apiKey, accountId, botManagementConfig, env)
          .then(data => ({ accountId, data })) // ✅ Include accountId with data
      );
      
      const botMgmtResults = await Promise.allSettled(botMgmtPromises);
      const botMgmtData = botMgmtResults
        .filter(result => result.status === 'fulfilled' && result.value?.data) // Check data exists
        .map(result => result.value); // Now has { accountId, data }
      
      // Aggregate bot management across accounts
      if (botMgmtData.length > 0) {
        // Merge timeSeries from all accounts
        const timeSeriesMap = new Map();
        botMgmtData.forEach(accountEntry => {
          if (accountEntry.data.timeSeries) {
            accountEntry.data.timeSeries.forEach(entry => {
              const existing = timeSeriesMap.get(entry.month);
              if (existing) {
                existing.likelyHuman += entry.likelyHuman || 0;
              } else {
                timeSeriesMap.set(entry.month, {
                  month: entry.month,
                  timestamp: entry.timestamp,
                  likelyHuman: entry.likelyHuman || 0,
                });
              }
            });
          }
        });

        const mergedTimeSeries = Array.from(timeSeriesMap.values())
          .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

        // Aggregate confidence from all accounts
        const botManagementConfidence = botMgmtData.find(entry => entry.data.current?.confidence)?.data.current.confidence || null;

        botManagementData = {
          enabled: true,
          threshold: botManagementConfig.threshold,
          current: {
            likelyHuman: botMgmtData.reduce((sum, entry) => sum + entry.data.current.likelyHuman, 0),
            zones: botMgmtData.flatMap(entry => entry.data.current.zones),
            confidence: botManagementConfidence,
          },
          previous: {
            likelyHuman: botMgmtData.reduce((sum, entry) => sum + entry.data.previous.likelyHuman, 0),
            zones: botMgmtData.flatMap(entry => entry.data.previous.zones),
          },
          timeSeries: mergedTimeSeries,
          // Store per-account data for filtering
          perAccountData: botMgmtData.map(entry => ({
            accountId: entry.accountId, // ✅ Use correct accountId
            current: entry.data.current,
            previous: entry.data.previous,
            timeSeries: entry.data.timeSeries,
          })),
        };
      }
    } else {
      console.log('⏭️ Bot Management disabled - skipping fetch');
    }
    
    // Fetch API Shield if enabled (reuses existing zone data!)
    let apiShieldData = null;
    if (config?.applicationServices?.apiShield?.enabled && successfulMetrics && successfulMetrics.length > 0) {
      console.log('🛡️ Calculating API Shield metrics from existing zone data...');
      const apiShieldConfig = config.applicationServices.apiShield;
      
      const apiShieldPromises = successfulMetrics.map(accountData =>
        calculateZoneBasedAddonForAccount(accountData, apiShieldConfig, env, 'api-shield')
          .then(data => ({ accountId: accountData.accountId, data }))
      );
      
      const apiShieldResults = await Promise.allSettled(apiShieldPromises);
      const apiShieldAccounts = apiShieldResults
        .filter(result => result.status === 'fulfilled' && result.value?.data)
        .map(result => result.value);
      
      if (apiShieldAccounts.length > 0) {
        // Merge timeSeries
        const timeSeriesMap = new Map();
        apiShieldAccounts.forEach(accountEntry => {
          if (accountEntry.data.timeSeries) {
            accountEntry.data.timeSeries.forEach(entry => {
              const existing = timeSeriesMap.get(entry.month);
              if (existing) {
                existing.requests += entry.requests || 0;
              } else {
                timeSeriesMap.set(entry.month, {
                  month: entry.month,
                  timestamp: entry.timestamp,
                  requests: entry.requests || 0,
                });
              }
            });
          }
        });

        const mergedTimeSeries = Array.from(timeSeriesMap.values())
          .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

        // Aggregate confidence from all accounts (use first non-null confidence as they should all be the same)
        const apiShieldConfidence = apiShieldAccounts.find(entry => entry.data.current?.confidence)?.data.current.confidence || null;

        apiShieldData = {
          enabled: true,
          threshold: apiShieldConfig.threshold,
          current: {
            requests: apiShieldAccounts.reduce((sum, entry) => sum + entry.data.current.requests, 0),
            zones: apiShieldAccounts.flatMap(entry => entry.data.current.zones),
            confidence: apiShieldConfidence,
          },
          previous: {
            requests: apiShieldAccounts.reduce((sum, entry) => sum + entry.data.previous.requests, 0),
            zones: apiShieldAccounts.flatMap(entry => entry.data.previous.zones),
          },
          timeSeries: mergedTimeSeries,
          perAccountData: apiShieldAccounts.map(entry => ({
            accountId: entry.accountId,
            current: entry.data.current,
            previous: entry.data.previous,
            timeSeries: entry.data.timeSeries,
          })),
        };
        console.log(`API Shield data calculated (${apiShieldData.current.zones.length} zones)`);
      }
    } else {
      console.log('⏭️ API Shield disabled - skipping calculation');
    }
    
    // Fetch Page Shield if enabled (reuses existing zone data!)
    let pageShieldData = null;
    if (config?.applicationServices?.pageShield?.enabled && successfulMetrics && successfulMetrics.length > 0) {
      console.log('📄 Calculating Page Shield metrics from existing zone data...');
      const pageShieldConfig = config.applicationServices.pageShield;
      
      const pageShieldPromises = successfulMetrics.map(accountData =>
        calculateZoneBasedAddonForAccount(accountData, pageShieldConfig, env, 'page-shield')
          .then(data => ({ accountId: accountData.accountId, data }))
      );
      
      const pageShieldResults = await Promise.allSettled(pageShieldPromises);
      const pageShieldAccounts = pageShieldResults
        .filter(result => result.status === 'fulfilled' && result.value?.data)
        .map(result => result.value);
      
      if (pageShieldAccounts.length > 0) {
        // Merge timeSeries
        const timeSeriesMap = new Map();
        pageShieldAccounts.forEach(accountEntry => {
          if (accountEntry.data.timeSeries) {
            accountEntry.data.timeSeries.forEach(entry => {
              const existing = timeSeriesMap.get(entry.month);
              if (existing) {
                existing.requests += entry.requests || 0;
              } else {
                timeSeriesMap.set(entry.month, {
                  month: entry.month,
                  timestamp: entry.timestamp,
                  requests: entry.requests || 0,
                });
              }
            });
          }
        });

        const mergedTimeSeries = Array.from(timeSeriesMap.values())
          .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

        // Aggregate confidence from all accounts (use first non-null confidence as they should all be the same)
        const pageShieldConfidence = pageShieldAccounts.find(entry => entry.data.current?.confidence)?.data.current.confidence || null;

        pageShieldData = {
          enabled: true,
          threshold: pageShieldConfig.threshold,
          current: {
            requests: pageShieldAccounts.reduce((sum, entry) => sum + entry.data.current.requests, 0),
            zones: pageShieldAccounts.flatMap(entry => entry.data.current.zones),
            confidence: pageShieldConfidence,
          },
          previous: {
            requests: pageShieldAccounts.reduce((sum, entry) => sum + entry.data.previous.requests, 0),
            zones: pageShieldAccounts.flatMap(entry => entry.data.previous.zones),
          },
          timeSeries: mergedTimeSeries,
          perAccountData: pageShieldAccounts.map(entry => ({
            accountId: entry.accountId,
            current: entry.data.current,
            previous: entry.data.previous,
            timeSeries: entry.data.timeSeries,
          })),
        };
        console.log(`Page Shield data calculated (${pageShieldData.current.zones.length} zones)`);
      }
    } else {
      console.log('⏭️ Page Shield disabled - skipping calculation');
    }
    
    // Fetch Advanced Rate Limiting if enabled (reuses existing zone data!)
    let advancedRateLimitingData = null;
    if (config?.applicationServices?.advancedRateLimiting?.enabled && successfulMetrics && successfulMetrics.length > 0) {
      console.log('⚡ Calculating Advanced Rate Limiting metrics from existing zone data...');
      const rateLimitingConfig = config.applicationServices.advancedRateLimiting;
      
      const rateLimitingPromises = successfulMetrics.map(accountData =>
        calculateZoneBasedAddonForAccount(accountData, rateLimitingConfig, env, 'advanced-rate-limiting')
          .then(data => ({ accountId: accountData.accountId, data }))
      );
      
      const rateLimitingResults = await Promise.allSettled(rateLimitingPromises);
      const rateLimitingAccounts = rateLimitingResults
        .filter(result => result.status === 'fulfilled' && result.value?.data)
        .map(result => result.value);
      
      if (rateLimitingAccounts.length > 0) {
        // Merge timeSeries
        const timeSeriesMap = new Map();
        rateLimitingAccounts.forEach(accountEntry => {
          if (accountEntry.data.timeSeries) {
            accountEntry.data.timeSeries.forEach(entry => {
              const existing = timeSeriesMap.get(entry.month);
              if (existing) {
                existing.requests += entry.requests || 0;
              } else {
                timeSeriesMap.set(entry.month, {
                  month: entry.month,
                  timestamp: entry.timestamp,
                  requests: entry.requests || 0,
                });
              }
            });
          }
        });

        const mergedTimeSeries = Array.from(timeSeriesMap.values())
          .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

        // Aggregate confidence from all accounts (use first non-null confidence as they should all be the same)
        const rateLimitingConfidence = rateLimitingAccounts.find(entry => entry.data.current?.confidence)?.data.current.confidence || null;

        advancedRateLimitingData = {
          enabled: true,
          threshold: rateLimitingConfig.threshold,
          current: {
            requests: rateLimitingAccounts.reduce((sum, entry) => sum + entry.data.current.requests, 0),
            zones: rateLimitingAccounts.flatMap(entry => entry.data.current.zones),
            confidence: rateLimitingConfidence,
          },
          previous: {
            requests: rateLimitingAccounts.reduce((sum, entry) => sum + entry.data.previous.requests, 0),
            zones: rateLimitingAccounts.flatMap(entry => entry.data.previous.zones),
          },
          timeSeries: mergedTimeSeries,
          perAccountData: rateLimitingAccounts.map(entry => ({
            accountId: entry.accountId,
            current: entry.data.current,
            previous: entry.data.previous,
            timeSeries: entry.data.timeSeries,
          })),
        };
        console.log(`Advanced Rate Limiting data calculated (${advancedRateLimitingData.current.zones.length} zones)`);
      }
    } else {
      console.log('⏭️ Advanced Rate Limiting disabled - skipping calculation');
    }
    
    // Build response with only enabled metrics
    const response = {
      phase: 3,
      ...(coreMetrics || {}),
      ...(botManagementData && { botManagement: botManagementData }),
      ...(apiShieldData && { apiShield: apiShieldData }),
      ...(pageShieldData && { pageShield: pageShieldData }),
      ...(advancedRateLimitingData && { advancedRateLimiting: advancedRateLimitingData }),
    };
    
    return new Response(
      JSON.stringify(response),
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

  // Make request to Cloudflare GraphQL API
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

  // Process and aggregate current month data from all Enterprise zones
  const zones = data.data?.viewer?.zones || [];
  
  if (zones.length === 0) {
    throw new Error(`No zone data found for account ${accountId}`);
  }

  // Aggregate current month stats across all Enterprise zones
  // Now tracking only clean/billable traffic directly
  let currentMonthTotal = { 
    requests: 0,  // Clean/billable requests only
    bytes: 0,     // Clean/billable bytes only
    dnsQueries: 0,
    confidence: {
      requests: null,
      bytes: null,
      dnsQueries: null
    }
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
  
  zones.forEach(zone => {
    // Get aggregated totals (single result, no loop needed)
    const totals = zone.totals?.[0];
    const zoneRequests = totals?.count || 0;
    const zoneBytes = totals?.sum?.edgeResponseBytes || 0;
    
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
    
    // Add to account totals (already filtered for clean/billable traffic)
    currentMonthTotal.requests += zoneRequests;
    currentMonthTotal.bytes += zoneBytes;
    
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
    dnsQueries: 0
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
    
    // Track per-zone metrics for previous month
    const prevZoneMetrics = [];
    const SECONDARY_ZONE_THRESHOLD = 50 * (1024 ** 3); // 50GB in bytes
    
    prevZones.forEach(zone => {
      // Get aggregated totals (single result, no loop needed)
      const totals = zone.totals?.[0];
      const zoneRequests = totals?.count || 0;
      const zoneBytes = totals?.sum?.edgeResponseBytes || 0;
      
      // Add to previous month totals (already filtered for clean/billable traffic)
      previousMonthStats.requests += zoneRequests;
      previousMonthStats.bytes += zoneBytes;
      
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
      }
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
    perAccountData: accountMetrics,  // Store for future filtering
  };

  // Aggregate current month
  const confidenceAggregator = {
    requests: { estimates: [], lowers: [], uppers: [], sampleSizes: [] },
    bytes: { estimates: [], lowers: [], uppers: [], sampleSizes: [] },
    dnsQueries: { estimates: [], lowers: [], uppers: [], sampleSizes: [] }
  };
  
  accountMetrics.forEach(accountData => {
    aggregated.current.requests += accountData.current.requests || 0;
    aggregated.current.bytes += accountData.current.bytes || 0;
    aggregated.current.dnsQueries += accountData.current.dnsQueries || 0;
    
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
  });

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

  // Fetch zones from all accounts
  const allEnterpriseZones = [];
  let totalZones = 0;
  
  for (const accountId of accountIds) {
    try {
      const zones = await fetchEnterpriseZones(apiKey, accountId);
      if (zones && zones.length > 0) {
        allEnterpriseZones.push(...zones);
        totalZones += zones.length;
      }
    } catch (error) {
      console.error(`Error fetching zones for account ${accountId}:`, error);
      // Continue with other accounts
    }
  }

  return new Response(
    JSON.stringify({
      total: allEnterpriseZones.length,
      enterprise: allEnterpriseZones.length,
      zones: allEnterpriseZones.map(z => ({ id: z.id, name: z.name })),
    }),
    {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    }
  );
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
 */
async function checkThresholds(request, env, corsHeaders) {
  const body = await request.json();
  const { metrics, thresholds, slackWebhook, accountIds, accountId, forceTest } = body;
  
  // Support both old and new format
  const accounts = accountIds || (accountId ? [accountId] : []);
  const accountsDisplay = accounts.length > 1 ? `${accounts.length} accounts` : accounts[0] || 'Unknown';

  // If forceTest is true, always send a test notification
  if (forceTest && slackWebhook) {
    const testMessage = {
      text: '🧪 *Test Notification - Enterprise Usage Dashboard*',
      blocks: [
        {
          type: 'header',
          text: {
            type: 'plain_text',
            text: '🧪 Test Notification',
            emoji: true
          }
        },
        {
          type: 'section',
          text: {
            type: 'mrkdwn',
            text: '*This is a test notification from your Enterprise Usage Dashboard.*\n\nYour Slack webhook is configured correctly and working! ✅'
          }
        },
        {
          type: 'section',
          fields: [
            {
              type: 'mrkdwn',
              text: `*Current Zones:*\n${metrics.zones || 0}`
            },
            {
              type: 'mrkdwn',
              text: `*Current Requests:*\n${(metrics.requests || 0).toLocaleString()}`
            },
            {
              type: 'mrkdwn',
              text: `*Current Bandwidth:*\n${((metrics.bandwidth || 0) / (1024 ** 4)).toFixed(2)} TB`
            },
            {
              type: 'mrkdwn',
              text: `*Bot Management (Likely Human):*\n${(metrics.botManagement || 0).toLocaleString()}`
            }
          ]
        },
        {
          type: 'context',
          elements: [
            {
              type: 'mrkdwn',
              text: `🕐 ${new Date().toLocaleString()} | Account(s): ${accountsDisplay}`
            }
          ]
        }
      ]
    };

    try {
      await fetch(slackWebhook, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(testMessage),
      });

      return new Response(
        JSON.stringify({
          success: true,
          message: 'Test notification sent successfully to Slack!'
        }),
        {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({
          success: false,
          message: 'Failed to send test notification: ' + error.message
        }),
        {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          status: 500,
        }
      );
    }
  }

  const alerts = [];

  // Check each metric against threshold
  if (metrics.zones && thresholds.zones) {
    const percentage = (metrics.zones / thresholds.zones) * 100;
    if (percentage >= 90) {
      alerts.push({
        metric: 'Enterprise Zones',
        metricKey: 'zones',
        current: metrics.zones,
        threshold: thresholds.zones,
        percentage: percentage.toFixed(1),
      });
    }
  }

  if (metrics.requests && thresholds.requests) {
    const percentage = (metrics.requests / thresholds.requests) * 100;
    if (percentage >= 90) {
      alerts.push({
        metric: 'HTTP Requests',
        metricKey: 'requests',
        current: metrics.requests.toLocaleString(),
        threshold: thresholds.requests.toLocaleString(),
        percentage: percentage.toFixed(1),
      });
    }
  }

  if (metrics.bandwidth && thresholds.bandwidth) {
    const percentage = (metrics.bandwidth / thresholds.bandwidth) * 100;
    if (percentage >= 90) {
      const formatBytes = (bytes) => {
        const tb = bytes / (1024 ** 4);
        return `${tb.toFixed(2)} TB`;
      };
      alerts.push({
        metric: 'Data Transfer',
        metricKey: 'bandwidth',
        current: formatBytes(metrics.bandwidth),
        threshold: formatBytes(thresholds.bandwidth),
        percentage: percentage.toFixed(1),
      });
    }
  }

  if (metrics.dnsQueries && thresholds.dnsQueries) {
    const percentage = (metrics.dnsQueries / thresholds.dnsQueries) * 100;
    if (percentage >= 90) {
      const formatQueries = (queries) => {
        if (queries >= 1e6) {
          return `${(queries / 1e6).toFixed(2)}M`;
        }
        return queries.toLocaleString();
      };
      alerts.push({
        metric: 'DNS Queries',
        metricKey: 'dnsQueries',
        current: formatQueries(metrics.dnsQueries),
        threshold: formatQueries(thresholds.dnsQueries),
        percentage: percentage.toFixed(1),
      });
    }
  }

  // Check Bot Management threshold (only if enabled)
  if (metrics.botManagement && thresholds.botManagement) {
    const percentage = (metrics.botManagement / thresholds.botManagement) * 100;
    if (percentage >= 90) {
      const formatRequests = (requests) => {
        if (requests >= 1e6) {
          return `${(requests / 1e6).toFixed(2)}M`;
        }
        return requests.toLocaleString();
      };
      alerts.push({
        metric: 'Bot Management (Likely Human)',
        metricKey: 'botManagement',
        current: formatRequests(metrics.botManagement),
        threshold: formatRequests(thresholds.botManagement),
        percentage: percentage.toFixed(1),
      });
    }
  }

  // Check API Shield threshold (only if enabled)
  if (metrics.apiShield && thresholds.apiShield) {
    const percentage = (metrics.apiShield / thresholds.apiShield) * 100;
    if (percentage >= 90) {
      const formatRequests = (requests) => {
        if (requests >= 1e6) {
          return `${(requests / 1e6).toFixed(2)}M`;
        }
        return requests.toLocaleString();
      };
      alerts.push({
        metric: 'API Shield (HTTP Requests)',
        metricKey: 'apiShield',
        current: formatRequests(metrics.apiShield),
        threshold: formatRequests(thresholds.apiShield),
        percentage: percentage.toFixed(1),
      });
    }
  }

  // Check Page Shield threshold (only if enabled)
  if (metrics.pageShield && thresholds.pageShield) {
    const percentage = (metrics.pageShield / thresholds.pageShield) * 100;
    if (percentage >= 90) {
      const formatRequests = (requests) => {
        if (requests >= 1e6) {
          return `${(requests / 1e6).toFixed(2)}M`;
        }
        return requests.toLocaleString();
      };
      alerts.push({
        metric: 'Page Shield (HTTP Requests)',
        metricKey: 'pageShield',
        current: formatRequests(metrics.pageShield),
        threshold: formatRequests(thresholds.pageShield),
        percentage: percentage.toFixed(1),
      });
    }
  }

  // Check Advanced Rate Limiting threshold (only if enabled)
  if (metrics.advancedRateLimiting && thresholds.advancedRateLimiting) {
    const percentage = (metrics.advancedRateLimiting / thresholds.advancedRateLimiting) * 100;
    if (percentage >= 90) {
      const formatRequests = (requests) => {
        if (requests >= 1e6) {
          return `${(requests / 1e6).toFixed(2)}M`;
        }
        return requests.toLocaleString();
      };
      alerts.push({
        metric: 'Advanced Rate Limiting (HTTP Requests)',
        metricKey: 'advancedRateLimiting',
        current: formatRequests(metrics.advancedRateLimiting),
        threshold: formatRequests(thresholds.advancedRateLimiting),
        percentage: percentage.toFixed(1),
      });
    }
  }

  // Send Slack notification if webhook is provided
  if (alerts.length > 0 && slackWebhook) {
    try {
      // Get current month for alert tracking
      const now = new Date();
      const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
      
      // Filter out alerts that have already been sent this month
      // Use combined account key for multi-account aggregation
      const accountsKey = accounts.sort().join('-');
      const newAlerts = [];
      for (const alert of alerts) {
        const alertKey = `alert-sent:${accountsKey}:${alert.metricKey}:${currentMonth}`;
        const alreadySent = await env.CONFIG_KV.get(alertKey);
        
        if (!alreadySent) {
          newAlerts.push(alert);
          // Mark this alert as sent (expires after 45 days)
          await env.CONFIG_KV.put(alertKey, 'true', { expirationTtl: 3888000 });
        }
      }
      
      // Only send Slack message if there are new alerts
      if (newAlerts.length > 0) {
        const dashboardUrl = new URL(request.url).origin;
        await sendSlackAlert(newAlerts, slackWebhook, dashboardUrl);
        return new Response(
          JSON.stringify({
            alerts: newAlerts,
            alertsTriggered: true,
            slackSent: true,
            skipped: alerts.length - newAlerts.length,
          }),
          {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          }
        );
      } else {
        // All alerts already sent this month
        return new Response(
          JSON.stringify({
            alerts: [],
            alertsTriggered: true,
            slackSent: false,
            message: 'All alerts already sent this month',
            skipped: alerts.length,
          }),
          {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          }
        );
      }
    } catch (error) {
      console.error('Slack error:', error);
      return new Response(
        JSON.stringify({
          alerts,
          alertsTriggered: true,
          slackSent: false,
          error: error.message,
        }),
        {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        }
      );
    }
  }

  return new Response(
    JSON.stringify({
      alerts,
      alertsTriggered: alerts.length > 0,
    }),
    {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    }
  );
}

/**
 * Send Slack alert
 */
async function sendSlackAlert(alerts, webhookUrl, dashboardUrl) {
  // Build Slack message with formatted blocks
  const alertFields = alerts.map(alert => ({
    type: "section",
    fields: [
      {
        type: "mrkdwn",
        text: `*${alert.metric}*\n${alert.percentage}% used`
      },
      {
        type: "mrkdwn",
        text: `*Current:* ${alert.current}\n*Threshold:* ${alert.threshold}`
      }
    ]
  }));

  // Build Slack message payload
  const slackPayload = {
    blocks: [
      {
        type: "header",
        text: {
          type: "plain_text",
          text: "⚠️ Cloudflare Usage Alert",
          emoji: true
        }
      },
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: "*Threshold Warning: 90% Reached*\nYour Cloudflare Enterprise usage has reached *90% or more* of your contracted thresholds:"
        }
      },
      {
        type: "divider"
      },
      ...alertFields,
      {
        type: "divider"
      },
      {
        type: "context",
        elements: [
          {
            type: "mrkdwn",
            text: `🕐 Alert triggered: <!date^${Math.floor(Date.now() / 1000)}^{date_short_pretty} at {time}|${new Date().toUTCString()}>`
          }
        ]
      },
      {
        type: "actions",
        elements: [
          {
            type: "button",
            text: {
              type: "plain_text",
              text: "View Dashboard",
              emoji: true
            },
            url: dashboardUrl,
            style: "primary"
          }
        ]
      }
    ]
  };

  const response = await fetch(webhookUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(slackPayload),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to send Slack message: ${response.status} - ${errorText}`);
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

    let coreMetrics = null;
    let zonesCount = 0;
    let zonesData = null;
    let botManagementData = null;
    let successfulMetrics = []; // ✅ Declare outside if block so add-ons can use it!

    // Fetch App Services Core if enabled
    if (config?.applicationServices?.core?.enabled !== false) {
      // Default to enabled for backward compatibility
      console.log('Pre-warm: Fetching App Services Core metrics...');
      
      const accountMetricsPromises = accountIds.map(accountId => 
        fetchAccountMetrics(apiKey, accountId, env)
      );
      
      const accountMetricsResults = await Promise.allSettled(accountMetricsPromises);
      successfulMetrics = accountMetricsResults
        .filter(result => result.status === 'fulfilled')
        .map(result => result.value);
      
      if (successfulMetrics.length > 0) {
        coreMetrics = aggregateAccountMetrics(successfulMetrics);
        console.log(`Pre-warm: Core metrics fetched successfully`);
      } else {
        console.log('Pre-warm: Failed to fetch core metrics from any account');
      }

      // Fetch zones list (needed for instant display)
      const zonesPromises = accountIds.map(accountId =>
        fetchEnterpriseZones(apiKey, accountId)
      );
      
      const zonesResults = await Promise.allSettled(zonesPromises);
      const allZones = zonesResults
        .filter(result => result.status === 'fulfilled')
        .flatMap(result => result.value);
      
      zonesCount = allZones.length;
      zonesData = {
        zones: allZones,
        enterprise: zonesCount
      };
      console.log(`Pre-warm: ${zonesCount} zones found and cached`);
    } else {
      console.log('Pre-warm: App Services Core disabled - skipping fetch');
    }
    
    // Fetch Bot Management if enabled
    if (config?.applicationServices?.botManagement?.enabled && accountIds.length > 0) {
      console.log('Pre-warm: Fetching Bot Management metrics...');
      const botManagementConfig = config.applicationServices.botManagement;
      
      const botMgmtPromises = accountIds.map(accountId =>
        fetchBotManagementForAccount(apiKey, accountId, botManagementConfig, env)
          .then(data => ({ accountId, data })) // ✅ Include accountId with data
      );
      
      const botMgmtResults = await Promise.allSettled(botMgmtPromises);
      const botMgmtData = botMgmtResults
        .filter(result => result.status === 'fulfilled' && result.value?.data) // Check data exists
        .map(result => result.value); // Now has { accountId, data }
      
      // Aggregate bot management across accounts
      if (botMgmtData.length > 0) {
        // Merge timeSeries from all accounts
        const timeSeriesMap = new Map();
        botMgmtData.forEach(accountEntry => {
          if (accountEntry.data.timeSeries) {
            accountEntry.data.timeSeries.forEach(entry => {
              const existing = timeSeriesMap.get(entry.month);
              if (existing) {
                existing.likelyHuman += entry.likelyHuman || 0;
              } else {
                timeSeriesMap.set(entry.month, {
                  month: entry.month,
                  timestamp: entry.timestamp,
                  likelyHuman: entry.likelyHuman || 0,
                });
              }
            });
          }
        });

        const mergedTimeSeries = Array.from(timeSeriesMap.values())
          .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

        // Aggregate confidence from all accounts
        const botManagementConfidence = botMgmtData.find(entry => entry.data.current?.confidence)?.data.current.confidence || null;

        botManagementData = {
          enabled: true,
          threshold: botManagementConfig.threshold,
          current: {
            likelyHuman: botMgmtData.reduce((sum, entry) => sum + entry.data.current.likelyHuman, 0),
            zones: botMgmtData.flatMap(entry => entry.data.current.zones),
            confidence: botManagementConfidence,
          },
          previous: {
            likelyHuman: botMgmtData.reduce((sum, entry) => sum + entry.data.previous.likelyHuman, 0),
            zones: botMgmtData.flatMap(entry => entry.data.previous.zones),
          },
          timeSeries: mergedTimeSeries,
          // Store per-account data for filtering
          perAccountData: botMgmtData.map(entry => ({
            accountId: entry.accountId, // ✅ Use correct accountId
            current: entry.data.current,
            previous: entry.data.previous,
            timeSeries: entry.data.timeSeries,
          })),
        };
        console.log(`Pre-warm: Bot Management data fetched (${botManagementData.current.zones.length} zones, ${mergedTimeSeries.length} months)`);
      }
    } else {
      console.log('Pre-warm: Bot Management disabled - skipping fetch');
    }
    
    // Fetch API Shield if enabled (reuses existing zone data!)
    let apiShieldData = null;
    if (config?.applicationServices?.apiShield?.enabled && successfulMetrics && successfulMetrics.length > 0) {
      console.log('Pre-warm: Calculating API Shield metrics from existing zone data...');
      const apiShieldConfig = config.applicationServices.apiShield;
      
      const apiShieldPromises = successfulMetrics.map(accountData =>
        calculateZoneBasedAddonForAccount(accountData, apiShieldConfig, env, 'api-shield')
          .then(data => ({ accountId: accountData.accountId, data }))
      );
      
      const apiShieldResults = await Promise.allSettled(apiShieldPromises);
      const apiShieldAccounts = apiShieldResults
        .filter(result => result.status === 'fulfilled' && result.value?.data)
        .map(result => result.value);
      
      if (apiShieldAccounts.length > 0) {
        const timeSeriesMap = new Map();
        apiShieldAccounts.forEach(accountEntry => {
          if (accountEntry.data.timeSeries) {
            accountEntry.data.timeSeries.forEach(entry => {
              const existing = timeSeriesMap.get(entry.month);
              if (existing) {
                existing.requests += entry.requests || 0;
              } else {
                timeSeriesMap.set(entry.month, {
                  month: entry.month,
                  timestamp: entry.timestamp,
                  requests: entry.requests || 0,
                });
              }
            });
          }
        });

        const mergedTimeSeries = Array.from(timeSeriesMap.values())
          .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

        // Aggregate confidence from all accounts
        const apiShieldConfidence = apiShieldAccounts.find(entry => entry.data.current?.confidence)?.data.current.confidence || null;

        apiShieldData = {
          enabled: true,
          threshold: apiShieldConfig.threshold,
          current: {
            requests: apiShieldAccounts.reduce((sum, entry) => sum + entry.data.current.requests, 0),
            zones: apiShieldAccounts.flatMap(entry => entry.data.current.zones),
            confidence: apiShieldConfidence,
          },
          previous: {
            requests: apiShieldAccounts.reduce((sum, entry) => sum + entry.data.previous.requests, 0),
            zones: apiShieldAccounts.flatMap(entry => entry.data.previous.zones),
          },
          timeSeries: mergedTimeSeries,
          perAccountData: apiShieldAccounts.map(entry => ({
            accountId: entry.accountId,
            current: entry.data.current,
            previous: entry.data.previous,
            timeSeries: entry.data.timeSeries,
          })),
        };
        console.log(`Pre-warm: API Shield data calculated (${apiShieldData.current.zones.length} zones, ${mergedTimeSeries.length} months)`);
      }
    } else {
      console.log('Pre-warm: API Shield disabled - skipping calculation');
    }
    
    // Fetch Page Shield if enabled (reuses existing zone data!)
    let pageShieldData = null;
    if (config?.applicationServices?.pageShield?.enabled && successfulMetrics && successfulMetrics.length > 0) {
      console.log('Pre-warm: Calculating Page Shield metrics from existing zone data...');
      const pageShieldConfig = config.applicationServices.pageShield;
      
      const pageShieldPromises = successfulMetrics.map(accountData =>
        calculateZoneBasedAddonForAccount(accountData, pageShieldConfig, env, 'page-shield')
          .then(data => ({ accountId: accountData.accountId, data }))
      );
      
      const pageShieldResults = await Promise.allSettled(pageShieldPromises);
      const pageShieldAccounts = pageShieldResults
        .filter(result => result.status === 'fulfilled' && result.value?.data)
        .map(result => result.value);
      
      if (pageShieldAccounts.length > 0) {
        const timeSeriesMap = new Map();
        pageShieldAccounts.forEach(accountEntry => {
          if (accountEntry.data.timeSeries) {
            accountEntry.data.timeSeries.forEach(entry => {
              const existing = timeSeriesMap.get(entry.month);
              if (existing) {
                existing.requests += entry.requests || 0;
              } else {
                timeSeriesMap.set(entry.month, {
                  month: entry.month,
                  timestamp: entry.timestamp,
                  requests: entry.requests || 0,
                });
              }
            });
          }
        });

        const mergedTimeSeries = Array.from(timeSeriesMap.values())
          .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

        // Aggregate confidence from all accounts
        const pageShieldConfidence = pageShieldAccounts.find(entry => entry.data.current?.confidence)?.data.current.confidence || null;

        pageShieldData = {
          enabled: true,
          threshold: pageShieldConfig.threshold,
          current: {
            requests: pageShieldAccounts.reduce((sum, entry) => sum + entry.data.current.requests, 0),
            zones: pageShieldAccounts.flatMap(entry => entry.data.current.zones),
            confidence: pageShieldConfidence,
          },
          previous: {
            requests: pageShieldAccounts.reduce((sum, entry) => sum + entry.data.previous.requests, 0),
            zones: pageShieldAccounts.flatMap(entry => entry.data.previous.zones),
          },
          timeSeries: mergedTimeSeries,
          perAccountData: pageShieldAccounts.map(entry => ({
            accountId: entry.accountId,
            current: entry.data.current,
            previous: entry.data.previous,
            timeSeries: entry.data.timeSeries,
          })),
        };
        console.log(`Pre-warm: Page Shield data calculated (${pageShieldData.current.zones.length} zones, ${mergedTimeSeries.length} months)`);
      }
    } else {
      console.log('Pre-warm: Page Shield disabled - skipping calculation');
    }
    
    // Fetch Advanced Rate Limiting if enabled (reuses existing zone data!)
    let advancedRateLimitingData = null;
    if (config?.applicationServices?.advancedRateLimiting?.enabled && successfulMetrics && successfulMetrics.length > 0) {
      console.log('Pre-warm: Calculating Advanced Rate Limiting metrics from existing zone data...');
      const rateLimitingConfig = config.applicationServices.advancedRateLimiting;
      
      const rateLimitingPromises = successfulMetrics.map(accountData =>
        calculateZoneBasedAddonForAccount(accountData, rateLimitingConfig, env, 'advanced-rate-limiting')
          .then(data => ({ accountId: accountData.accountId, data }))
      );
      
      const rateLimitingResults = await Promise.allSettled(rateLimitingPromises);
      const rateLimitingAccounts = rateLimitingResults
        .filter(result => result.status === 'fulfilled' && result.value?.data)
        .map(result => result.value);
      
      if (rateLimitingAccounts.length > 0) {
        const timeSeriesMap = new Map();
        rateLimitingAccounts.forEach(accountEntry => {
          if (accountEntry.data.timeSeries) {
            accountEntry.data.timeSeries.forEach(entry => {
              const existing = timeSeriesMap.get(entry.month);
              if (existing) {
                existing.requests += entry.requests || 0;
              } else {
                timeSeriesMap.set(entry.month, {
                  month: entry.month,
                  timestamp: entry.timestamp,
                  requests: entry.requests || 0,
                });
              }
            });
          }
        });

        const mergedTimeSeries = Array.from(timeSeriesMap.values())
          .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

        // Aggregate confidence from all accounts
        const rateLimitingConfidence = rateLimitingAccounts.find(entry => entry.data.current?.confidence)?.data.current.confidence || null;

        advancedRateLimitingData = {
          enabled: true,
          threshold: rateLimitingConfig.threshold,
          current: {
            requests: rateLimitingAccounts.reduce((sum, entry) => sum + entry.data.current.requests, 0),
            zones: rateLimitingAccounts.flatMap(entry => entry.data.current.zones),
            confidence: rateLimitingConfidence,
          },
          previous: {
            requests: rateLimitingAccounts.reduce((sum, entry) => sum + entry.data.previous.requests, 0),
            zones: rateLimitingAccounts.flatMap(entry => entry.data.previous.zones),
          },
          timeSeries: mergedTimeSeries,
          perAccountData: rateLimitingAccounts.map(entry => ({
            accountId: entry.accountId,
            current: entry.data.current,
            previous: entry.data.previous,
            timeSeries: entry.data.timeSeries,
          })),
        };
        console.log(`Pre-warm: Advanced Rate Limiting data calculated (${advancedRateLimitingData.current.zones.length} zones, ${mergedTimeSeries.length} months)`);
      }
    } else {
      console.log('Pre-warm: Advanced Rate Limiting disabled - skipping calculation');
    }
    
    // Store in cache with timestamp (only enabled metrics)
    const cacheKey = `pre-warmed:${accountIds.join(',')}`;
    const cacheData = {
      timestamp: Date.now(),
      data: {
        ...(coreMetrics || {}),
        zonesCount: zonesCount,
        zones: zonesData, // ✅ Include full zones list for instant display
        ...(botManagementData && { botManagement: botManagementData }),
        ...(apiShieldData && { apiShield: apiShieldData }),
        ...(pageShieldData && { pageShield: pageShieldData }),
        ...(advancedRateLimitingData && { advancedRateLimiting: advancedRateLimitingData }),
      },
    };

    // Cache for 6 hours (matching cron schedule)
    await env.CONFIG_KV.put(cacheKey, JSON.stringify(cacheData), {
      expirationTtl: 6 * 60 * 60, // 6 hours
    });

    const duration = Date.now() - startTime;
    console.log(`✅ Pre-warm complete! Cached in ${(duration / 1000).toFixed(1)}s. Next dashboard load will be INSTANT! ⚡`);
    
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

    // Fetch current metrics
    const accountMetricsPromises = accountIds.map(accountId => 
      fetchAccountMetrics(apiKey, accountId, env)
    );
    
    const accountMetricsResults = await Promise.allSettled(accountMetricsPromises);
    
    const successfulMetrics = accountMetricsResults
      .filter(result => result.status === 'fulfilled')
      .map(result => result.value);
    
    if (successfulMetrics.length === 0) {
      console.log('Scheduled check: Failed to fetch metrics from any account');
      return;
    }

    // Aggregate metrics
    const aggregated = aggregateAccountMetrics(successfulMetrics);

    // Fetch zones count
    const zonesPromises = accountIds.map(accountId =>
      fetchEnterpriseZones(apiKey, accountId)
    );
    
    const zonesResults = await Promise.allSettled(zonesPromises);
    const allZones = zonesResults
      .filter(result => result.status === 'fulfilled')
      .flatMap(result => result.value);
    
    const totalZones = allZones.length;

    console.log(`Scheduled check: Current metrics - Zones: ${totalZones}, Requests: ${aggregated.current.requests}, Bandwidth: ${aggregated.current.bytes}`);

    // Check thresholds
    const alerts = [];
    const thresholds = {
      zones: config.thresholdZones,
      requests: config.thresholdRequests,
      bandwidth: config.thresholdBandwidth,
      dnsQueries: config.thresholdDnsQueries,
    };

    if (thresholds.zones && totalZones > thresholds.zones) {
      alerts.push({
        metric: 'Enterprise Zones',
        current: totalZones,
        threshold: thresholds.zones,
        percentage: ((totalZones / thresholds.zones) * 100).toFixed(1),
      });
    }

    if (thresholds.requests && aggregated.current.requests > thresholds.requests) {
      alerts.push({
        metric: 'HTTP Requests',
        current: aggregated.current.requests,
        threshold: thresholds.requests,
        percentage: ((aggregated.current.requests / thresholds.requests) * 100).toFixed(1),
      });
    }

    if (thresholds.bandwidth && aggregated.current.bytes > thresholds.bandwidth) {
      alerts.push({
        metric: 'Data Transfer',
        current: aggregated.current.bytes,
        threshold: thresholds.bandwidth,
        percentage: ((aggregated.current.bytes / thresholds.bandwidth) * 100).toFixed(1),
      });
    }

    if (thresholds.dnsQueries && aggregated.current.dnsQueries > thresholds.dnsQueries) {
      alerts.push({
        metric: 'DNS Queries',
        current: aggregated.current.dnsQueries,
        threshold: thresholds.dnsQueries,
        percentage: ((aggregated.current.dnsQueries / thresholds.dnsQueries) * 100).toFixed(1),
      });
    }

    if (alerts.length === 0) {
      console.log('Scheduled check: All metrics within thresholds');
      return;
    }

    // Check if we already sent alerts this month
    const now = new Date();
    const monthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const alertKey = `alerts-sent:${accountIds[0]}:${monthKey}`;
    const alreadySent = await env.CONFIG_KV.get(alertKey);

    if (alreadySent) {
      console.log('Scheduled check: Alerts already sent this month, skipping');
      return;
    }

    // Send Slack notification
    const slackSent = await sendSlackAlert(config.slackWebhook, alerts, accountIds);
    
    if (slackSent) {
      // Mark alerts as sent for this month
      await env.CONFIG_KV.put(alertKey, JSON.stringify({ sentAt: now.toISOString(), alerts }), {
        expirationTtl: 32 * 24 * 60 * 60, // 32 days
      });
      console.log(`Scheduled check: Sent ${alerts.length} alert(s) to Slack`);
    } else {
      console.log('Scheduled check: Failed to send Slack notification');
    }
  } catch (error) {
    console.error('Scheduled check error:', error);
  }
}

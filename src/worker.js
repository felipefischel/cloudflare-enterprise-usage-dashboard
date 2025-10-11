/**
 * Cloudflare Worker for Enterprise Usage Dashboard
 * Handles API requests and serves static React assets
 */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    
    // API routes
    if (url.pathname.startsWith('/api/')) {
      return handleApiRequest(request, env, url);
    }
    
    // Serve static assets
    return env.ASSETS.fetch(request);
  },

  async scheduled(event, env, ctx) {
    // Run automatic threshold checks every 6 hours
    ctx.waitUntil(runScheduledThresholdCheck(env));
  },
};

/**
 * Handle API requests
 */
async function handleApiRequest(request, env, url) {
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
  
  // API Token: Prefer secret over KV (secondary option for enhanced security)
  const apiKey = env.CLOUDFLARE_API_TOKEN || body.apiKey;
  // Account IDs: Always from KV/UI (supports multi-account)
  const accountIds = parseAccountIds(body);

  if (!apiKey || accountIds.length === 0) {
    return new Response(JSON.stringify({ error: 'API credentials not configured. Please configure them in Settings.' }), {
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
  
  // First, fetch Enterprise zones to get their IDs
  const enterpriseZones = await fetchEnterpriseZones(apiKey, accountId);
  
  // If no enterprise zones, return empty metrics (don't throw error)
  if (!enterpriseZones || enterpriseZones.length === 0) {
    return {
      accountId,
      accountName,
      current: {
        totalRequests: 0,
        blockedRequests: 0,
        cleanRequests: 0,
        bytes: 0,
        dnsQueries: 0,
        requests: 0,
      },
      previous: {
        totalRequests: 0,
        blockedRequests: 0,
        cleanRequests: 0,
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

  // Calculate date ranges
  const now = new Date();
  const currentMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const currentMonthEnd = now;
  
  const previousMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59);
  
  // Check if we have cached previous month data
  const previousMonthKey = `${previousMonthStart.getFullYear()}-${String(previousMonthStart.getMonth() + 1).padStart(2, '0')}`;
  const cachedPreviousMonth = await env.CONFIG_KV.get(`monthly-stats:${accountId}:${previousMonthKey}`, 'json');

  // Build GraphQL query for current month (Enterprise zones only)
  // Use date format YYYY-MM-DD for httpRequests1dGroups
  const currentMonthDateStart = currentMonthStart.toISOString().split('T')[0];
  const currentMonthDateEnd = currentMonthEnd.toISOString().split('T')[0];
  
  // Query for total requests (main query only - firewall query causes issues)
  const currentMonthQuery = {
    operationName: 'GetEnterpriseZoneStats',
    variables: {
      zoneIds: zoneIds,
      dateStart: currentMonthDateStart,
      dateEnd: currentMonthDateEnd,
    },
    query: `query GetEnterpriseZoneStats($zoneIds: [String!]!, $dateStart: String!, $dateEnd: String!) {
      viewer {
        zones(filter: {zoneTag_in: $zoneIds}) {
          zoneTag
          httpRequests1dGroups(filter: {date_geq: $dateStart, date_leq: $dateEnd}, limit: 10000) {
            sum {
              requests
              bytes
            }
            dimensions {
              date
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
  let currentMonthTotal = { 
    totalRequests: 0, 
    blockedRequests: 0,
    cleanRequests: 0,
    bytes: 0,
    dnsQueries: 0
  };
  
  // Track per-zone metrics for primary/secondary classification
  const zoneMetrics = [];
  const SECONDARY_ZONE_THRESHOLD = 50 * (1024 ** 3); // 50GB in bytes
  
  zones.forEach(zone => {
    let zoneRequests = 0;
    let zoneBytes = 0;
    
    // Aggregate traffic per zone
    zone.httpRequests1dGroups.forEach(item => {
      zoneRequests += item.sum.requests || 0;
      zoneBytes += item.sum.bytes || 0;
      currentMonthTotal.totalRequests += item.sum.requests || 0;
      currentMonthTotal.bytes += item.sum.bytes || 0;
    });
    
    // Classify zone as primary or secondary based on bandwidth
    const isPrimary = zoneBytes >= SECONDARY_ZONE_THRESHOLD;
    
    zoneMetrics.push({
      zoneTag: zone.zoneTag,
      requests: zoneRequests,
      bytes: zoneBytes,
      dnsQueries: 0,
      isPrimary,
    });
  });
  
  // Query blocked requests using httpRequestsAdaptiveGroups with securityAction filter
  // Include all security actions: blocks, challenges, and connection closes
  try {
    const datetimeStart = currentMonthStart.toISOString();
    const datetimeEnd = currentMonthEnd.toISOString();
    
    const blockedQuery = {
      operationName: 'GetBlockedRequests',
      variables: {
        accountTag: accountId,
        filter: {
          AND: [
            {
              datetime_geq: datetimeStart,
              datetime_leq: datetimeEnd,
              requestSource: 'eyeball'
            },
            {
              OR: [
                { securityAction: 'block' },
                { securityAction: 'challenge' },
                { securityAction: 'jschallenge' },
                { securityAction: 'connection_close' },
                { securityAction: 'challenge_failed' },
                { securityAction: 'jschallenge_failed' },
                { securityAction: 'force_connection_close' },
                { securityAction: 'managed_challenge' },
                { securityAction: 'managed_challenge_failed' }
              ]
            }
          ]
        }
      },
      query: `query GetBlockedRequests($accountTag: string, $filter: AccountHttpRequestsAdaptiveGroupsFilter_InputObject) {
        viewer {
          accounts(filter: {accountTag: $accountTag}) {
            total: httpRequestsAdaptiveGroups(filter: $filter, limit: 1) {
              count
            }
          }
        }
      }`
    };

    const blockedResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`,
      },
      body: JSON.stringify(blockedQuery),
    });

    const blockedData = await blockedResponse.json();
    
    if (blockedResponse.ok && blockedData.data?.viewer?.accounts?.[0]?.total?.[0]?.count) {
      currentMonthTotal.blockedRequests = blockedData.data.viewer.accounts[0].total[0].count;
    }
  } catch (blockedError) {
    console.error('Error fetching blocked requests:', blockedError);
  }
  
  // Calculate clean traffic (billable requests = total - blocked)
  currentMonthTotal.cleanRequests = currentMonthTotal.totalRequests - currentMonthTotal.blockedRequests;

  // Fetch blocked requests and DNS queries for each zone
  try {
    const datetimeStart = currentMonthStart.toISOString();
    const datetimeEnd = currentMonthEnd.toISOString();
    
    for (const zoneMetric of zoneMetrics) {
      // Fetch blocked requests for this zone
      try {
        const blockedZoneQuery = {
          operationName: 'GetZoneBlockedRequests',
          variables: {
            zoneTag: zoneMetric.zoneTag,
            filter: {
              AND: [
                {
                  datetime_geq: datetimeStart,
                  datetime_leq: datetimeEnd,
                  requestSource: 'eyeball'
                },
                {
                  OR: [
                    { securityAction: 'block' },
                    { securityAction: 'challenge' },
                    { securityAction: 'jschallenge' },
                    { securityAction: 'connection_close' },
                    { securityAction: 'challenge_failed' },
                    { securityAction: 'jschallenge_failed' },
                    { securityAction: 'force_connection_close' },
                    { securityAction: 'managed_challenge' },
                    { securityAction: 'managed_challenge_failed' }
                  ]
                }
              ]
            }
          },
          query: `query GetZoneBlockedRequests($zoneTag: string, $filter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject) {
            viewer {
              zones(filter: {zoneTag: $zoneTag}) {
                total: httpRequestsAdaptiveGroups(filter: $filter, limit: 1) {
                  count
                }
              }
            }
          }`
        };

        const blockedZoneResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`,
          },
          body: JSON.stringify(blockedZoneQuery),
        });

        const blockedZoneData = await blockedZoneResponse.json();
        
        if (blockedZoneResponse.ok && blockedZoneData.data?.viewer?.zones?.[0]?.total?.[0]?.count) {
          zoneMetric.blockedRequests = blockedZoneData.data.viewer.zones[0].total[0].count;
        } else {
          zoneMetric.blockedRequests = 0;
        }
      } catch (blockedZoneError) {
        console.error(`Error fetching blocked requests for zone ${zoneMetric.zoneTag}:`, blockedZoneError);
        zoneMetric.blockedRequests = 0;
      }

      // Calculate clean requests for this zone
      zoneMetric.cleanRequests = zoneMetric.requests - zoneMetric.blockedRequests;
      
      // Fetch DNS queries for this zone
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
        const dnsCount = dnsData.data.viewer.zones[0].queryTotals[0].count;
        zoneMetric.dnsQueries = dnsCount;
        currentMonthTotal.dnsQueries += dnsCount;
      }
    }
  } catch (error) {
    console.error('Error fetching zone metrics:', error);
  }

  // Handle previous month data
  let previousMonthStats = { 
    totalRequests: 0,
    blockedRequests: 0,
    cleanRequests: 0,
    requests: 0, 
    bytes: 0,
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
    const previousMonthDateStart = previousMonthStart.toISOString().split('T')[0];
    const previousMonthDateEnd = previousMonthEnd.toISOString().split('T')[0];
    
    const previousMonthQuery = {
      operationName: 'GetPreviousMonthStats',
      variables: {
        zoneIds: zoneIds,
        dateStart: previousMonthDateStart,
        dateEnd: previousMonthDateEnd,
      },
      query: `query GetPreviousMonthStats($zoneIds: [String!]!, $dateStart: String!, $dateEnd: String!) {
        viewer {
          zones(filter: {zoneTag_in: $zoneIds}) {
            zoneTag
            httpRequests1dGroups(filter: {date_geq: $dateStart, date_leq: $dateEnd}, limit: 10000) {
              sum {
                requests
                bytes
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
      let zoneRequests = 0;
      let zoneBytes = 0;
      
      zone.httpRequests1dGroups.forEach(item => {
        zoneRequests += item.sum.requests || 0;
        zoneBytes += item.sum.bytes || 0;
        previousMonthStats.totalRequests += item.sum.requests || 0;
        previousMonthStats.bytes += item.sum.bytes || 0;
      });
      
      // Classify zone as primary or secondary based on bandwidth
      const isPrimary = zoneBytes >= SECONDARY_ZONE_THRESHOLD;
      
      prevZoneMetrics.push({
        zoneTag: zone.zoneTag,
        requests: zoneRequests,
        bytes: zoneBytes,
        dnsQueries: 0,
        isPrimary,
      });
    });
    
    // Fetch DNS queries for previous month
    try {
      const prevDatetimeStart = previousMonthStart.toISOString();
      const prevDatetimeEnd = previousMonthEnd.toISOString();
      
      for (const prevZoneMetric of prevZoneMetrics) {
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
          const dnsCount = dnsData.data.viewer.zones[0].queryTotals[0].count;
          prevZoneMetric.dnsQueries = dnsCount;
          previousMonthStats.dnsQueries += dnsCount;
        }
      }
    } catch (prevDnsError) {
      console.error('Error fetching previous month DNS queries:', prevDnsError);
    }
    
    // Store zone metrics in previous month stats for caching
    previousMonthStats.zoneMetrics = prevZoneMetrics;

    // Query blocked requests for previous month
    // Include all security actions: blocks, challenges, and connection closes
    try {
      const prevDatetimeStart = previousMonthStart.toISOString();
      const prevDatetimeEnd = previousMonthEnd.toISOString();
      
      const prevBlockedQuery = {
        operationName: 'GetPreviousBlockedRequests',
        variables: {
          accountTag: accountId,
          filter: {
            AND: [
              {
                datetime_geq: prevDatetimeStart,
                datetime_leq: prevDatetimeEnd,
                requestSource: 'eyeball'
              },
              {
                OR: [
                  { securityAction: 'block' },
                  { securityAction: 'challenge' },
                  { securityAction: 'jschallenge' },
                  { securityAction: 'connection_close' },
                  { securityAction: 'challenge_failed' },
                  { securityAction: 'jschallenge_failed' },
                  { securityAction: 'force_connection_close' },
                  { securityAction: 'managed_challenge' },
                  { securityAction: 'managed_challenge_failed' }
                ]
              }
            ]
          }
        },
        query: `query GetPreviousBlockedRequests($accountTag: string, $filter: AccountHttpRequestsAdaptiveGroupsFilter_InputObject) {
          viewer {
            accounts(filter: {accountTag: $accountTag}) {
              total: httpRequestsAdaptiveGroups(filter: $filter, limit: 1) {
                count
              }
            }
          }
        }`
      };

      const prevBlockedResponse = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${apiKey}`,
        },
        body: JSON.stringify(prevBlockedQuery),
      });

      const prevBlockedData = await prevBlockedResponse.json();
      
      if (prevBlockedResponse.ok && prevBlockedData.data?.viewer?.accounts?.[0]?.total?.[0]?.count) {
        previousMonthStats.blockedRequests = prevBlockedData.data.viewer.accounts[0].total[0].count;
      }
    } catch (prevBlockedError) {
      console.error('Error fetching previous month blocked requests:', prevBlockedError);
    }

    // Calculate clean traffic for previous month
    previousMonthStats.cleanRequests = previousMonthStats.totalRequests - previousMonthStats.blockedRequests;
    previousMonthStats.requests = previousMonthStats.cleanRequests; // For backward compatibility

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
      
      for (const prevZoneMetric of prevZoneMetricsForDns) {
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
          const dnsCount = dnsData.data.viewer.zones[0].queryTotals[0].count;
          prevZoneMetric.dnsQueries = dnsCount;
          previousMonthStats.dnsQueries = (previousMonthStats.dnsQueries || 0) + dnsCount;
        }
      }
      
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
      requests: currentMonthTotal.cleanRequests, // Use clean requests for historical trend
      bytes: currentMonthTotal.bytes,
      dnsQueries: currentMonthTotal.dnsQueries,
    }
  ].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

  // Calculate primary/secondary zone counts for current month
  const primaryZonesCount = zoneMetrics.filter(z => z.isPrimary).length;
  const secondaryZonesCount = zoneMetrics.filter(z => !z.isPrimary).length;

  // Calculate primary/secondary zone counts for previous month
  const prevZoneMetrics = previousMonthStats.zoneMetrics || [];
  const prevPrimaryZonesCount = prevZoneMetrics.filter(z => z.isPrimary).length;
  const prevSecondaryZonesCount = prevZoneMetrics.filter(z => !z.isPrimary).length;

  // Return structured data (not Response object)
  const result = {
    accountId,
    accountName,
    current: {
      totalRequests: currentMonthTotal.totalRequests,
      blockedRequests: currentMonthTotal.blockedRequests,
      cleanRequests: currentMonthTotal.cleanRequests,
      bytes: currentMonthTotal.bytes,
      dnsQueries: currentMonthTotal.dnsQueries,
      requests: currentMonthTotal.cleanRequests,
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
  
  return result;
}

/**
 * Aggregate metrics from multiple accounts
 */
function aggregateAccountMetrics(accountMetrics) {
  const aggregated = {
    current: {
      totalRequests: 0,
      blockedRequests: 0,
      cleanRequests: 0,
      bytes: 0,
      dnsQueries: 0,
      requests: 0,
    },
    previous: {
      totalRequests: 0,
      blockedRequests: 0,
      cleanRequests: 0,
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
  accountMetrics.forEach(accountData => {
    aggregated.current.totalRequests += accountData.current.totalRequests || 0;
    aggregated.current.blockedRequests += accountData.current.blockedRequests || 0;
    aggregated.current.cleanRequests += accountData.current.cleanRequests || 0;
    aggregated.current.bytes += accountData.current.bytes || 0;
    aggregated.current.dnsQueries += accountData.current.dnsQueries || 0;
    aggregated.current.requests += accountData.current.requests || 0;
  });

  // Aggregate previous month
  accountMetrics.forEach(accountData => {
    aggregated.previous.totalRequests += accountData.previous.totalRequests || 0;
    aggregated.previous.blockedRequests += accountData.previous.blockedRequests || 0;
    aggregated.previous.cleanRequests += accountData.previous.cleanRequests || 0;
    aggregated.previous.requests += accountData.previous.requests || accountData.previous.cleanRequests || 0;
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
  
  // API Token: Prefer secret over KV (secondary option for enhanced security)
  const apiKey = env.CLOUDFLARE_API_TOKEN || body.apiKey;
  // Account IDs: Always from KV/UI (supports multi-account)
  const accountIds = parseAccountIds(body);

  if (!apiKey || accountIds.length === 0) {
    return new Response(JSON.stringify({ error: 'API credentials not configured. Please configure them in Settings.' }), {
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

  // Store config in KV (don't store API key in plain text - this is just for demo)
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
 * Get all historical monthly data from KV
 */
async function getHistoricalMonthlyData(env, accountId) {
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
  
  return historicalData;
}

/**
 * Test firewall query to debug the correct syntax
 */
async function testFirewallQuery(request, env, corsHeaders) {
  const body = await request.json();
  const apiKey = env.CLOUDFLARE_API_TOKEN || body.apiKey;
  const accountId = body.accountId;  // Always from request body/KV
  
  if (!apiKey || !accountId) {
    return new Response(JSON.stringify({ error: 'API credentials required' }), {
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

    // API Token: Prefer secret over KV (same as API endpoints)
    const apiKey = env.CLOUDFLARE_API_TOKEN || config.apiKey;
    const accountIds = config.accountIds || (config.accountId ? [config.accountId] : []);
    
    if (accountIds.length === 0 || !apiKey) {
      console.log('Scheduled check: Missing account IDs or API key');
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

    console.log(`Scheduled check: Current metrics - Zones: ${totalZones}, Requests: ${aggregated.current.cleanRequests}, Bandwidth: ${aggregated.current.bytes}`);

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

    if (thresholds.requests && aggregated.current.cleanRequests > thresholds.requests) {
      alerts.push({
        metric: 'Clean HTTP Requests',
        current: aggregated.current.cleanRequests,
        threshold: thresholds.requests,
        percentage: ((aggregated.current.cleanRequests / thresholds.requests) * 100).toFixed(1),
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

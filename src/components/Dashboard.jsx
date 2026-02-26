import React, { useState, useEffect } from 'react';
import ConsolidatedCard from './ConsolidatedCard';
import ZonesList from './ZonesList';
import { RefreshCw, AlertCircle, Bell, BellOff, Filter, ChevronRight, Info } from 'lucide-react';
import { formatNumber, formatRequests, formatBandwidthTB, formatBytes, formatStorageMB } from '../utils/formatters';
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
  const [showAlertPopover, setShowAlertPopover] = useState(false);

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

    } catch (err) {
      console.error('Error fetching data:', err);
      setError(err.message || 'Failed to fetch data from Cloudflare API');
    } finally {
      setLoading(false);
      setLoadingPhase(null);
    }
  };

  const buildSkuMetrics = (metricsData, zonesData) => {
    const skus = [];
    const fmtNum = (v) => { if (v >= 1e9) return `${(v/1e9).toFixed(2)}B`; if (v >= 1e6) return `${(v/1e6).toFixed(2)}M`; if (v >= 1e3) return `${(v/1e3).toFixed(1)}K`; return String(Math.round(v)); };
    const fmtBytes = (b) => { if (b >= 1e12) return `${(b/1e12).toFixed(2)} TB`; if (b >= 1e9) return `${(b/1e9).toFixed(2)} GB`; if (b >= 1e6) return `${(b/1e6).toFixed(2)} MB`; return `${b} B`; };
    const fmtGB = (gb) => gb >= 1000 ? `${(gb/1000).toFixed(2)} TB` : `${gb.toFixed(2)} GB`;
    const fmtMB = (mb) => mb >= 1000 ? fmtGB(mb / 1000) : `${mb.toFixed(2)} MB`;
    const fmtMin = (m) => m >= 1e6 ? `${(m/1e6).toFixed(2)}M min` : m >= 1e3 ? `${(m/1e3).toFixed(1)}K min` : `${Math.round(m)} min`;
    const fmtBw = (m) => m >= 1000 ? `${(m/1000).toFixed(2)} Gbps` : `${m.toFixed(2)} Mbps`;
    const pct = (cur, thr) => thr ? (cur / thr) * 100 : 0;
    const add = (key, name, category, current, threshold, formatted, thresholdFormatted) => {
      if (current !== undefined && current !== null) {
        skus.push({ key, name, category, current, threshold: threshold || null, formatted, thresholdFormatted: thresholdFormatted || '', percentage: threshold ? pct(current, threshold) : 0 });
      }
    };

    const appCfg = config?.applicationServices || {};
    const zc = zonesData?.enterprise || 0;
    const zt = appCfg.core?.thresholdZones || config?.thresholdZones;
    add('zones', 'Enterprise Zones', 'Zones & Traffic', zc, zt, `${zc} zones`, zt ? `${zt} zones` : '');
    const rc = metricsData?.current?.requests || 0;
    const rt = appCfg.core?.thresholdRequests || config?.thresholdRequests;
    add('requests', 'HTTP Requests', 'Zones & Traffic', rc, rt, fmtNum(rc), rt ? fmtNum(rt) : '');
    const bc = metricsData?.current?.bytes || 0;
    const bt = appCfg.core?.thresholdBandwidth || config?.thresholdBandwidth;
    add('bandwidth', 'Data Transfer', 'Zones & Traffic', bc, bt, fmtBytes(bc), bt ? fmtBytes(bt) : '');
    const dc = metricsData?.current?.dnsQueries || 0;
    const dt = appCfg.core?.thresholdDnsQueries || config?.thresholdDnsQueries;
    add('dnsQueries', 'DNS Queries', 'Zones & Traffic', dc, dt, fmtNum(dc), dt ? fmtNum(dt) : '');

    if (metricsData?.botManagement?.enabled) {
      const c = metricsData.botManagement.current?.likelyHuman || 0, t = appCfg.botManagement?.threshold;
      add('botManagement', 'Bot Management', 'Application Security', c, t, fmtNum(c), t ? fmtNum(t) : '');
    }
    if (metricsData?.apiShield?.enabled) {
      const c = metricsData.apiShield.current?.requests || 0, t = appCfg.apiShield?.threshold;
      add('apiShield', 'API Shield', 'Application Security', c, t, fmtNum(c), t ? fmtNum(t) : '');
    }
    if (metricsData?.pageShield?.enabled) {
      const c = metricsData.pageShield.current?.requests || 0, t = appCfg.pageShield?.threshold;
      add('pageShield', 'Page Shield', 'Application Security', c, t, fmtNum(c), t ? fmtNum(t) : '');
    }
    if (metricsData?.advancedRateLimiting?.enabled) {
      const c = metricsData.advancedRateLimiting.current?.requests || 0, t = appCfg.advancedRateLimiting?.threshold;
      add('advancedRateLimiting', 'Adv. Rate Limiting', 'Application Security', c, t, fmtNum(c), t ? fmtNum(t) : '');
    }
    if (metricsData?.argo?.enabled) {
      const c = metricsData.argo.current?.bytes || 0, t = appCfg.argo?.threshold;
      add('argo', 'Argo Smart Routing', 'Delivery & Performance', c, t, fmtBytes(c), t ? fmtBytes(t) : '');
    }
    if (metricsData?.cacheReserve?.enabled) {
      const cr = metricsData.cacheReserve, cfg = appCfg.cacheReserve || {};
      const crGB = cr.current?.storageGBDays || 0;
      const crThr = cfg.storageThreshold ? cfg.storageThreshold * 1000 : null;
      add('cacheReserve-storage', 'Cache Reserve — Storage', 'Delivery & Performance', crGB, crThr, crGB >= 1000 ? `${(crGB/1000).toFixed(2)} TB` : `${crGB.toFixed(2)} GB`, crThr ? (crThr >= 1000 ? `${(crThr/1000).toFixed(2)} TB` : `${crThr.toFixed(2)} GB`) : '');
      add('cacheReserve-classA', 'Cache Reserve — Class A Ops', 'Delivery & Performance', cr.current?.classAOps || 0, cfg.classAOpsThreshold, fmtNum(cr.current?.classAOps || 0), cfg.classAOpsThreshold ? fmtNum(cfg.classAOpsThreshold) : '');
      add('cacheReserve-classB', 'Cache Reserve — Class B Ops', 'Delivery & Performance', cr.current?.classBOps || 0, cfg.classBOpsThreshold, fmtNum(cr.current?.classBOps || 0), cfg.classBOpsThreshold ? fmtNum(cfg.classBOpsThreshold) : '');
    }
    if (metricsData?.loadBalancing?.enabled) {
      const c = metricsData.loadBalancing.current?.endpoints || 0, t = appCfg.loadBalancing?.threshold;
      add('loadBalancing', 'Load Balancing', 'Delivery & Performance', c, t, `${c} endpoints`, t ? `${t} endpoints` : '');
    }
    if (metricsData?.customHostnames?.enabled) {
      const c = metricsData.customHostnames.current?.hostnames || 0, t = appCfg.customHostnames?.threshold;
      add('customHostnames', 'Custom Hostnames', 'Delivery & Performance', c, t, `${c} hostnames`, t ? `${t} hostnames` : '');
    }
    if (metricsData?.logExplorer?.enabled) {
      const c = metricsData.logExplorer.current?.billableGB || 0, t = appCfg.logExplorer?.threshold;
      add('logExplorer', 'Log Explorer', 'Logs & Analytics', c, t, fmtGB(c), t ? fmtGB(t) : '');
    }

    const ztCfg = config?.zeroTrust || {};
    if (metricsData?.zeroTrustSeats?.enabled) {
      const c = metricsData.zeroTrustSeats.current?.seats || 0, t = ztCfg.seats?.threshold;
      add('zeroTrustSeats', 'Zero Trust Seats', 'Cloudflare One', c, t, `${c} seats`, t ? `${t} seats` : '');
    }

    const netCfg = config?.networkServices || {};
    if (metricsData?.magicTransit?.enabled) {
      const c = metricsData.magicTransit.current?.ingressP95Mbps || 0, t = netCfg.magicTransit?.threshold;
      add('magicTransit', 'Magic Transit — Ingress', 'Network Services', c, t, fmtBw(c), t ? fmtBw(t) : '');
      if (netCfg.magicTransit?.egressEnabled) {
        const ec = metricsData.magicTransit.current?.egressP95Mbps || 0, et = netCfg.magicTransit?.egressThreshold;
        add('magicTransit-egress', 'Magic Transit — Egress', 'Network Services', ec, et, fmtBw(ec), et ? fmtBw(et) : '');
      }
    }
    if (metricsData?.magicWan?.enabled) {
      const c = metricsData.magicWan.current?.p95Mbps || 0, t = netCfg.magicWan?.threshold;
      add('magicWan', 'Magic WAN', 'Cloudflare One', c, t, fmtBw(c), t ? fmtBw(t) : '');
    }
    if (metricsData?.spectrum?.enabled) {
      const sp = metricsData.spectrum, cfg = netCfg.spectrum || {};
      const spdt = cfg.dataTransferThreshold ? cfg.dataTransferThreshold * 1e12 : null;
      add('spectrum-transfer', 'Spectrum — Data Transfer', 'Network Services', sp.current?.dataTransfer || 0, spdt, fmtBytes(sp.current?.dataTransfer || 0), spdt ? fmtBytes(spdt) : '');
      add('spectrum-conns', 'Spectrum — Connections', 'Network Services', sp.current?.p95Concurrent || 0, cfg.connectionsThreshold, fmtNum(sp.current?.p95Concurrent || 0), cfg.connectionsThreshold ? fmtNum(cfg.connectionsThreshold) : '');
    }

    const devCfg = config?.developerServices || {};
    if (metricsData?.workersPages?.enabled) {
      const wp = metricsData.workersPages, cfg = devCfg.workersPages || {};
      const wpt = cfg.requestsThreshold ? cfg.requestsThreshold * 1e6 : null;
      const wpct = cfg.cpuTimeThreshold ? cfg.cpuTimeThreshold * 1e6 : null;
      add('workersPages-req', 'Workers & Pages — Requests', 'Developer Platform', wp.current?.requests || 0, wpt, fmtNum(wp.current?.requests || 0), wpt ? fmtNum(wpt) : '');
      add('workersPages-cpu', 'Workers & Pages — CPU Time', 'Developer Platform', wp.current?.cpuTimeMs || 0, wpct, fmtNum(wp.current?.cpuTimeMs || 0) + ' ms', wpct ? fmtNum(wpct) + ' ms' : '');
    }
    if (metricsData?.r2Storage?.enabled) {
      const r2 = metricsData.r2Storage, cfg = devCfg.r2Storage || {};
      const r2at = cfg.classAOpsThreshold ? cfg.classAOpsThreshold * 1e6 : null;
      const r2bt = cfg.classBOpsThreshold ? cfg.classBOpsThreshold * 1e6 : null;
      add('r2-storage', 'R2 — Storage', 'Developer Platform', r2.current?.storageGB || 0, cfg.storageThreshold, fmtGB(r2.current?.storageGB || 0), cfg.storageThreshold ? fmtGB(cfg.storageThreshold) : '');
      add('r2-classA', 'R2 — Class A Ops', 'Developer Platform', r2.current?.classAOps || 0, r2at, fmtNum(r2.current?.classAOps || 0), r2at ? fmtNum(r2at) : '');
      add('r2-classB', 'R2 — Class B Ops', 'Developer Platform', r2.current?.classBOps || 0, r2bt, fmtNum(r2.current?.classBOps || 0), r2bt ? fmtNum(r2bt) : '');
    }
    if (metricsData?.d1?.enabled) {
      const d = metricsData.d1, cfg = devCfg.d1 || {};
      const d1rr = cfg.rowsReadThreshold ? cfg.rowsReadThreshold * 1e6 : null;
      const d1rw = cfg.rowsWrittenThreshold ? cfg.rowsWrittenThreshold * 1e6 : null;
      const d1mb = d.current?.storageMB || 0;
      const d1st = cfg.storageThreshold ? cfg.storageThreshold * 1000 : null;
      add('d1-storage', 'D1 — Storage', 'Developer Platform', d1mb, d1st, fmtMB(d1mb), d1st ? fmtMB(d1st) : '');
      add('d1-rowsRead', 'D1 — Rows Read', 'Developer Platform', d.current?.rowsRead || 0, d1rr, fmtNum(d.current?.rowsRead || 0), d1rr ? fmtNum(d1rr) : '');
      add('d1-rowsWritten', 'D1 — Rows Written', 'Developer Platform', d.current?.rowsWritten || 0, d1rw, fmtNum(d.current?.rowsWritten || 0), d1rw ? fmtNum(d1rw) : '');
    }
    if (metricsData?.kv?.enabled) {
      const k = metricsData.kv, cfg = devCfg.kv || {};
      const kvr = cfg.readsThreshold ? cfg.readsThreshold * 1e6 : null;
      const kvw = cfg.writesThreshold ? cfg.writesThreshold * 1e6 : null;
      const kvd = cfg.deletesThreshold ? cfg.deletesThreshold * 1e6 : null;
      const kvl = cfg.listsThreshold ? cfg.listsThreshold * 1e6 : null;
      add('kv-reads', 'KV — Reads', 'Developer Platform', k.current?.reads || 0, kvr, fmtNum(k.current?.reads || 0), kvr ? fmtNum(kvr) : '');
      add('kv-writes', 'KV — Writes', 'Developer Platform', k.current?.writes || 0, kvw, fmtNum(k.current?.writes || 0), kvw ? fmtNum(kvw) : '');
      add('kv-deletes', 'KV — Deletes', 'Developer Platform', k.current?.deletes || 0, kvd, fmtNum(k.current?.deletes || 0), kvd ? fmtNum(kvd) : '');
      add('kv-lists', 'KV — Lists', 'Developer Platform', k.current?.lists || 0, kvl, fmtNum(k.current?.lists || 0), kvl ? fmtNum(kvl) : '');
      const kvst = cfg.storageThreshold ? cfg.storageThreshold * 1000 : null;
      add('kv-storage', 'KV — Storage', 'Developer Platform', k.current?.storageMB || 0, kvst, fmtMB(k.current?.storageMB || 0), kvst ? fmtMB(kvst) : '');
    }
    if (metricsData?.stream?.enabled) {
      const s = metricsData.stream, cfg = devCfg.stream || {};
      const sst = cfg.minutesStoredThreshold ? cfg.minutesStoredThreshold * 1e3 : null;
      const sdt = cfg.minutesDeliveredThreshold ? cfg.minutesDeliveredThreshold * 1e3 : null;
      add('stream-stored', 'Stream — Minutes Stored', 'Developer Platform', s.current?.minutesStored || 0, sst, fmtMin(s.current?.minutesStored || 0), sst ? fmtMin(sst) : '');
      add('stream-delivered', 'Stream — Minutes Delivered', 'Developer Platform', s.current?.minutesDelivered || 0, sdt, fmtMin(s.current?.minutesDelivered || 0), sdt ? fmtMin(sdt) : '');
    }
    if (metricsData?.images?.enabled) {
      const im = metricsData.images, cfg = devCfg.images || {};
      const ist = cfg.imagesStoredThreshold ? cfg.imagesStoredThreshold * 1e3 : null;
      const idt = cfg.imagesDeliveredThreshold ? cfg.imagesDeliveredThreshold * 1e3 : null;
      add('images-stored', 'Images — Stored', 'Developer Platform', im.current?.imagesStored || 0, ist, fmtNum(im.current?.imagesStored || 0), ist ? fmtNum(ist) : '');
      add('images-delivered', 'Images — Delivered', 'Developer Platform', im.current?.imagesDelivered || 0, idt, fmtNum(im.current?.imagesDelivered || 0), idt ? fmtNum(idt) : '');
    }
    if (metricsData?.workersAI?.enabled) {
      const c = metricsData.workersAI.current?.neurons || 0, t = devCfg.workersAI?.neuronsThreshold ? devCfg.workersAI.neuronsThreshold * 1e6 : null;
      add('workersAI', 'Workers AI', 'Developer Platform', c, t, fmtNum(c) + ' neurons', t ? fmtNum(t) + ' neurons' : '');
    }
    if (metricsData?.queues?.enabled) {
      const c = metricsData.queues.current?.operations || 0, t = devCfg.queues?.operationsThreshold ? devCfg.queues.operationsThreshold * 1e6 : null;
      add('queues', 'Queues', 'Developer Platform', c, t, fmtNum(c) + ' ops', t ? fmtNum(t) + ' ops' : '');
    }
    if (metricsData?.workersLogsTraces?.enabled) {
      const c = metricsData.workersLogsTraces.current?.events || 0, t = devCfg.workersLogsTraces?.eventsThreshold ? devCfg.workersLogsTraces.eventsThreshold * 1e6 : null;
      add('workersLogsTraces', 'Workers Observability', 'Developer Platform', c, t, fmtNum(c) + ' events', t ? fmtNum(t) + ' events' : '');
    }
    if (metricsData?.durableObjects?.enabled) {
      const d = metricsData.durableObjects, cfg = devCfg.durableObjects || {};
      const dort = cfg.requestsThreshold ? cfg.requestsThreshold * 1e6 : null;
      const fmtGBs = (v) => v >= 1e6 ? `${(v/1e6).toFixed(2)}M GB-s` : v >= 1e3 ? `${(v/1e3).toFixed(2)}K GB-s` : `${v.toFixed(2)} GB-s`;
      add('do-requests', 'Durable Objects — Requests', 'Developer Platform', d.current?.requests || 0, dort, fmtNum(d.current?.requests || 0), dort ? fmtNum(dort) : '');
      add('do-duration', 'Durable Objects — Duration', 'Developer Platform', d.current?.durationGBs || 0, cfg.durationThreshold, fmtGBs(d.current?.durationGBs || 0), cfg.durationThreshold ? fmtGBs(cfg.durationThreshold) : '');
      const domb = d.current?.storageMB || 0;
      const dost = cfg.storageThreshold ? cfg.storageThreshold * 1000 : null;
      add('do-storage', 'Durable Objects — Storage', 'Developer Platform', domb, dost, fmtMB(domb), dost ? fmtMB(dost) : '');
    }
    return skus;
  };

  const sendSlackMessage = async (metricsData, zonesData, mode = 'alert') => {
    const accountIds = config?.accountIds || (config?.accountId ? [config.accountId] : []);
    const skuMetrics = buildSkuMetrics(metricsData, zonesData);

    try {
      const response = await fetch('/api/webhook/check', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          skuMetrics,
          slackWebhook: config.slackWebhook,
          accountIds,
          mode,
          alertFrequency: config.alertFrequency || 'monthly',
        }),
      });

      const result = await response.json();
      setLastChecked(new Date());

      if (mode === 'report') {
        alert(result.success ? `✅ ${result.message}` : `❌ ${result.message}`);
      }
    } catch (error) {
      console.error('Error sending Slack message:', error);
      if (mode === 'report') {
        alert('❌ Failed to send report. Please try again.');
      }
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
          argo: metrics.argo ? {
            ...metrics.argo,
            threshold: config?.applicationServices?.argo?.threshold || metrics.argo.threshold,
          } : null,
          cacheReserve: metrics.cacheReserve ? {
            ...metrics.cacheReserve,
            storageThreshold: config?.applicationServices?.cacheReserve?.storageThreshold || metrics.cacheReserve.storageThreshold,
            classAOpsThreshold: config?.applicationServices?.cacheReserve?.classAOpsThreshold || metrics.cacheReserve.classAOpsThreshold,
            classBOpsThreshold: config?.applicationServices?.cacheReserve?.classBOpsThreshold || metrics.cacheReserve.classBOpsThreshold,
          } : null,
          loadBalancing: metrics.loadBalancing ? {
            ...metrics.loadBalancing,
            threshold: config?.applicationServices?.loadBalancing?.threshold || metrics.loadBalancing.threshold,
          } : null,
          customHostnames: metrics.customHostnames ? {
            ...metrics.customHostnames,
            threshold: config?.applicationServices?.customHostnames?.threshold || metrics.customHostnames.threshold,
          } : null,
          logExplorer: metrics.logExplorer ? {
            ...metrics.logExplorer,
            threshold: config?.applicationServices?.logExplorer?.threshold || metrics.logExplorer.threshold,
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
          d1: metrics.d1 ? {
            ...metrics.d1,
            rowsReadThreshold: config?.developerServices?.d1?.rowsReadThreshold || metrics.d1.rowsReadThreshold,
            rowsWrittenThreshold: config?.developerServices?.d1?.rowsWrittenThreshold || metrics.d1.rowsWrittenThreshold,
            storageThreshold: config?.developerServices?.d1?.storageThreshold || metrics.d1.storageThreshold,
          } : null,
          kv: metrics.kv ? {
            ...metrics.kv,
            readsThreshold: config?.developerServices?.kv?.readsThreshold || metrics.kv.readsThreshold,
            writesThreshold: config?.developerServices?.kv?.writesThreshold || metrics.kv.writesThreshold,
            deletesThreshold: config?.developerServices?.kv?.deletesThreshold || metrics.kv.deletesThreshold,
            listsThreshold: config?.developerServices?.kv?.listsThreshold || metrics.kv.listsThreshold,
            storageThreshold: config?.developerServices?.kv?.storageThreshold || metrics.kv.storageThreshold,
          } : null,
          stream: metrics.stream ? {
            ...metrics.stream,
            minutesStoredThreshold: config?.developerServices?.stream?.minutesStoredThreshold || metrics.stream.minutesStoredThreshold,
            minutesDeliveredThreshold: config?.developerServices?.stream?.minutesDeliveredThreshold || metrics.stream.minutesDeliveredThreshold,
          } : null,
          images: metrics.images ? {
            ...metrics.images,
            imagesStoredThreshold: config?.developerServices?.images?.imagesStoredThreshold || metrics.images.imagesStoredThreshold,
            imagesDeliveredThreshold: config?.developerServices?.images?.imagesDeliveredThreshold || metrics.images.imagesDeliveredThreshold,
          } : null,
          workersAI: metrics.workersAI ? {
            ...metrics.workersAI,
            neuronsThreshold: config?.developerServices?.workersAI?.neuronsThreshold || metrics.workersAI.neuronsThreshold,
          } : null,
          queues: metrics.queues ? {
            ...metrics.queues,
            operationsThreshold: config?.developerServices?.queues?.operationsThreshold || metrics.queues.operationsThreshold,
          } : null,
          workersLogsTraces: metrics.workersLogsTraces ? {
            ...metrics.workersLogsTraces,
            eventsThreshold: config?.developerServices?.workersLogsTraces?.eventsThreshold || metrics.workersLogsTraces.eventsThreshold,
          } : null,
          durableObjects: metrics.durableObjects ? {
            ...metrics.durableObjects,
            sqliteEnabled: config?.developerServices?.durableObjects?.sqliteEnabled || false,
            kvStorageEnabled: config?.developerServices?.durableObjects?.kvStorageEnabled || false,
            requestsThreshold: config?.developerServices?.durableObjects?.requestsThreshold || metrics.durableObjects.requestsThreshold,
            durationThreshold: config?.developerServices?.durableObjects?.durationThreshold || metrics.durableObjects.durationThreshold,
            sqliteRowsReadThreshold: config?.developerServices?.durableObjects?.sqliteRowsReadThreshold || metrics.durableObjects.sqliteRowsReadThreshold,
            sqliteRowsWrittenThreshold: config?.developerServices?.durableObjects?.sqliteRowsWrittenThreshold || metrics.durableObjects.sqliteRowsWrittenThreshold,
            kvReadUnitsThreshold: config?.developerServices?.durableObjects?.kvReadUnitsThreshold || metrics.durableObjects.kvReadUnitsThreshold,
            kvWriteUnitsThreshold: config?.developerServices?.durableObjects?.kvWriteUnitsThreshold || metrics.durableObjects.kvWriteUnitsThreshold,
            kvDeletesThreshold: config?.developerServices?.durableObjects?.kvDeletesThreshold || metrics.durableObjects.kvDeletesThreshold,
            storageThreshold: config?.developerServices?.durableObjects?.storageThreshold || metrics.durableObjects.storageThreshold,
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
          spectrum: metrics.spectrum ? {
            ...metrics.spectrum,
            dataTransferThreshold: config?.networkServices?.spectrum?.dataTransferThreshold || metrics.spectrum.dataTransferThreshold,
            connectionsThreshold: config?.networkServices?.spectrum?.connectionsThreshold || metrics.spectrum.connectionsThreshold,
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
    }

    // Filter Argo data for selected account
    let filteredArgo = null;
    if (metrics.argo && metrics.argo.enabled) {
      const accountArgoData = metrics.argo.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );

      if (accountArgoData) {
        filteredArgo = {
          enabled: true,
          threshold: config?.applicationServices?.argo?.threshold || metrics.argo.threshold,
          current: accountArgoData.current,
          previous: accountArgoData.previous,
          timeSeries: accountArgoData.timeSeries,
        };
      }
    }

    let filteredCacheReserve = null;
    if (metrics.cacheReserve && metrics.cacheReserve.enabled) {
      const accountZoneIds = new Set((accountZones || []).map(z => z.id));
      const accountCRZones = metrics.cacheReserve.perZoneData?.filter(
        z => accountZoneIds.has(z.zoneId)
      ) || [];
      if (accountCRZones.length > 0) {
        const timeSeriesMap = new Map();
        accountCRZones.forEach(z => {
          (z.timeSeries || []).forEach(ts => {
            const existing = timeSeriesMap.get(ts.month);
            if (existing) {
              existing.storageGBDays += ts.storageGBDays || 0;
              existing.classAOps += ts.classAOps || 0;
              existing.classBOps += ts.classBOps || 0;
            } else {
              timeSeriesMap.set(ts.month, { ...ts, storageGBDays: ts.storageGBDays || 0, classAOps: ts.classAOps || 0, classBOps: ts.classBOps || 0 });
            }
          });
        });
        filteredCacheReserve = {
          enabled: true,
          storageThreshold: config?.applicationServices?.cacheReserve?.storageThreshold || metrics.cacheReserve.storageThreshold,
          classAOpsThreshold: config?.applicationServices?.cacheReserve?.classAOpsThreshold || metrics.cacheReserve.classAOpsThreshold,
          classBOpsThreshold: config?.applicationServices?.cacheReserve?.classBOpsThreshold || metrics.cacheReserve.classBOpsThreshold,
          current: {
            storageGBDays: accountCRZones.reduce((s, z) => s + (z.current?.storageGBDays || 0), 0),
            classAOps: accountCRZones.reduce((s, z) => s + (z.current?.classAOps || 0), 0),
            classBOps: accountCRZones.reduce((s, z) => s + (z.current?.classBOps || 0), 0),
            zones: accountCRZones.map(z => ({ zoneId: z.zoneId, zoneName: z.zoneName, storageGBDays: z.current?.storageGBDays || 0, classAOps: z.current?.classAOps || 0, classBOps: z.current?.classBOps || 0 })),
          },
          previous: {
            storageGBDays: accountCRZones.reduce((s, z) => s + (z.previous?.storageGBDays || 0), 0),
            classAOps: accountCRZones.reduce((s, z) => s + (z.previous?.classAOps || 0), 0),
            classBOps: accountCRZones.reduce((s, z) => s + (z.previous?.classBOps || 0), 0),
            zones: accountCRZones.map(z => ({ zoneId: z.zoneId, zoneName: z.zoneName, storageGBDays: z.previous?.storageGBDays || 0, classAOps: z.previous?.classAOps || 0, classBOps: z.previous?.classBOps || 0 })),
          },
          timeSeries: Array.from(timeSeriesMap.values()).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp)),
          perZoneData: accountCRZones,
        };
      }
    }

    // Filter Load Balancing data for selected account
    let filteredLoadBalancing = null;
    if (metrics.loadBalancing && metrics.loadBalancing.enabled) {
      const accountLbData = metrics.loadBalancing.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      if (accountLbData) {
        filteredLoadBalancing = {
          enabled: true,
          threshold: config?.applicationServices?.loadBalancing?.threshold || metrics.loadBalancing.threshold,
          current: accountLbData.current,
          previous: accountLbData.previous,
          timeSeries: accountLbData.timeSeries,
        };
      }
    }
    
    // Filter Custom Hostnames data for selected account
    let filteredCustomHostnames = null;
    if (metrics.customHostnames && metrics.customHostnames.enabled) {
      const accountChData = metrics.customHostnames.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      if (accountChData) {
        filteredCustomHostnames = {
          enabled: true,
          threshold: config?.applicationServices?.customHostnames?.threshold || metrics.customHostnames.threshold,
          current: accountChData.current,
          previous: accountChData.previous,
          timeSeries: accountChData.timeSeries,
        };
      }
    }
    
    // Filter Log Explorer data for selected account
    let filteredLogExplorer = null;
    if (metrics.logExplorer && metrics.logExplorer.enabled) {
      const accountLeData = metrics.logExplorer.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );
      if (accountLeData) {
        filteredLogExplorer = {
          enabled: true,
          threshold: config?.applicationServices?.logExplorer?.threshold || metrics.logExplorer.threshold,
          current: accountLeData.current,
          previous: accountLeData.previous,
          timeSeries: accountLeData.timeSeries,
        };
      }
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

    // Filter D1 data for selected account
    let filteredD1 = null;
    if (metrics.d1 && metrics.d1.enabled) {
      const accountD1Data = metrics.d1.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );

      if (accountD1Data) {
        filteredD1 = {
          enabled: true,
          rowsReadThreshold: config?.developerServices?.d1?.rowsReadThreshold || metrics.d1.rowsReadThreshold,
          rowsWrittenThreshold: config?.developerServices?.d1?.rowsWrittenThreshold || metrics.d1.rowsWrittenThreshold,
          storageThreshold: config?.developerServices?.d1?.storageThreshold || metrics.d1.storageThreshold,
          current: accountD1Data.current,
          previous: accountD1Data.previous,
          timeSeries: accountD1Data.timeSeries,
        };
      }
    }

    // Filter Durable Objects data for selected account
    let filteredDO = null;
    if (metrics.durableObjects && metrics.durableObjects.enabled) {
      const accountDOData = metrics.durableObjects.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );

      if (accountDOData) {
        filteredDO = {
          enabled: true,
          sqliteEnabled: config?.developerServices?.durableObjects?.sqliteEnabled ?? true,
          kvStorageEnabled: config?.developerServices?.durableObjects?.kvStorageEnabled ?? true,
          requestsThreshold: config?.developerServices?.durableObjects?.requestsThreshold || metrics.durableObjects.requestsThreshold,
          durationThreshold: config?.developerServices?.durableObjects?.durationThreshold || metrics.durableObjects.durationThreshold,
          sqliteRowsReadThreshold: config?.developerServices?.durableObjects?.sqliteRowsReadThreshold || metrics.durableObjects.sqliteRowsReadThreshold,
          sqliteRowsWrittenThreshold: config?.developerServices?.durableObjects?.sqliteRowsWrittenThreshold || metrics.durableObjects.sqliteRowsWrittenThreshold,
          kvReadUnitsThreshold: config?.developerServices?.durableObjects?.kvReadUnitsThreshold || metrics.durableObjects.kvReadUnitsThreshold,
          kvWriteUnitsThreshold: config?.developerServices?.durableObjects?.kvWriteUnitsThreshold || metrics.durableObjects.kvWriteUnitsThreshold,
          kvDeletesThreshold: config?.developerServices?.durableObjects?.kvDeletesThreshold || metrics.durableObjects.kvDeletesThreshold,
          storageThreshold: config?.developerServices?.durableObjects?.storageThreshold || metrics.durableObjects.storageThreshold,
          current: accountDOData.current,
          previous: accountDOData.previous,
          timeSeries: accountDOData.timeSeries,
        };
      }
    }

    // Filter KV data for selected account
    let filteredKV = null;
    if (metrics.kv && metrics.kv.enabled) {
      const accountKVData = metrics.kv.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );

      if (accountKVData) {
        filteredKV = {
          enabled: true,
          readsThreshold: config?.developerServices?.kv?.readsThreshold || metrics.kv.readsThreshold,
          writesThreshold: config?.developerServices?.kv?.writesThreshold || metrics.kv.writesThreshold,
          deletesThreshold: config?.developerServices?.kv?.deletesThreshold || metrics.kv.deletesThreshold,
          listsThreshold: config?.developerServices?.kv?.listsThreshold || metrics.kv.listsThreshold,
          storageThreshold: config?.developerServices?.kv?.storageThreshold || metrics.kv.storageThreshold,
          current: accountKVData.current,
          previous: accountKVData.previous,
          timeSeries: accountKVData.timeSeries,
        };
      }
    }
    
    // Filter Workers Logs & Traces data for selected account
    let filteredWorkersLogsTraces = null;
    if (metrics.workersLogsTraces && metrics.workersLogsTraces.enabled) {
      const accountWLTData = metrics.workersLogsTraces.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );

      if (accountWLTData) {
        filteredWorkersLogsTraces = {
          enabled: true,
          eventsThreshold: config?.developerServices?.workersLogsTraces?.eventsThreshold || metrics.workersLogsTraces.eventsThreshold,
          current: accountWLTData.current,
          previous: accountWLTData.previous,
          timeSeries: accountWLTData.timeSeries,
        };
      }
    }

    // Filter Queues data for selected account
    let filteredQueues = null;
    if (metrics.queues && metrics.queues.enabled) {
      const accountQueuesData = metrics.queues.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );

      if (accountQueuesData) {
        filteredQueues = {
          enabled: true,
          operationsThreshold: config?.developerServices?.queues?.operationsThreshold || metrics.queues.operationsThreshold,
          current: accountQueuesData.current,
          previous: accountQueuesData.previous,
          timeSeries: accountQueuesData.timeSeries,
        };
      }
    }

    // Filter Workers AI data for selected account
    let filteredWorkersAI = null;
    if (metrics.workersAI && metrics.workersAI.enabled) {
      const accountWAIData = metrics.workersAI.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );

      if (accountWAIData) {
        filteredWorkersAI = {
          enabled: true,
          neuronsThreshold: config?.developerServices?.workersAI?.neuronsThreshold || metrics.workersAI.neuronsThreshold,
          current: accountWAIData.current,
          previous: accountWAIData.previous,
          timeSeries: accountWAIData.timeSeries,
        };
      }
    }

    // Filter Images data for selected account
    let filteredImages = null;
    if (metrics.images && metrics.images.enabled) {
      const accountImagesData = metrics.images.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );

      if (accountImagesData) {
        filteredImages = {
          enabled: true,
          imagesStoredThreshold: config?.developerServices?.images?.imagesStoredThreshold || metrics.images.imagesStoredThreshold,
          imagesDeliveredThreshold: config?.developerServices?.images?.imagesDeliveredThreshold || metrics.images.imagesDeliveredThreshold,
          current: accountImagesData.current,
          previous: accountImagesData.previous,
          timeSeries: accountImagesData.timeSeries,
        };
      }
    }

    // Filter Stream data for selected account
    let filteredStream = null;
    if (metrics.stream && metrics.stream.enabled) {
      const accountStreamData = metrics.stream.perAccountData?.find(
        acc => acc.accountId === selectedAccount
      );

      if (accountStreamData) {
        filteredStream = {
          enabled: true,
          minutesStoredThreshold: config?.developerServices?.stream?.minutesStoredThreshold || metrics.stream.minutesStoredThreshold,
          minutesDeliveredThreshold: config?.developerServices?.stream?.minutesDeliveredThreshold || metrics.stream.minutesDeliveredThreshold,
          current: accountStreamData.current,
          previous: accountStreamData.previous,
          timeSeries: accountStreamData.timeSeries,
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
    
    // Filter Spectrum data for selected account (zone-based)
    let filteredSpectrum = null;
    if (metrics.spectrum && metrics.spectrum.enabled) {
      const accountZoneIds = new Set((accountZones || []).map(z => z.id));
      const accountSpecZones = metrics.spectrum.perZoneData?.filter(
        z => accountZoneIds.has(z.zoneId)
      ) || [];
      if (accountSpecZones.length > 0) {
        const timeSeriesMap = new Map();
        accountSpecZones.forEach(z => {
          (z.timeSeries || []).forEach(ts => {
            const existing = timeSeriesMap.get(ts.month);
            if (existing) {
              existing.dataTransfer += ts.dataTransfer || 0;
              existing.p95Concurrent = Math.max(existing.p95Concurrent, ts.p95Concurrent || 0);
            } else {
              timeSeriesMap.set(ts.month, { ...ts, dataTransfer: ts.dataTransfer || 0, p95Concurrent: ts.p95Concurrent || 0 });
            }
          });
        });
        filteredSpectrum = {
          enabled: true,
          dataTransferThreshold: config?.networkServices?.spectrum?.dataTransferThreshold || metrics.spectrum.dataTransferThreshold,
          connectionsThreshold: config?.networkServices?.spectrum?.connectionsThreshold || metrics.spectrum.connectionsThreshold,
          current: {
            dataTransfer: accountSpecZones.reduce((s, z) => s + (z.current?.dataTransfer || 0), 0),
            p95Concurrent: Math.max(...accountSpecZones.map(z => z.current?.p95Concurrent || 0)),
          },
          previous: {
            dataTransfer: accountSpecZones.reduce((s, z) => s + (z.previous?.dataTransfer || 0), 0),
            p95Concurrent: Math.max(...accountSpecZones.map(z => z.previous?.p95Concurrent || 0)),
          },
          timeSeries: Array.from(timeSeriesMap.values()).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp)),
          perZoneData: accountSpecZones,
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
        argo: filteredArgo,
        cacheReserve: filteredCacheReserve,
        loadBalancing: filteredLoadBalancing,
        customHostnames: filteredCustomHostnames,
        logExplorer: filteredLogExplorer,
        zeroTrustSeats: filteredZeroTrustSeats,
        workersPages: filteredWorkersPages,
        r2Storage: filteredR2Storage,
        d1: filteredD1,
        kv: filteredKV,
        stream: filteredStream,
        images: filteredImages,
        workersAI: filteredWorkersAI,
        queues: filteredQueues,
        workersLogsTraces: filteredWorkersLogsTraces,
        durableObjects: filteredDO,
        magicTransit: filteredMagicTransit,
        magicWan: filteredMagicWan,
        spectrum: filteredSpectrum,
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
  if (isInitialSetup) {
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
              <div className="bg-gray-100 rounded-lg p-4 text-left">
                <div className="flex items-center space-x-3">
                  <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                  <span className="text-sm text-gray-700">Fetching your usage data from Cloudflare</span>
                </div>
              </div>
              
              <p className="text-xs text-gray-500 mt-4">Hold tight — this can take anywhere from 30 seconds to a few minutes depending on how many products need to be fetched</p>
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
    <div className="space-y-4 relative">

      {/* Compact Toolbar */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Usage Overview</h2>
          <div className="flex items-center gap-3 mt-1">
            <p className="text-sm text-gray-600">
              Monitor your Cloudflare Enterprise consumption
            </p>
            {cacheAge !== null && (
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                {cacheAge < 60 
                  ? `${cacheAge}s ago` 
                  : cacheAge < 3600 
                    ? `${Math.floor(cacheAge / 60)}m ago`
                    : `${Math.floor(cacheAge / 3600)}h ago`
                }
              </span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-3">
          {showAccountFilter && (
            <select
              value={selectedAccount}
              onChange={(e) => setSelectedAccount(e.target.value)}
              className="px-3 py-2 text-sm border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-white"
            >
              <option value="all">All Accounts</option>
              {accountsWithNames.map((account) => (
                <option key={account.id} value={account.id}>
                  {account.name}
                </option>
              ))}
            </select>
          )}
          <div className="relative">
            <button
              onClick={() => setShowAlertPopover(!showAlertPopover)}
              className={`p-2 rounded-lg border transition-colors ${
                alertsEnabled 
                  ? 'border-blue-200 bg-blue-50 text-blue-600 hover:bg-blue-100' 
                  : 'border-gray-300 bg-white text-gray-400 hover:bg-gray-50'
              }`}
              title="Threshold Alerts"
            >
              {alertsEnabled ? <Bell className="w-4 h-4" /> : <BellOff className="w-4 h-4" />}
            </button>
            {showAlertPopover && (
              <>
                <div className="fixed inset-0 z-10" onClick={() => setShowAlertPopover(false)} />
                <div className="absolute right-0 top-full mt-2 w-72 bg-white rounded-lg shadow-lg border border-gray-200 p-4 z-20">
                  <div className="flex items-center justify-between mb-3">
                    <h4 className="text-sm font-semibold text-gray-900">Threshold Alerts</h4>
                    <button
                      onClick={toggleAlerts}
                      className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                        alertsEnabled ? 'bg-blue-600' : 'bg-gray-300'
                      }`}
                    >
                      <span className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white transition-transform ${
                        alertsEnabled ? 'translate-x-[18px]' : 'translate-x-[3px]'
                      }`} />
                    </button>
                  </div>
                  <p className="text-xs text-gray-500 mb-3">Notify when usage reaches 90% of contracted limits ({config.alertFrequency === 'weekly' ? 'weekly' : 'monthly'})</p>
                  {alertsEnabled && metrics && zones && (
                    <div className="space-y-2">
                      <button
                        onClick={() => { sendSlackMessage(metrics, zones, 'alert'); setShowAlertPopover(false); }}
                        className="w-full px-3 py-1.5 text-xs font-medium bg-amber-100 text-amber-700 rounded-md hover:bg-amber-200 transition-colors"
                      >
                        Check Alerts Now
                      </button>
                      <button
                        onClick={() => { sendSlackMessage(metrics, zones, 'report'); setShowAlertPopover(false); }}
                        className="w-full px-3 py-1.5 text-xs font-medium bg-blue-100 text-blue-700 rounded-md hover:bg-blue-200 transition-colors"
                      >
                        Send Full Report
                      </button>
                    </div>
                  )}
                  {lastChecked && (
                    <p className="text-[10px] text-gray-400 mt-2">Last checked: {lastChecked.toLocaleTimeString()}</p>
                  )}
                </div>
              </>
            )}
          </div>
          <div className="flex flex-col items-end flex-shrink-0">
            <button
              onClick={prewarmCache}
              disabled={loading || prewarming}
              className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-sm text-sm font-medium whitespace-nowrap"
              title="Fetch fresh data and cache for instant future loads"
            >
              <RefreshCw className={`w-4 h-4 ${prewarming ? 'animate-spin' : ''}`} />
              <span>{prewarming ? 'Refreshing...' : 'Refresh'}</span>
            </button>
            {prewarming && (
              <p className="text-[11px] text-gray-400 mt-1.5 max-w-[200px] text-right leading-tight">Hold tight — this typically takes 30s to a couple of minutes.</p>
            )}
          </div>
        </div>
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
          {activeServiceTab === SERVICE_CATEGORIES.NETWORK_SERVICES && renderNetworkServices()}
          {activeServiceTab === SERVICE_CATEGORIES.CLOUDFLARE_ONE && renderCloudflareOne()}
          {activeServiceTab === SERVICE_CATEGORIES.DEVELOPER_PLATFORM && renderDeveloperPlatform()}
        </div>
      </div>
    </div>
  );

  function renderSidebarLayout(sidebarItems, renderContent) {
    const firstClickable = sidebarItems.find(item => item.type !== 'header');
    const activeProduct = selectedProduct || (firstClickable ? firstClickable.id : null);

    return (
      <div className="flex min-h-[500px]">
        <div className="w-64 border-r border-gray-200 bg-white flex-shrink-0 pt-6">
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

    const coreEnabled = config?.applicationServices?.core?.enabled !== false;
    const trafficEnabled = config?.applicationServices?.core?.trafficEnabled !== undefined ? config.applicationServices.core.trafficEnabled : coreEnabled;
    const dnsEnabled = config?.applicationServices?.core?.dnsEnabled !== undefined ? config.applicationServices.core.dnsEnabled : coreEnabled;
    const coreItems = [];
    if (coreEnabled && metrics?.current) coreItems.push({ id: 'enterpriseZones', label: 'Enterprise Zones' });
    if (trafficEnabled && metrics?.current) coreItems.push({ id: 'appServices', label: 'Traffic' });
    if (dnsEnabled && metrics?.current) coreItems.push({ id: 'dns', label: 'DNS' });
    if (coreItems.length > 0) sidebarItems.push({ type: 'header', label: 'Zones & Traffic' }, ...coreItems);

    const securityItems = [];
    if (displayMetrics?.botManagement?.enabled) securityItems.push({ id: 'botManagement', label: 'Bot Management' });
    if (displayMetrics?.apiShield?.enabled) securityItems.push({ id: 'apiShield', label: 'API Shield' });
    if (displayMetrics?.pageShield?.enabled) securityItems.push({ id: 'pageShield', label: 'Page Shield' });
    if (displayMetrics?.advancedRateLimiting?.enabled) securityItems.push({ id: 'advancedRateLimiting', label: 'Advanced Rate Limiting' });
    if (securityItems.length > 0) sidebarItems.push({ type: 'header', label: 'Application Security' }, ...securityItems);

    const deliveryItems = [];
    if (displayMetrics?.argo?.enabled) deliveryItems.push({ id: 'argo', label: 'Argo Smart Routing' });
    if (displayMetrics?.cacheReserve?.enabled) deliveryItems.push({ id: 'cacheReserve', label: 'Cache Reserve' });
    if (displayMetrics?.loadBalancing?.enabled) deliveryItems.push({ id: 'loadBalancing', label: 'Load Balancing' });
    if (displayMetrics?.customHostnames?.enabled) deliveryItems.push({ id: 'customHostnames', label: 'Custom Hostnames' });
    if (deliveryItems.length > 0) sidebarItems.push({ type: 'header', label: 'Delivery & Performance' }, ...deliveryItems);

    const logsItems = [];
    if (displayMetrics?.logExplorer?.enabled) logsItems.push({ id: 'logExplorer', label: 'Log Explorer' });
    if (logsItems.length > 0) sidebarItems.push({ type: 'header', label: 'Logs & Analytics' }, ...logsItems);

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
        case 'botManagement':
          return renderBotManagement();
        case 'apiShield':
          return renderAddonProduct('apiShield', 'API Shield', 'HTTP Requests', 'requests', 'requests', '#8b5cf6');
        case 'pageShield':
          return renderAddonProduct('pageShield', 'Page Shield', 'HTTP Requests', 'requests', 'requests', '#ec4899');
        case 'advancedRateLimiting':
          return renderAddonProduct('advancedRateLimiting', 'Advanced Rate Limiting', 'HTTP Requests', 'requests', 'requests', '#14b8a6');
        case 'argo':
          return renderArgo();
        case 'cacheReserve':
          return renderCacheReserve();
        case 'loadBalancing':
          return renderLoadBalancing();
        case 'customHostnames':
          return renderCustomHostnames();
        case 'logExplorer':
          return renderLogExplorer();
        default:
          return null;
      }
    });
  }

  function renderNetworkServices() {
    const sidebarItems = [];

    if (displayMetrics?.magicTransit?.enabled) {
      sidebarItems.push({ id: 'magicTransit', label: 'Magic Transit' });
    }
    if (displayMetrics?.spectrum?.enabled) {
      sidebarItems.push({ id: 'spectrum', label: 'Spectrum' });
    }

    if (sidebarItems.length === 0) {
      return (
        <div className="text-center py-20">
          <AlertCircle className="w-16 h-16 mx-auto text-gray-400 mb-4" />
          <h3 className="text-xl font-semibold text-gray-700 mb-2">Network Services</h3>
          <p className="text-sm text-gray-500">No Network Services configured. Go to Settings to enable them.</p>
        </div>
      );
    }

    return renderSidebarLayout(sidebarItems, (activeProduct) => {
      switch (activeProduct) {
        case 'magicTransit':
          return renderMagicTransit();
        case 'spectrum':
          return renderSpectrum();
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
        {renderZoneBreakdown('dns')}
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
              title={
                <span className="inline-flex items-center gap-1.5">
                  Magic Transit (Ingress)
                  <span className="group relative">
                    <Info className="w-3.5 h-3.5 text-gray-400 cursor-help" />
                    <span className="invisible group-hover:visible absolute top-full left-0 mt-2 w-64 p-2.5 bg-gray-900 text-white text-xs rounded-lg shadow-lg z-50 leading-relaxed">
                      Bandwidth data is cached for up to 6 hours to optimize refresh performance.
                    </span>
                  </span>
                </span>
              }
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
              title={
                <span className="inline-flex items-center gap-1.5">
                  Magic Transit (Egress)
                  <span className="group relative">
                    <Info className="w-3.5 h-3.5 text-gray-400 cursor-help" />
                    <span className="invisible group-hover:visible absolute top-full left-0 mt-2 w-64 p-2.5 bg-gray-900 text-white text-xs rounded-lg shadow-lg z-50 leading-relaxed">
                      Bandwidth data is cached for up to 6 hours to optimize refresh performance.
                    </span>
                  </span>
                </span>
              }
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
            title={
              <span className="inline-flex items-center gap-1.5">
                Magic Transit
                <span className="group relative">
                  <Info className="w-3.5 h-3.5 text-gray-400 cursor-help" />
                  <span className="invisible group-hover:visible absolute top-full left-0 mt-2 w-64 p-2.5 bg-gray-900 text-white text-xs rounded-lg shadow-lg z-50 leading-relaxed">
                    Bandwidth data is cached for up to 6 hours to optimize refresh performance.
                  </span>
                </span>
              </span>
            }
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

  function renderSpectrum() {
    const spec = displayMetrics?.spectrum;
    if (!spec?.enabled) return null;

    const formatDataTransfer = (bytes) => {
      if (bytes >= 1099511627776) return `${(bytes / 1099511627776).toFixed(2)} TB`;
      if (bytes >= 1073741824) return `${(bytes / 1073741824).toFixed(2)} GB`;
      if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(2)} MB`;
      if (bytes >= 1024) return `${(bytes / 1024).toFixed(2)} KB`;
      return `${bytes} B`;
    };

    const formatConnections = (val) => {
      if (val >= 1000000) return `${(val / 1000000).toFixed(2)}M`;
      if (val >= 1000) return `${(val / 1000).toFixed(2)}K`;
      return Math.round(val).toLocaleString();
    };

    const dataTransferThresholdBytes = spec.dataTransferThreshold ? spec.dataTransferThreshold * 1e12 : null;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title={
            <span className="inline-flex items-center gap-1.5">
              Data Transfer
              <span className="group relative">
                <Info className="w-3.5 h-3.5 text-gray-400 cursor-help" />
                <span className="invisible group-hover:visible absolute top-full left-0 mt-2 w-72 p-2.5 bg-gray-900 text-white text-xs rounded-lg shadow-lg z-50 leading-relaxed">
                  Data transfer is cached for up to 6 hours to optimize refresh performance. Includes all Spectrum ingress + egress bytes. HTTP traffic on Spectrum hostnames is processed by Cloudflare's CDN unless explicitly blocked, so it may be counted both here and in your HTTP Data Transfer.
                </span>
              </span>
            </span>
          }
          subtitle="Spectrum ingress + egress bytes"
          value={spec.current?.dataTransfer || 0}
          formatted={formatDataTransfer(spec.current?.dataTransfer || 0)}
          threshold={dataTransferThresholdBytes}
          percentage={calculatePercentage(spec.current?.dataTransfer || 0, dataTransferThresholdBytes)}
          icon="bandwidth"
          unit=""
          color="#8b5cf6"
          timeSeries={spec.timeSeries}
          dataKey="dataTransfer"
          chartFormatter={formatDataTransfer}
          yAxisLabel="Transfer"
        />
        <div className="relative">
          <ConsolidatedCard
            title={
              <span className="inline-flex items-center gap-1.5">
                Concurrent Connections (P95)
                <span className="group relative">
                  <Info className="w-3.5 h-3.5 text-gray-400 cursor-help" />
                  <span className="invisible group-hover:visible absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-72 p-2.5 bg-gray-900 text-white text-xs rounded-lg shadow-lg z-50 leading-relaxed">
                    P95 is calculated from per-minute samples, zero-filled from the start of the month. For accurate results, ensure Spectrum is configured from the 1st of the month.
                  </span>
                </span>
              </span>
            }
            subtitle="P95 of per-minute concurrent client samples"
            value={spec.current?.p95Concurrent || 0}
            formatted={formatConnections(spec.current?.p95Concurrent || 0)}
            threshold={spec.connectionsThreshold}
            percentage={calculatePercentage(spec.current?.p95Concurrent || 0, spec.connectionsThreshold)}
            icon="connections"
            unit=""
            color="#06b6d4"
            timeSeries={spec.timeSeries}
            dataKey="p95Concurrent"
            chartFormatter={formatConnections}
            yAxisLabel="P95 Concurrent"
          />
        </div>
      </div>
    );
  }

  function renderCacheReserve() {
    const cr = displayMetrics?.cacheReserve;
    if (!cr?.enabled) return null;

    const formatStorageTB = (bytes) => {
      if (bytes >= 1e12) return `${(bytes / 1e12).toFixed(2)} TB`;
      if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(2)} GB`;
      if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(2)} MB`;
      return `${(bytes / 1e3).toFixed(2)} KB`;
    };

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Storage"
          subtitle="Aggregate storage usage"
          value={cr.current?.storageGBDays || 0}
          formatted={(() => {
            const gb = cr.current?.storageGBDays || 0;
            if (gb >= 1000) return `${(gb / 1000).toFixed(2)} TB`;
            if (gb >= 1) return `${gb.toFixed(2)} GB`;
            return `${(gb * 1000).toFixed(2)} MB`;
          })()}
          threshold={cr.storageThreshold || null}
          percentage={cr.storageThreshold ? (cr.current?.storageGBDays || 0) / (cr.storageThreshold * 1000) * 100 : null}
          icon="cr-storage"
          unit=""
          color="#10b981"
          timeSeries={cr.timeSeries}
          dataKey="storageGBDays"
          chartFormatter={(v) => v >= 1 ? `${v.toFixed(1)} GB` : `${(v * 1000).toFixed(0)} MB`}
          yAxisLabel="Storage"
          isZoneFiltered={true}
        />
        <ConsolidatedCard
          title="Class A Operations"
          subtitle="Write operations"
          value={cr.current?.classAOps || 0}
          formatted={formatNumber(cr.current?.classAOps || 0)}
          threshold={cr.classAOpsThreshold || null}
          percentage={calculatePercentage(cr.current?.classAOps || 0, cr.classAOpsThreshold || null)}
          icon="upload"
          unit=""
          color="#3b82f6"
          timeSeries={cr.timeSeries}
          dataKey="classAOps"
          chartFormatter={formatNumber}
          yAxisLabel="Operations"
          isZoneFiltered={true}
        />
        <ConsolidatedCard
          title="Class B Operations"
          subtitle="Read operations"
          value={cr.current?.classBOps || 0}
          formatted={formatNumber(cr.current?.classBOps || 0)}
          threshold={cr.classBOpsThreshold || null}
          percentage={calculatePercentage(cr.current?.classBOps || 0, cr.classBOpsThreshold || null)}
          icon="download"
          unit=""
          color="#6366f1"
          timeSeries={cr.timeSeries}
          dataKey="classBOps"
          chartFormatter={formatNumber}
          yAxisLabel="Operations"
          isZoneFiltered={true}
        />
        {renderCacheReserveZoneBreakdown()}
      </div>
    );
  }

  function renderLoadBalancing() {
    const lb = displayMetrics?.loadBalancing;
    if (!lb?.enabled) return null;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Endpoints"
          subtitle="Load balancer origins"
          value={lb.current?.endpoints || 0}
          formatted={(lb.current?.endpoints || 0).toLocaleString()}
          threshold={lb.threshold || null}
          percentage={calculatePercentage(lb.current?.endpoints || 0, lb.threshold || null)}
          icon="endpoints"
          unit=""
          color="#f59e0b"
          timeSeries={lb.timeSeries}
          dataKey="endpoints"
          chartFormatter={(v) => v.toLocaleString()}
          yAxisLabel="Endpoints"
        />
      </div>
    );
  }

  function renderLogExplorer() {
    const le = displayMetrics?.logExplorer;
    if (!le?.enabled) return null;

    const formatGB = (gb) => {
      if (gb >= 1000) return `${(gb / 1000).toFixed(2)} TB`;
      if (gb >= 1) return `${gb.toFixed(2)} GB`;
      if (gb >= 0.001) return `${(gb * 1000).toFixed(2)} MB`;
      return '0 GB';
    };

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Data Retention"
          subtitle="Billable data ingested this month"
          value={le.current?.billableGB || 0}
          formatted={formatGB(le.current?.billableGB || 0)}
          threshold={le.threshold || null}
          percentage={calculatePercentage(le.current?.billableGB || 0, le.threshold || null)}
          icon="database"
          unit="GB"
          color="#6366f1"
          timeSeries={le.timeSeries}
          dataKey="billableGB"
          chartFormatter={formatGB}
          yAxisLabel="GB"
        />
      </div>
    );
  }

  function renderCustomHostnames() {
    const ch = displayMetrics?.customHostnames;
    if (!ch?.enabled) return null;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Custom Hostnames"
          subtitle="Active custom hostnames"
          value={ch.current?.hostnames || 0}
          formatted={(ch.current?.hostnames || 0).toLocaleString()}
          threshold={ch.threshold || null}
          percentage={calculatePercentage(ch.current?.hostnames || 0, ch.threshold || null)}
          icon="hostnames"
          unit=""
          color="#6366f1"
          timeSeries={ch.timeSeries}
          dataKey="hostnames"
          chartFormatter={(v) => v.toLocaleString()}
          yAxisLabel="Hostnames"
        />
      </div>
    );
  }

  function renderCacheReserveZoneBreakdown() {
    const cr = displayMetrics?.cacheReserve;
    const zoneData = zonesViewMode === 'current' ? cr?.current?.zones : cr?.previous?.zones;
    if (!zoneData || zoneData.length === 0) return null;

    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">Cache Reserve - Breakdown by Zone</h3>
            <p className="text-sm text-gray-500 mt-1">Storage and operations per zone</p>
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
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Storage</th>
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Class A Ops</th>
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Class B Ops</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {zoneData.map((zone, index) => (
                    <tr key={zone.zoneId || index} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-sm font-medium text-gray-900">{zone.zoneName || zone.zoneId || 'Unknown Zone'}</div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <span className="text-sm font-semibold text-gray-900">{(() => { const gb = zone.storageGBDays || 0; if (gb >= 1000) return `${(gb/1000).toFixed(2)} TB`; if (gb >= 1) return `${gb.toFixed(2)} GB`; return `${(gb*1000).toFixed(2)} MB`; })()}</span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <span className="text-sm font-semibold text-gray-900">{formatNumber(zone.classAOps || 0)}</span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <span className="text-sm font-semibold text-gray-900">{formatNumber(zone.classBOps || 0)}</span>
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

  function renderArgo() {
    const argo = displayMetrics?.argo;
    if (!argo?.enabled) return null;

    const currentBytes = argo.current?.bytes || 0;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Argo Smart Routing"
          subtitle="Data Transfer (ingress + egress)"
          value={currentBytes}
          formatted={formatBytes(currentBytes)}
          threshold={argo.threshold}
          percentage={calculatePercentage(currentBytes, argo.threshold)}
          icon="bandwidth"
          unit=""
          color="#f97316"
          timeSeries={argo.timeSeries}
          dataKey="bytes"
          chartFormatter={formatBytes}
          yAxisLabel="Transfer"
          confidence={argo.current?.confidence}
          confidenceMetricType="Data Transfer"
          isZoneFiltered={true}
        />
        {renderArgoZoneBreakdown()}
      </div>
    );
  }

  function renderArgoZoneBreakdown() {
    const argo = displayMetrics?.argo;
    const zoneData = zonesViewMode === 'current' ? argo?.current?.zones : argo?.previous?.zones;
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
            <h3 className="text-lg font-semibold text-gray-900">Argo - Breakdown by Zone</h3>
            <p className="text-sm text-gray-500 mt-1">Data transfer per zone</p>
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
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Data Transfer</th>
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
                          {formatBytes(zone.bytes || 0)}
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

  function renderBotManagement() {
    const product = displayMetrics?.botManagement;
    if (!product?.enabled) return null;

    const currentVal = product.current?.likelyHuman || 0;
    const totalTraffic = product.current?.totalTraffic || 0;
    const botTraffic = product.current?.botTraffic || 0;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Bot Management"
          subtitle="Billable Human Requests (Bot Score ≥ 30)"
          value={currentVal}
          formatted={formatRequests(currentVal)}
          threshold={product.threshold}
          percentage={calculatePercentage(currentVal, product.threshold)}
          icon="traffic"
          unit="M"
          color="#f59e0b"
          timeSeries={product.timeSeries}
          dataKey="likelyHuman"
          chartFormatter={formatRequests}
          yAxisLabel="Likely Human Requests"
          confidence={product.current?.confidence}
          confidenceMetricType="Likely Human Requests"
          isZoneFiltered={true}
          summaryBadge={totalTraffic > 0 ? (
            <div className="bg-gray-50 border border-gray-200 rounded-lg px-3 py-2 text-xs">
              <div className="flex items-center justify-between space-x-2 mb-1">
                <span className="text-gray-600">Total Traffic:</span>
                <span className="font-semibold text-gray-900">{formatRequests(totalTraffic)}</span>
              </div>
              <div className="flex items-center justify-between space-x-2">
                <span className="text-gray-600">Bot Traffic:</span>
                <span className="font-semibold text-red-600">{formatRequests(botTraffic)}</span>
              </div>
            </div>
          ) : null}
        />
        {renderBotManagementZoneBreakdown()}
      </div>
    );
  }

  function renderBotManagementZoneBreakdown() {
    const product = displayMetrics?.botManagement;
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
            <h3 className="text-lg font-semibold text-gray-900">Bot Management - Breakdown by Zone</h3>
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
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Likely Human</th>
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Bot Traffic</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {deduplicatedZones.map((zone, index) => {
                    const botTraffic = (zone.automated || 0) + (zone.likelyAutomated || 0);
                    return (
                      <tr key={zone.zoneId || index} className="hover:bg-gray-50">
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm font-medium text-gray-900">{zone.zoneName || zone.zoneId || 'Unknown Zone'}</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <span className="text-sm font-semibold text-gray-900">{formatRequests(zone.likelyHuman || 0)}</span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <span className="text-sm font-semibold text-red-600">{formatRequests(botTraffic)}</span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
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
                <div className="absolute bottom-full left-0 mb-2 w-64 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50 pointer-events-none">
                  Primary/secondary classifications are based on previous month's usage (zones with ≥50GB are Primary).
                  <div className="absolute top-full left-4 border-4 border-transparent border-t-gray-900"></div>
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
            visibleColumns={type === 'dns' ? ['dns'] : ['type', 'bandwidth', 'requests', 'dns']}
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
      sidebarItems.push({ id: 'zeroTrustSeats', label: 'Zero Trust' });
    }
    if (displayMetrics?.magicWan?.enabled) {
      sidebarItems.push({ id: 'wan', label: 'WAN' });
    }

    if (sidebarItems.length === 0) {
      return (
        <div className="text-center py-20">
          <AlertCircle className="w-16 h-16 mx-auto text-gray-400 mb-4" />
          <h3 className="text-xl font-semibold text-gray-700 mb-2">Cloudflare One</h3>
          <p className="text-sm text-gray-500">No Cloudflare One services configured. Go to Settings to enable Zero Trust or WAN.</p>
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
          title={
            <span className="inline-flex items-center gap-1.5">
              WAN
              <span className="group relative">
                <Info className="w-3.5 h-3.5 text-gray-400 cursor-help" />
                <span className="invisible group-hover:visible absolute top-full left-0 mt-2 w-64 p-2.5 bg-gray-900 text-white text-xs rounded-lg shadow-lg z-50 leading-relaxed">
                  Bandwidth data is cached for up to 6 hours to optimize refresh performance.
                </span>
              </span>
            </span>
          }
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

    const computeItems = [];
    if (displayMetrics?.workersPages?.enabled) computeItems.push({ id: 'workersPages', label: 'Workers & Pages' });
    if (displayMetrics?.queues?.enabled) computeItems.push({ id: 'queues', label: 'Queues' });
    if (displayMetrics?.durableObjects?.enabled) computeItems.push({ id: 'durableObjects', label: 'Durable Objects' });
    if (displayMetrics?.workersLogsTraces?.enabled) computeItems.push({ id: 'workersLogsTraces', label: 'Workers Observability' });
    if (computeItems.length > 0) sidebarItems.push({ type: 'header', label: 'Compute' }, ...computeItems);

    const aiItems = [];
    if (displayMetrics?.workersAI?.enabled) aiItems.push({ id: 'workersAI', label: 'Workers AI' });
    if (aiItems.length > 0) sidebarItems.push({ type: 'header', label: 'AI' }, ...aiItems);

    const storageItems = [];
    if (displayMetrics?.r2Storage?.enabled) storageItems.push({ id: 'r2Storage', label: 'R2 Storage' });
    if (displayMetrics?.d1?.enabled) storageItems.push({ id: 'd1', label: 'D1 Database' });
    if (displayMetrics?.kv?.enabled) storageItems.push({ id: 'kv', label: 'Workers KV' });
    if (storageItems.length > 0) sidebarItems.push({ type: 'header', label: 'Storage & Databases' }, ...storageItems);

    const mediaItems = [];
    if (displayMetrics?.stream?.enabled) mediaItems.push({ id: 'stream', label: 'Stream' });
    if (displayMetrics?.images?.enabled) mediaItems.push({ id: 'images', label: 'Images' });
    if (mediaItems.length > 0) sidebarItems.push({ type: 'header', label: 'Media' }, ...mediaItems);

    if (sidebarItems.length === 0) {
      return (
        <div className="text-center py-20">
          <AlertCircle className="w-16 h-16 mx-auto text-gray-400 mb-4" />
          <h3 className="text-xl font-semibold text-gray-700 mb-2">Developer Platform</h3>
          <p className="text-sm text-gray-500">No Developer Platform services configured. Go to Settings to enable them.</p>
        </div>
      );
    }

    return renderSidebarLayout(sidebarItems, (activeProduct) => {
      switch (activeProduct) {
        case 'workersPages':
          return renderWorkersPages();
        case 'r2Storage':
          return renderR2Storage();
        case 'd1':
          return renderD1();
        case 'kv':
          return renderKV();
        case 'stream':
          return renderStream();
        case 'images':
          return renderImages();
        case 'workersAI':
          return renderWorkersAI();
        case 'queues':
          return renderQueues();
        case 'workersLogsTraces':
          return renderWorkersLogsTraces();
        case 'durableObjects':
          return renderDurableObjects();
        default:
          return null;
      }
    });
  }

  function renderWorkersPages() {
    const wp = displayMetrics?.workersPages;
    if (!wp?.enabled) return null;

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
          formatted={formatNumber(wp.current?.requests || 0)}
          threshold={wp.requestsThreshold ? wp.requestsThreshold * 1000000 : null}
          percentage={calculatePercentage(wp.current?.requests || 0, wp.requestsThreshold ? wp.requestsThreshold * 1000000 : null)}
          icon="activity"
          unit=""
          color="#3b82f6"
          timeSeries={wp.timeSeries}
          dataKey="requests"
          chartFormatter={formatNumber}
          yAxisLabel="Requests"
          confidence={wp.current?.confidence}
          confidenceMetricType="Worker Invocations"
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

    const formatStorageGB = (gb) => {
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
          formatted={formatNumber(r2.current?.classAOps || 0)}
          threshold={r2.classAOpsThreshold ? r2.classAOpsThreshold * 1000000 : null}
          percentage={calculatePercentage(r2.current?.classAOps || 0, r2.classAOpsThreshold ? r2.classAOpsThreshold * 1000000 : null)}
          icon="upload"
          unit=""
          color="#3b82f6"
          timeSeries={r2.timeSeries}
          dataKey="classAOps"
          chartFormatter={formatNumber}
          yAxisLabel="Operations"
        />
        <ConsolidatedCard
          title="Class B Operations"
          subtitle="Read"
          value={r2.current?.classBOps || 0}
          formatted={formatNumber(r2.current?.classBOps || 0)}
          threshold={r2.classBOpsThreshold ? r2.classBOpsThreshold * 1000000 : null}
          percentage={calculatePercentage(r2.current?.classBOps || 0, r2.classBOpsThreshold ? r2.classBOpsThreshold * 1000000 : null)}
          icon="download"
          unit=""
          color="#6366f1"
          timeSeries={r2.timeSeries}
          dataKey="classBOps"
          chartFormatter={formatNumber}
          yAxisLabel="Operations"
        />
        <ConsolidatedCard
          title="Total Storage"
          subtitle="Capacity used"
          value={r2.current?.storageGB || 0}
          formatted={formatStorageGB(r2.current?.storageGB || 0)}
          threshold={r2.storageThreshold || null}
          percentage={calculatePercentage(r2.current?.storageGB || 0, r2.storageThreshold || null)}
          icon="database"
          unit=""
          color="#10b981"
          timeSeries={r2.timeSeries}
          dataKey="storageGB"
          chartFormatter={formatStorageGB}
          yAxisLabel="Storage (GB)"
        />
      </div>
    );
  }

  function renderD1() {
    const d1 = displayMetrics?.d1;
    if (!d1?.enabled) return null;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Rows Read"
          subtitle="Database queries"
          value={d1.current?.rowsRead || 0}
          formatted={formatNumber(d1.current?.rowsRead || 0)}
          threshold={d1.rowsReadThreshold ? d1.rowsReadThreshold * 1000000 : null}
          percentage={calculatePercentage(d1.current?.rowsRead || 0, d1.rowsReadThreshold ? d1.rowsReadThreshold * 1000000 : null)}
          icon="table"
          unit=""
          color="#3b82f6"
          timeSeries={d1.timeSeries}
          dataKey="rowsRead"
          chartFormatter={formatNumber}
          yAxisLabel="Rows"
        />
        <ConsolidatedCard
          title="Rows Written"
          subtitle="Database mutations"
          value={d1.current?.rowsWritten || 0}
          formatted={formatNumber(d1.current?.rowsWritten || 0)}
          threshold={d1.rowsWrittenThreshold ? d1.rowsWrittenThreshold * 1000000 : null}
          percentage={calculatePercentage(d1.current?.rowsWritten || 0, d1.rowsWrittenThreshold ? d1.rowsWrittenThreshold * 1000000 : null)}
          icon="table"
          unit=""
          color="#6366f1"
          timeSeries={d1.timeSeries}
          dataKey="rowsWritten"
          chartFormatter={formatNumber}
          yAxisLabel="Rows"
        />
        <ConsolidatedCard
          title="Total Storage"
          subtitle="Database size"
          value={d1.current?.storageMB || 0}
          formatted={formatStorageMB(d1.current?.storageMB || 0)}
          threshold={d1.storageThreshold ? d1.storageThreshold * 1000 : null}
          percentage={calculatePercentage(d1.current?.storageMB || 0, d1.storageThreshold ? d1.storageThreshold * 1000 : null)}
          icon="storage-mb"
          unit=""
          color="#10b981"
          timeSeries={d1.timeSeries}
          dataKey="storageMB"
          chartFormatter={formatStorageMB}
          yAxisLabel="Storage (MB)"
        />
      </div>
    );
  }

  function renderKV() {
    const kv = displayMetrics?.kv;
    if (!kv?.enabled) return null;

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Keys Read"
          subtitle="Read operations"
          value={kv.current?.reads || 0}
          formatted={formatNumber(kv.current?.reads || 0)}
          threshold={kv.readsThreshold ? kv.readsThreshold * 1000000 : null}
          percentage={calculatePercentage(kv.current?.reads || 0, kv.readsThreshold ? kv.readsThreshold * 1000000 : null)}
          icon="table"
          unit=""
          color="#3b82f6"
          timeSeries={kv.timeSeries}
          dataKey="reads"
          chartFormatter={formatNumber}
          yAxisLabel="Operations"
        />
        <ConsolidatedCard
          title="Keys Written"
          subtitle="Write operations"
          value={kv.current?.writes || 0}
          formatted={formatNumber(kv.current?.writes || 0)}
          threshold={kv.writesThreshold ? kv.writesThreshold * 1000000 : null}
          percentage={calculatePercentage(kv.current?.writes || 0, kv.writesThreshold ? kv.writesThreshold * 1000000 : null)}
          icon="table"
          unit=""
          color="#6366f1"
          timeSeries={kv.timeSeries}
          dataKey="writes"
          chartFormatter={formatNumber}
          yAxisLabel="Operations"
        />
        <ConsolidatedCard
          title="Keys Deleted"
          subtitle="Delete operations"
          value={kv.current?.deletes || 0}
          formatted={formatNumber(kv.current?.deletes || 0)}
          threshold={kv.deletesThreshold ? kv.deletesThreshold * 1000000 : null}
          percentage={calculatePercentage(kv.current?.deletes || 0, kv.deletesThreshold ? kv.deletesThreshold * 1000000 : null)}
          icon="table"
          unit=""
          color="#f59e0b"
          timeSeries={kv.timeSeries}
          dataKey="deletes"
          chartFormatter={formatNumber}
          yAxisLabel="Operations"
        />
        <ConsolidatedCard
          title="List Requests"
          subtitle="List operations"
          value={kv.current?.lists || 0}
          formatted={formatNumber(kv.current?.lists || 0)}
          threshold={kv.listsThreshold ? kv.listsThreshold * 1000000 : null}
          percentage={calculatePercentage(kv.current?.lists || 0, kv.listsThreshold ? kv.listsThreshold * 1000000 : null)}
          icon="table"
          unit=""
          color="#8b5cf6"
          timeSeries={kv.timeSeries}
          dataKey="lists"
          chartFormatter={formatNumber}
          yAxisLabel="Operations"
        />
        <ConsolidatedCard
          title="Stored Data"
          subtitle="Total storage"
          value={kv.current?.storageMB || 0}
          formatted={formatStorageMB(kv.current?.storageMB || 0)}
          threshold={kv.storageThreshold ? kv.storageThreshold * 1000 : null}
          percentage={calculatePercentage(kv.current?.storageMB || 0, kv.storageThreshold ? kv.storageThreshold * 1000 : null)}
          icon="storage-mb"
          unit=""
          color="#10b981"
          timeSeries={kv.timeSeries}
          dataKey="storageMB"
          chartFormatter={formatStorageMB}
          yAxisLabel="Storage (MB)"
        />
      </div>
    );
  }

  function renderStream() {
    const streamData = displayMetrics?.stream;
    if (!streamData?.enabled) return null;

    const formatMinutes = (val) => {
      if (val >= 1000000) return `${(val / 1000000).toFixed(2)}M min`;
      if (val >= 1000) return `${(val / 1000).toFixed(2)}K min`;
      return `${Math.round(val).toLocaleString()} min`;
    };

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Minutes Stored"
          subtitle="Total video duration stored"
          value={streamData.current?.minutesStored || 0}
          formatted={formatMinutes(streamData.current?.minutesStored || 0)}
          threshold={streamData.minutesStoredThreshold ? streamData.minutesStoredThreshold * 1000 : null}
          percentage={calculatePercentage(streamData.current?.minutesStored || 0, streamData.minutesStoredThreshold ? streamData.minutesStoredThreshold * 1000 : null)}
          icon="minutes"
          unit=""
          color="#f59e0b"
          timeSeries={streamData.timeSeries}
          dataKey="minutesStored"
          chartFormatter={formatMinutes}
          yAxisLabel="Minutes"
        />
        <ConsolidatedCard
          title="Minutes Delivered"
          subtitle="Video delivered to viewers"
          value={streamData.current?.minutesDelivered || 0}
          formatted={formatMinutes(streamData.current?.minutesDelivered || 0)}
          threshold={streamData.minutesDeliveredThreshold ? streamData.minutesDeliveredThreshold * 1000 : null}
          percentage={calculatePercentage(streamData.current?.minutesDelivered || 0, streamData.minutesDeliveredThreshold ? streamData.minutesDeliveredThreshold * 1000 : null)}
          icon="minutes"
          unit=""
          color="#3b82f6"
          timeSeries={streamData.timeSeries}
          dataKey="minutesDelivered"
          chartFormatter={formatMinutes}
          yAxisLabel="Minutes"
        />
      </div>
    );
  }

  function renderImages() {
    const imgData = displayMetrics?.images;
    if (!imgData?.enabled) return null;

    const formatCount = (val) => {
      if (val >= 1000000) return `${(val / 1000000).toFixed(2)}M`;
      if (val >= 1000) return `${(val / 1000).toFixed(2)}K`;
      return Math.round(val).toLocaleString();
    };

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Images Stored"
          subtitle="Total images in storage"
          value={imgData.current?.imagesStored || 0}
          formatted={formatCount(imgData.current?.imagesStored || 0)}
          threshold={imgData.imagesStoredThreshold ? imgData.imagesStoredThreshold * 1000 : null}
          percentage={calculatePercentage(imgData.current?.imagesStored || 0, imgData.imagesStoredThreshold ? imgData.imagesStoredThreshold * 1000 : null)}
          icon="images"
          unit=""
          color="#f59e0b"
          timeSeries={imgData.timeSeries}
          dataKey="imagesStored"
          chartFormatter={formatCount}
          yAxisLabel="Images"
        />
        <ConsolidatedCard
          title="Images Delivered"
          subtitle="Images served to viewers"
          value={imgData.current?.imagesDelivered || 0}
          formatted={formatCount(imgData.current?.imagesDelivered || 0)}
          threshold={imgData.imagesDeliveredThreshold ? imgData.imagesDeliveredThreshold * 1000 : null}
          percentage={calculatePercentage(imgData.current?.imagesDelivered || 0, imgData.imagesDeliveredThreshold ? imgData.imagesDeliveredThreshold * 1000 : null)}
          icon="images"
          unit=""
          color="#3b82f6"
          timeSeries={imgData.timeSeries}
          dataKey="imagesDelivered"
          chartFormatter={formatCount}
          yAxisLabel="Requests"
        />
      </div>
    );
  }

  function renderWorkersAI() {
    const waiData = displayMetrics?.workersAI;
    if (!waiData?.enabled) return null;

    const formatNeurons = (val) => {
      if (val >= 1000000) return `${(val / 1000000).toFixed(2)}M`;
      if (val >= 1000) return `${(val / 1000).toFixed(2)}K`;
      return val.toFixed(2);
    };

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Neurons"
          subtitle="AI inference usage"
          value={waiData.current?.neurons || 0}
          formatted={formatNeurons(waiData.current?.neurons || 0)}
          threshold={waiData.neuronsThreshold ? waiData.neuronsThreshold * 1000000 : null}
          percentage={calculatePercentage(waiData.current?.neurons || 0, waiData.neuronsThreshold ? waiData.neuronsThreshold * 1000000 : null)}
          icon="neurons"
          unit=""
          color="#8b5cf6"
          timeSeries={waiData.timeSeries}
          dataKey="neurons"
          chartFormatter={formatNeurons}
          yAxisLabel="Neurons"
        />
      </div>
    );
  }

  function renderWorkersLogsTraces() {
    const wltData = displayMetrics?.workersLogsTraces;
    if (!wltData?.enabled) return null;

    const formatEvents = (val) => {
      if (val >= 1000000) return `${(val / 1000000).toFixed(2)}M`;
      if (val >= 1000) return `${(val / 1000).toFixed(2)}K`;
      return Math.round(val).toLocaleString();
    };

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Events"
          subtitle="Workers Logs & Traces observability events"
          value={wltData.current?.events || 0}
          formatted={formatEvents(wltData.current?.events || 0)}
          threshold={wltData.eventsThreshold ? wltData.eventsThreshold * 1000000 : null}
          percentage={calculatePercentage(wltData.current?.events || 0, wltData.eventsThreshold ? wltData.eventsThreshold * 1000000 : null)}
          icon="events"
          unit=""
          color="#f97316"
          timeSeries={wltData.timeSeries}
          dataKey="events"
          chartFormatter={formatEvents}
          yAxisLabel="Events"
        />
      </div>
    );
  }

  function renderQueues() {
    const qData = displayMetrics?.queues;
    if (!qData?.enabled) return null;

    const formatOps = (val) => {
      if (val >= 1000000) return `${(val / 1000000).toFixed(2)}M`;
      if (val >= 1000) return `${(val / 1000).toFixed(2)}K`;
      return Math.round(val).toLocaleString();
    };

    return (
      <div className="space-y-6">
        <ConsolidatedCard
          title="Operations"
          subtitle="Billable message operations"
          value={qData.current?.operations || 0}
          formatted={formatOps(qData.current?.operations || 0)}
          threshold={qData.operationsThreshold ? qData.operationsThreshold * 1000000 : null}
          percentage={calculatePercentage(qData.current?.operations || 0, qData.operationsThreshold ? qData.operationsThreshold * 1000000 : null)}
          icon="operations"
          unit=""
          color="#10b981"
          timeSeries={qData.timeSeries}
          dataKey="operations"
          chartFormatter={formatOps}
          yAxisLabel="Operations"
        />
      </div>
    );
  }

  function renderDurableObjects() {
    const doData = displayMetrics?.durableObjects;
    if (!doData?.enabled) return null;

    const formatGBs = (val) => {
      if (val >= 1000000) return `${(val / 1000000).toFixed(2)}M GB-s`;
      if (val >= 1000) return `${(val / 1000).toFixed(2)}K GB-s`;
      return `${val.toFixed(2)} GB-s`;
    };

    return (
      <div className="space-y-6">
        <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wider">Compute</h4>
        <ConsolidatedCard
          title="Requests"
          subtitle="Invocations"
          value={doData.current?.requests || 0}
          formatted={formatNumber(doData.current?.requests || 0)}
          threshold={doData.requestsThreshold ? doData.requestsThreshold * 1000000 : null}
          percentage={calculatePercentage(doData.current?.requests || 0, doData.requestsThreshold ? doData.requestsThreshold * 1000000 : null)}
          icon="table"
          unit=""
          color="#3b82f6"
          timeSeries={doData.timeSeries}
          dataKey="requests"
          chartFormatter={formatNumber}
          yAxisLabel="Requests"
        />
        <ConsolidatedCard
          title="Duration"
          subtitle="Wall-clock time (GB-s)"
          value={doData.current?.durationGBs || 0}
          formatted={formatGBs(doData.current?.durationGBs || 0)}
          threshold={doData.durationThreshold ? Number(doData.durationThreshold) : null}
          percentage={calculatePercentage(doData.current?.durationGBs || 0, doData.durationThreshold ? Number(doData.durationThreshold) : null)}
          icon="table"
          unit=""
          color="#6366f1"
          timeSeries={doData.timeSeries}
          dataKey="durationGBs"
          chartFormatter={formatGBs}
          yAxisLabel="GB-s"
        />
        {doData.sqliteEnabled && (
          <>
            <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wider pt-2">SQLite Storage Backend</h4>
            <ConsolidatedCard
              title="Rows Read"
              subtitle="SQLite read operations"
              value={doData.current?.sqliteRowsRead || 0}
              formatted={formatNumber(doData.current?.sqliteRowsRead || 0)}
              threshold={doData.sqliteRowsReadThreshold ? doData.sqliteRowsReadThreshold * 1000000 : null}
              percentage={calculatePercentage(doData.current?.sqliteRowsRead || 0, doData.sqliteRowsReadThreshold ? doData.sqliteRowsReadThreshold * 1000000 : null)}
              icon="table"
              unit=""
              color="#3b82f6"
              timeSeries={doData.timeSeries}
              dataKey="sqliteRowsRead"
              chartFormatter={formatNumber}
              yAxisLabel="Rows"
            />
            <ConsolidatedCard
              title="Rows Written"
              subtitle="SQLite write operations"
              value={doData.current?.sqliteRowsWritten || 0}
              formatted={formatNumber(doData.current?.sqliteRowsWritten || 0)}
              threshold={doData.sqliteRowsWrittenThreshold ? doData.sqliteRowsWrittenThreshold * 1000000 : null}
              percentage={calculatePercentage(doData.current?.sqliteRowsWritten || 0, doData.sqliteRowsWrittenThreshold ? doData.sqliteRowsWrittenThreshold * 1000000 : null)}
              icon="table"
              unit=""
              color="#6366f1"
              timeSeries={doData.timeSeries}
              dataKey="sqliteRowsWritten"
              chartFormatter={formatNumber}
              yAxisLabel="Rows"
            />
          </>
        )}
        {doData.kvStorageEnabled && (
          <>
            <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wider pt-2">KV Storage Backend</h4>
            <ConsolidatedCard
              title="Read Request Units"
              subtitle="KV read units"
              value={doData.current?.kvReadUnits || 0}
              formatted={formatNumber(doData.current?.kvReadUnits || 0)}
              threshold={doData.kvReadUnitsThreshold ? doData.kvReadUnitsThreshold * 1000000 : null}
              percentage={calculatePercentage(doData.current?.kvReadUnits || 0, doData.kvReadUnitsThreshold ? doData.kvReadUnitsThreshold * 1000000 : null)}
              icon="table"
              unit=""
              color="#3b82f6"
              timeSeries={doData.timeSeries}
              dataKey="kvReadUnits"
              chartFormatter={formatNumber}
              yAxisLabel="Units"
            />
            <ConsolidatedCard
              title="Write Request Units"
              subtitle="KV write units"
              value={doData.current?.kvWriteUnits || 0}
              formatted={formatNumber(doData.current?.kvWriteUnits || 0)}
              threshold={doData.kvWriteUnitsThreshold ? doData.kvWriteUnitsThreshold * 1000000 : null}
              percentage={calculatePercentage(doData.current?.kvWriteUnits || 0, doData.kvWriteUnitsThreshold ? doData.kvWriteUnitsThreshold * 1000000 : null)}
              icon="table"
              unit=""
              color="#6366f1"
              timeSeries={doData.timeSeries}
              dataKey="kvWriteUnits"
              chartFormatter={formatNumber}
              yAxisLabel="Units"
            />
            <ConsolidatedCard
              title="Delete Requests"
              subtitle="KV delete operations"
              value={doData.current?.kvDeletes || 0}
              formatted={formatNumber(doData.current?.kvDeletes || 0)}
              threshold={doData.kvDeletesThreshold ? doData.kvDeletesThreshold * 1000000 : null}
              percentage={calculatePercentage(doData.current?.kvDeletes || 0, doData.kvDeletesThreshold ? doData.kvDeletesThreshold * 1000000 : null)}
              icon="table"
              unit=""
              color="#f59e0b"
              timeSeries={doData.timeSeries}
              dataKey="kvDeletes"
              chartFormatter={formatNumber}
              yAxisLabel="Requests"
            />
          </>
        )}
        {(doData.sqliteEnabled || doData.kvStorageEnabled) && (
          <>
            <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wider pt-2">Storage</h4>
            <ConsolidatedCard
              title="Stored Data"
              subtitle="All backends"
              value={doData.current?.storageMB || 0}
              formatted={formatStorageMB(doData.current?.storageMB || 0)}
              threshold={doData.storageThreshold ? doData.storageThreshold * 1000 : null}
              percentage={calculatePercentage(doData.current?.storageMB || 0, doData.storageThreshold ? doData.storageThreshold * 1000 : null)}
              icon="storage-mb"
              unit=""
              color="#10b981"
              timeSeries={doData.timeSeries}
              dataKey="storageMB"
              chartFormatter={formatStorageMB}
              yAxisLabel="Storage (MB)"
            />
          </>
        )}
      </div>
    );
  }
}

export default Dashboard;


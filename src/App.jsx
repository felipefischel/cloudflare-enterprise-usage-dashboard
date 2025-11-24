import React, { useState, useEffect } from 'react';
import Dashboard from './components/Dashboard';
import ConfigFormNew from './components/ConfigFormNew';
import { Settings, Info, X, AlertTriangle } from 'lucide-react';

function App() {
  const [isConfigured, setIsConfigured] = useState(false);
  const [showConfig, setShowConfig] = useState(false);
  const [showAbout, setShowAbout] = useState(false);
  const [config, setConfig] = useState(null);
  const [zones, setZones] = useState(null); // Shared zones state
  const [refreshTrigger, setRefreshTrigger] = useState(0); // Trigger for forcing Dashboard refresh

  useEffect(() => {
    // Load saved configuration
    loadConfig();
  }, []);

  const loadConfig = async () => {
    try {
      const response = await fetch('/api/config?userId=default');
      const data = await response.json();
      
      // Migrate old single accountId to accountIds array
      if (data && data.accountId && !data.accountIds) {
        data.accountIds = [data.accountId];
        delete data.accountId;  // Remove old format
        
        // Save migrated config
        try {
          await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              userId: 'default',
              config: data,
            }),
          });
        } catch (migrationError) {
          console.error('Failed to save migrated config:', migrationError);
        }
      }
      
      // Check if account IDs are configured (support both formats)
      const hasAccountIds = data?.accountIds && Array.isArray(data.accountIds) && data.accountIds.length > 0;
      const hasLegacyAccountId = data?.accountId;
      
      if (data && (hasAccountIds || hasLegacyAccountId)) {
        setConfig(data);
        setIsConfigured(true);
      }
    } catch (error) {
      console.error('Failed to load config:', error);
    }
  };

  const handleConfigSave = async (newConfig) => {
    try {
      const response = await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          userId: 'default',
          config: newConfig,
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to save configuration');
      }

      // Update config state immediately to trigger Dashboard re-render
      setConfig(newConfig);
      setIsConfigured(true);
      setShowConfig(false);
      
      // Scroll to top of page
      window.scrollTo({ top: 0, behavior: 'smooth' });
      
      // Trigger Dashboard refresh to pick up new config (including disabled SKUs)
      setRefreshTrigger(prev => prev + 1);
      
      // Note: Dashboard will show prewarming state automatically
      // The Dashboard component will handle the cache prewarm on mount/config change
    } catch (error) {
      console.error('Failed to save config:', error);
      alert('Failed to save configuration. Please try again.');
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-gradient-to-r from-slate-800 to-slate-700 shadow-lg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <div className="flex flex-col">
              <h1 className="text-2xl font-bold text-white">Cloudflare</h1>
              <p className="text-xs text-white font-medium tracking-wide">Enterprise Usage Dashboard</p>
            </div>
          </div>
          
          <div className="flex items-center space-x-3">
            <button
              onClick={() => setShowAbout(true)}
              className="flex items-center space-x-2 px-5 py-2.5 bg-white text-slate-700 rounded-lg hover:bg-slate-50 transition-colors shadow-sm font-medium"
            >
              <Info className="w-4 h-4" />
              <span>About</span>
            </button>
            
            <button
              onClick={() => setShowConfig(!showConfig)}
              className="flex items-center space-x-2 px-5 py-2.5 bg-white text-slate-700 rounded-lg hover:bg-slate-50 transition-colors shadow-sm font-medium"
            >
              <Settings className="w-4 h-4" />
              <span>Settings</span>
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {!isConfigured || showConfig ? (
          <div className="max-w-4xl mx-auto">
            <ConfigFormNew 
              onSave={handleConfigSave} 
              initialConfig={config}
              onCancel={isConfigured ? () => setShowConfig(false) : null}
              cachedZones={zones}
            />
          </div>
        ) : null}
        
        {/* Keep Dashboard mounted but hidden when showing config */}
        {isConfigured && (
          <div className={showConfig ? 'hidden' : ''}>
            <Dashboard 
              config={config}
              zones={zones}
              setZones={setZones}
              refreshTrigger={refreshTrigger}
            />
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="mt-16 bg-white border-t border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <p className="text-center text-sm text-gray-500">
            Built with <span className="text-slate-700 font-medium">Cloudflare Workers</span> • Enterprise Usage Dashboard
          </p>
        </div>
      </footer>

      {/* About Modal */}
      {showAbout && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-xl shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            {/* Header */}
            <div className="bg-gradient-to-r from-slate-800 to-slate-700 px-6 py-4 flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <Info className="w-6 h-6 text-white" />
                <h2 className="text-xl font-bold text-white">About This Dashboard</h2>
              </div>
              <button
                onClick={() => setShowAbout(false)}
                className="text-white hover:bg-white hover:bg-opacity-20 rounded-lg p-2 transition-colors"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            {/* Content */}
            <div className="p-6 space-y-6">
              {/* Important Disclaimer */}
              <div className="bg-yellow-50 border-l-4 border-yellow-400 p-4 rounded-r-lg">
                <div className="flex items-start">
                  <AlertTriangle className="w-5 h-5 text-yellow-600 mt-0.5 mr-3 flex-shrink-0" />
                  <div>
                    <h3 className="text-sm font-bold text-yellow-800 mb-2">⚠️ Important Disclaimer</h3>
                    <p className="text-sm text-yellow-700">
                      <strong>This is NOT an official Cloudflare tool.</strong> Official billing data from Cloudflare may vary from the metrics shown here. For authoritative usage information, always rely on official Cloudflare data and invoices.
                    </p>
                  </div>
                </div>
              </div>

              {/* What is this dashboard */}
              <div>
                <h3 className="text-lg font-semibold text-gray-900 mb-3">What is this?</h3>
                <p className="text-gray-700 leading-relaxed mb-3">
                  The <strong>Enterprise Usage Dashboard</strong> is a tool that helps Cloudflare Enterprise customers monitor their usage against contracted thresholds. It provides real-time visibility into your contracted services.
                </p>
              </div>

              {/* How it works */}
              <div>
                <h3 className="text-lg font-semibold text-gray-900 mb-3">How it works</h3>
                <p className="text-gray-700 leading-relaxed mb-3">
                  The dashboard uses <strong>Cloudflare's GraphQL Analytics API</strong> to query your account's usage data. It can fetch metrics such as:
                </p>
                <ul className="list-disc list-inside text-gray-700 space-y-2 ml-4">
                  <li>Enterprise zone counts</li>
                  <li>HTTP request volumes</li>
                  <li>Data transfer</li>
                  <li>DNS query volumes</li>
                  <li>And many more products!</li>
                </ul>
              </div>

              {/* Features */}
              <div>
                <h3 className="text-lg font-semibold text-gray-900 mb-3">Key Features</h3>
                <ul className="list-disc list-inside text-gray-700 space-y-2 ml-4">
                  <li><strong>Real-time Monitoring:</strong> Track current and previous month usage</li>
                  <li><strong>Threshold Alerts:</strong> Set contracted limits and receive Slack notifications at 90%</li>
                  <li><strong>Zone Breakdowns:</strong> View per-zone metrics and primary/secondary classifications</li>
                  <li><strong>Historical Trends:</strong> Monthly usage charts for long-term visibility</li>
                </ul>
              </div>

              {/* Blocked Traffic */}
              <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                <h3 className="text-sm font-semibold text-green-900 mb-2">🛡️ Blocked Traffic Not Charged</h3>
                <p className="text-sm text-green-800 leading-relaxed">
                  Cloudflare does not charge for traffic blocked by security features (DDoS, WAF, etc.). The <strong>HTTP Requests</strong> and <strong>Data Transfer</strong> metrics shown in this dashboard automatically exclude blocked traffic and reflect only billable/clean traffic that reached your origin or was served from cache.
                </p>
              </div>

              {/* Data Accuracy */}
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                <h3 className="text-sm font-semibold text-blue-900 mb-2">📊 Data Accuracy</h3>
                <p className="text-sm text-blue-800 leading-relaxed">
                  This dashboard queries the same GraphQL API that powers your Cloudflare dashboard. While this data provides a good indication of general usage, it relies on sampling. Each metric on the dashboard includes a Confidence Level metric, based on a 95% confidence interval from Cloudflare's adaptive sampling. For billing purposes, always rely on official Cloudflare data and invoices.
                </p>
              </div>
            </div>

            {/* Footer */}
            <div className="bg-gray-50 px-6 py-4 flex justify-end border-t border-gray-200">
              <button
                onClick={() => setShowAbout(false)}
                className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-medium"
              >
                Got it
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;

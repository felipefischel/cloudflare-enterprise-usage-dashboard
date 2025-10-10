import React, { useState } from 'react';
import { Save, X, TrendingUp, Key, AlertTriangle, Plus, Trash2 } from 'lucide-react';
import { MessageSquare } from 'lucide-react';

function ConfigForm({ onSave, initialConfig, onCancel }) {
  // Migrate old single accountId to accountIds array
  const getInitialAccountIds = () => {
    if (initialConfig?.accountIds && Array.isArray(initialConfig.accountIds)) {
      return initialConfig.accountIds;
    }
    if (initialConfig?.accountId) {
      // Migrate old format
      return [initialConfig.accountId];
    }
    return [''];
  };

  const [formData, setFormData] = useState({
    apiKey: initialConfig?.apiKey || '',
    accountIds: getInitialAccountIds(),
    thresholdZones: initialConfig?.thresholdZones || '',
    primaryZones: initialConfig?.primaryZones || '',
    secondaryZones: initialConfig?.secondaryZones || '',
    // Convert stored values to millions for display
    thresholdRequests: initialConfig?.thresholdRequests ? (initialConfig.thresholdRequests / 1e6) : '',
    // Convert stored bytes to TB for display
    thresholdBandwidth: initialConfig?.thresholdBandwidth ? (initialConfig.thresholdBandwidth / (1024 ** 4)) : '',
    // Convert stored values to millions for display
    thresholdDnsQueries: initialConfig?.thresholdDnsQueries ? (initialConfig.thresholdDnsQueries / 1e6) : '',
    slackWebhook: initialConfig?.slackWebhook || '',
  });

  const [newAccountId, setNewAccountId] = useState('');

  const [errors, setErrors] = useState({});

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
    // Clear error for this field
    if (errors[name]) {
      setErrors(prev => ({ ...prev, [name]: null }));
    }
    // Clear zone breakdown error when any zone field changes
    if (['thresholdZones', 'primaryZones', 'secondaryZones'].includes(name) && errors.zoneBreakdown) {
      setErrors(prev => ({ ...prev, zoneBreakdown: null }));
    }
  };

  const addAccountId = () => {
    if (newAccountId.trim() && !formData.accountIds.includes(newAccountId.trim())) {
      setFormData(prev => ({
        ...prev,
        accountIds: [...prev.accountIds, newAccountId.trim()]
      }));
      setNewAccountId('');
      // Clear any accountIds error
      if (errors.accountIds) {
        setErrors(prev => ({ ...prev, accountIds: null }));
      }
    }
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

  const validate = () => {
    const newErrors = {};

    // API credentials are required
    if (!formData.apiKey) {
      newErrors.apiKey = 'API Token is required';
    }
    
    // At least one account ID is required
    const validAccountIds = formData.accountIds.filter(id => id.trim());
    if (validAccountIds.length === 0) {
      newErrors.accountIds = 'At least one Account ID is required';
    }

    // Validate zone thresholds: primary + secondary must equal total
    const totalZones = formData.thresholdZones ? parseInt(formData.thresholdZones, 10) : 0;
    const primaryZones = formData.primaryZones ? parseInt(formData.primaryZones, 10) : 0;
    const secondaryZones = formData.secondaryZones ? parseInt(formData.secondaryZones, 10) : 0;
    
    if (totalZones > 0 && (primaryZones > 0 || secondaryZones > 0)) {
      const sum = primaryZones + secondaryZones;
      if (sum !== totalZones) {
        newErrors.zoneBreakdown = `Primary zones (${primaryZones}) + Secondary zones (${secondaryZones}) = ${sum}, but Total zones is ${totalZones}. They must be equal.`;
      }
    }

    // Validate Slack webhook if provided
    if (formData.slackWebhook && !formData.slackWebhook.startsWith('https://hooks.slack.com/')) {
      newErrors.slackWebhook = 'Invalid Slack webhook URL';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    
    if (validate()) {
      // Filter out empty account IDs
      const validAccountIds = formData.accountIds.filter(id => id.trim());
      
      const config = {
        apiKey: formData.apiKey,
        accountIds: validAccountIds,
        thresholdZones: formData.thresholdZones ? parseInt(formData.thresholdZones, 10) : null,
        primaryZones: formData.primaryZones ? parseInt(formData.primaryZones, 10) : null,
        secondaryZones: formData.secondaryZones ? parseInt(formData.secondaryZones, 10) : null,
        // Convert millions to actual number (store as raw count)
        thresholdRequests: formData.thresholdRequests ? Math.round(parseFloat(formData.thresholdRequests) * 1e6) : null,
        // Convert TB to bytes (store as bytes)
        thresholdBandwidth: formData.thresholdBandwidth ? Math.round(parseFloat(formData.thresholdBandwidth) * (1024 ** 4)) : null,
        // Convert millions to actual number (store as raw count)
        thresholdDnsQueries: formData.thresholdDnsQueries ? Math.round(parseFloat(formData.thresholdDnsQueries) * 1e6) : null,
        slackWebhook: formData.slackWebhook || '',
        // Preserve alertsEnabled state from initial config
        alertsEnabled: initialConfig?.alertsEnabled !== undefined ? initialConfig.alertsEnabled : false,
      };
      
      onSave(config);
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-lg border border-gray-200 overflow-hidden">
      <div className="bg-gradient-to-r from-slate-700 to-slate-600 px-6 py-4">
        <h2 className="text-xl font-bold text-white">Dashboard Configuration</h2>
        <p className="text-slate-200 text-sm mt-1">
          Configure your Cloudflare credentials and usage thresholds
        </p>
      </div>

      <form onSubmit={handleSubmit} className="p-6 space-y-6">
        {/* Security Warning */}
        <div className="bg-yellow-50 border-l-4 border-yellow-400 p-4">
          <div className="flex items-start">
            <AlertTriangle className="w-5 h-5 text-yellow-400 mt-0.5 mr-3 flex-shrink-0" />
            <div>
              <h3 className="text-sm font-semibold text-yellow-800">Security Notice</h3>
              <p className="text-xs text-yellow-700 mt-1">
                API credentials are stored in KV for easy setup. Consider migrating your API token to a Wrangler secret for enhanced security.
                <a href="#" className="underline ml-1 font-medium">Learn more</a>
              </p>
            </div>
          </div>
        </div>

        {/* API Credentials Section */}
        <div className="space-y-4">
          <h3 className="text-lg font-semibold text-gray-900 flex items-center space-x-2">
            <Key className="w-5 h-5" />
            <span>API Credentials</span>
          </h3>
          <p className="text-sm text-gray-600">
            Your Cloudflare API token and Account ID
          </p>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Cloudflare API Token *
            </label>
            <input
              type="password"
              name="apiKey"
              value={formData.apiKey}
              onChange={handleChange}
              className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                errors.apiKey ? 'border-red-500' : 'border-gray-300'
              }`}
              placeholder="Your Cloudflare API Token"
            />
            {errors.apiKey && (
              <p className="text-red-600 text-sm mt-1">{errors.apiKey}</p>
            )}
            <p className="text-gray-500 text-xs mt-1">
              Create an API token at{' '}
              <a 
                href="https://dash.cloudflare.com/profile/api-tokens" 
                target="_blank" 
                rel="noopener noreferrer"
                className="text-blue-600 hover:underline"
              >
                Cloudflare Dashboard
              </a>
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Account IDs *
            </label>
            
            {/* List of existing account IDs */}
            <div className="space-y-2 mb-3">
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

            {/* Add new account ID */}
            <div className="flex items-center space-x-2">
              <input
                type="text"
                value={newAccountId}
                onChange={(e) => setNewAccountId(e.target.value)}
                onKeyPress={(e) => {
                  if (e.key === 'Enter') {
                    e.preventDefault();
                    addAccountId();
                  }
                }}
                className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="Add another Account ID"
              />
              <button
                type="button"
                onClick={addAccountId}
                className="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                title="Add account"
              >
                <Plus className="w-5 h-5" />
              </button>
            </div>

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

        <div className="border-t border-gray-200 pt-6 space-y-4">
          <h3 className="text-lg font-semibold text-gray-900 flex items-center space-x-2">
            <TrendingUp className="w-5 h-5" />
            <span>Contracted Thresholds</span>
          </h3>
          <p className="text-sm text-gray-600">
            Set your contracted limits to monitor usage against thresholds
          </p>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Enterprise Zones (Total)
              </label>
              <input
                type="number"
                name="thresholdZones"
                value={formData.thresholdZones}
                onChange={handleChange}
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
                name="primaryZones"
                value={formData.primaryZones}
                onChange={handleChange}
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
                name="secondaryZones"
                value={formData.secondaryZones}
                onChange={handleChange}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="e.g., 20"
                min="0"
              />
              <p className="text-xs text-gray-500 mt-1">Zones with &lt;50GB bandwidth/month</p>
            </div>
          </div>

          {/* Zone Breakdown Error */}
          {errors.zoneBreakdown && (
            <div className="bg-red-50 border-l-4 border-red-400 p-4 mt-4">
              <div className="flex items-start">
                <AlertTriangle className="w-5 h-5 text-red-400 mt-0.5 mr-3 flex-shrink-0" />
                <p className="text-sm text-red-700">{errors.zoneBreakdown}</p>
              </div>
            </div>
          )}

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                HTTP Requests (Millions)
              </label>
              <input
                type="number"
                name="thresholdRequests"
                value={formData.thresholdRequests}
                onChange={handleChange}
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
                name="thresholdBandwidth"
                value={formData.thresholdBandwidth}
                onChange={handleChange}
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
                name="thresholdDnsQueries"
                value={formData.thresholdDnsQueries}
                onChange={handleChange}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="e.g., 500"
                min="0"
                step="0.01"
              />
              <p className="text-xs text-gray-500 mt-1">In millions (M)</p>
            </div>
          </div>
        </div>

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
              name="slackWebhook"
              value={formData.slackWebhook}
              onChange={handleChange}
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
        <div className="flex items-center justify-end space-x-3 pt-6 border-t border-gray-200">
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
      </form>
    </div>
  );
}

export default ConfigForm;

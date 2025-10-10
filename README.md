# Cloudflare Enterprise Usage Dashboard

A real-time dashboard for Cloudflare Enterprise customers to monitor their monthly consumption against contracted limits. Built with Cloudflare Workers, React, and Vite.

## ⚠️ Important disclaimer:
This is NOT an official Cloudflare tool. Official billing data from Cloudflare may vary from the metrics shown here. Always refer to your official Cloudflare invoices and account dashboard for authoritative usage information.

## Features

- 📊 **Real-time Usage Monitoring**: Track key metrics:
  - Number of Enterprise Zones
  - HTTP Requests
  - Data Transfer
  - DNS Queries

- 📈 **Usage Analytics**: 
  - Current month vs. previous month comparison
  - Charts showing usage trends over time
  - Visual progress bars showing consumption against contracted thresholds

- 🔔 **Alerts**: 
  - Slack webhook notifications when usage reaches 90% of thresholds
  - Toggle alerts on/off as needed

## Prerequisites

- Node.js 18+ and npm
- Cloudflare account with Enterprise plan
- Cloudflare API Token with appropriate permissions

## Getting Started

### 1. Clone the Repository

```bash
git clone <repository-url>
cd cloudflare-enterprise-usage-dashboard
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Create KV Namespace

```bash
npx wrangler kv namespace create CONFIG_KV
```

Copy the namespace ID from the output and update `wrangler.toml`:

```toml
[[kv_namespaces]]
binding = "CONFIG_KV"
id = "YOUR_KV_NAMESPACE_ID"
```

### 4. Deploy to Cloudflare Workers

```bash
npm run build
npx wrangler deploy
```

After deployment, wrangler will output your Worker URL (e.g., `https://your-worker.your-subdomain.workers.dev`)

## Configuration

After deployment, access your dashboard using the link provided by wrangler and configure the following:

### API Credentials

Create a 'Read all resources' API token at [Cloudflare Dashboard](https://dash.cloudflare.com/profile/api-tokens)

Then enter in the dashboard:

- **Cloudflare API Token**: Your API token created above
- **Account IDs**: Found in Cloudflare Dashboard URL or account settings

**💡 Multi-Account Support:**

- You can add multiple Cloudflare account IDs to monitor usage across all your accounts
- Metrics are automatically aggregated (zones, requests, bandwidth, DNS queries)
- Your API token must have access to all accounts you want to monitor
- Click the "+" button to add more accounts

### Contracted Thresholds

Set your contracted limits for **aggregated usage** across all accounts:
- **Enterprise Zones**: Total number of enterprise zones across all accounts
- **HTTP Requests**: Total HTTP requests contracted per month (all accounts combined)
- **Data Transfer**: Total data transfer contracted per month (all accounts combined)
- **DNS Queries**: Total DNS Queries contracted per month (all accounts combined)

### Slack Notifications (Optional)
- **Slack Webhook URL**: Get from Slack's Incoming Webhooks app
- Alerts trigger when usage reaches 90% of any threshold
- One alert per metric per month (automatic deduplication)
- "Send Now" button for manual testing

## Security Considerations

### API Token Storage

By default, the API token is configured via the dashboard UI and stored in Cloudflare KV.

**For enhanced security**, you can optionally migrate to using Wrangler secrets for the API token (do this AFTER initial deployment and configuration):

```bash
npx wrangler secret put CLOUDFLARE_API_TOKEN
```

**Important:** You must deploy the Worker first (step 4) before you can set secrets.

If this secret is set, it will override the UI-configured API token. This approach:

- ✅ Keeps API token out of KV storage
- ✅ Managed via Cloudflare's secret management
- ✅ Can be rotated without redeploying code

**Note:** Account IDs are always configured via the dashboard UI (not via secrets) to support multi-account setups.

## Architecture

### Automatic Threshold Monitoring

The dashboard includes a **Cloudflare Cron Trigger** that automatically checks thresholds every 6 hours:

- Runs at: 00:00, 06:00, 12:00, 18:00 UTC
- No dashboard access required
- Fetches current metrics from all configured accounts
- Sends Slack alerts if thresholds exceeded
- View logs: `npx wrangler tail --format pretty`

### Data Storage

- **KV Storage**: Configuration, thresholds, historical data
- **Monthly snapshots**: Stored for 1 year
- **Alert tracking**: Prevents duplicate notifications

## Troubleshooting

### "Failed to fetch metrics" Error
- Verify your API token has the correct permissions
- Check that your Account ID is correct
- Ensure the API token hasn't expired
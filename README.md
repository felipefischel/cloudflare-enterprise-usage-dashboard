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

### 4. Set Your API Token (Required)

Create a 'Read all resources' API token at [Cloudflare Dashboard](https://dash.cloudflare.com/profile/api-tokens), then store it securely as a Wrangler secret:

```bash
npx wrangler secret put CLOUDFLARE_API_TOKEN
```

When prompted, paste your API token. This stores it encrypted and keeps it out of your codebase.

### 5. Deploy to Cloudflare Workers

First build the project:

```bash
npm run build
```

Then deploy:

```bash
npx wrangler deploy
```

After deployment, wrangler will output your Worker URL (e.g., `https://your-worker.your-subdomain.workers.dev`)

### 6. (Optional) Enable Cloudflare Access

To limit access to your Worker to specific users or groups, you can enable Cloudflare Access:

1. In the [Cloudflare dashboard](https://dash.cloudflare.com), go to **Workers & Pages**
2. Select your Worker from the Overview
3. Go to **Settings → Domains & Routes**
4. For `workers.dev` or Preview URLs, click **Enable Cloudflare Access**
5. (Optional) Click **Manage Cloudflare Access** to configure authorized email addresses

Access allows you to restrict access to yourself, your teammates, your organization, or anyone else you specify in your Access policy. Learn more about [Access policies](https://developers.cloudflare.com/cloudflare-one/policies/access/).

## Configuration

After deployment, access your dashboard using the link provided by wrangler and click the **Settings** icon to configure:

### Account IDs

Enter your Cloudflare Account ID(s):

- **Account IDs**: Found in Cloudflare Dashboard URL or account settings
- Click **"+ Add Another Account"** to monitor multiple accounts

**💡 Multi-Account Support:**

- Monitor usage across multiple Cloudflare accounts
- Metrics are automatically aggregated (zones, requests, bandwidth, DNS queries)
- Your API token (set via wrangler secret) must have access to all accounts

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
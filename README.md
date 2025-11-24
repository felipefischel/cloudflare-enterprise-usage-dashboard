# Cloudflare Enterprise Usage Dashboard

A real-time dashboard for Cloudflare Enterprise customers to monitor their monthly consumption against contracted limits. Built with Cloudflare Workers, React, and Vite.

<img width="857" height="935" alt="Screenshot 2025-11-06 at 17 30 08" src="https://github.com/user-attachments/assets/68b8c7c4-a3e4-4e5a-a9ae-aa77471bd20a" />

<img width="823" height="868" alt="Screenshot 2025-11-06 at 17 30 24" src="https://github.com/user-attachments/assets/0249c863-1bd9-47fa-8870-9fca098aad69" />


## ⚠️ Important Disclaimer

This is NOT an official Cloudflare tool. Official billing data from Cloudflare may vary from the metrics shown here. For authoritative usage information, always rely on official Cloudflare data and invoices.

## Features

- 📊 **Real-time Usage Monitoring**: Track your contracted services:
  - Enterprise Zones
  - HTTP Requests (billable traffic only)
  - Data Transfer (bilable traffic only)
  - DNS Queries
  - Application Security products (Bot Management, API Shield, Page Shield, etc.)
  - Other add-on products

- 🛡️ **Blocked Traffic Excluded**:
Cloudflare does not charge for traffic blocked by security features (DDoS, WAF, etc.). The HTTP Requests and Data Transfer metrics shown in this dashboard automatically exclude blocked traffic and reflect only billable/clean traffic that reached your origin or was served from cache.

- 📈 **Usage Analytics**:
  - Current month vs. previous month comparison
  - Historical trends with monthly charts
  - Visual progress bars showing consumption against thresholds
  - Per-zone breakdowns for detailed analysis

- 🔔 **Threshold Alerts**:
  - Slack notifications when usage reaches 90% of thresholds
  - Automatic monitoring every 6 hours via cron trigger
  - Toggle alerts on/off as needed

## Prerequisites

- Node.js 18+ and npm
- Cloudflare account with Enterprise plan
- Cloudflare API Token with appropriate permissions

## How to Deploy

## Automatic Deployment (Recommended)

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/felipefischel/cloudflare-enterprise-usage-dashboard)

*Tip: Right-click and "Open in new tab" to keep this page open.*

The easiest way to get started is using the **Deploy to Cloudflare** button above.

**During deployment, you'll be prompted to:**

1. ✅ **Set your API token** - Paste your Cloudflare API token with "Read all resources" permissions (create one at [API Tokens](https://dash.cloudflare.com/profile/api-tokens))
2. ✅ **Accept default settings** - The KV namespace and other resources will be created automatically

**The deploy process will automatically:**

1. ✅ Clone the repository to your GitHub account
2. ✅ Create and configure a KV namespace
3. ✅ Build and deploy the Worker to your Cloudflare account
4. ✅ Set up cron triggers for automatic monitoring

**After deployment:**

1. **Configure your dashboard:**
   - Visit your Worker URL
   - Click the Settings icon
   - Enter your Account IDs and contracted thresholds

2. **(Optional) Enable Cloudflare Access:**
   - Navigate to: [Cloudflare Dashboard](https://dash.cloudflare.com) → **Workers & Pages** → **enterprise-usage-dashboard**
   - Go to **Settings** → **Domains & Routes**
   - For `workers.dev` or Preview URLs, click **Enable Cloudflare Access**
   - (Optional) Click **Manage Cloudflare Access** to configure authorized email addresses
   - Learn more: [Access policies documentation](https://developers.cloudflare.com/cloudflare-one/policies/access/)

   This allows you to restrict access to yourself, your teammates, your organization, or anyone else you specify.

**That's it! Your dashboard is ready to use.**

## Manual Deployment

If you prefer to deploy manually or need more control over the setup:

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

First build the project:

```bash
npm run build
```

Then deploy:

```bash
npx wrangler deploy
```

After deployment, wrangler will output your Worker URL (e.g., `https://your-worker.your-subdomain.workers.dev`)

### 5. Set Your API Token

Create a 'Read all resources' API token at [Cloudflare Dashboard](https://dash.cloudflare.com/profile/api-tokens).

Then store it securely as a Wrangler secret:

```bash
npx wrangler secret put CLOUDFLARE_API_TOKEN
```

When prompted, paste your API token. This stores it encrypted in Cloudflare's secret management system.

### 6. (Optional) Enable Cloudflare Access

To limit access to your Worker to specific users or groups, you can enable Cloudflare Access:

1. In the [Cloudflare dashboard](https://dash.cloudflare.com), go to **Workers & Pages**
2. Select your Worker from the Overview
3. Go to **Settings → Domains & Routes**
4. For `workers.dev` or Preview URLs, click **Enable Cloudflare Access**
5. (Optional) Click **Manage Cloudflare Access** to configure authorized email addresses

Access allows you to restrict access to yourself, your teammates, your organization, or anyone else you specify in your Access policy. Learn more about [Access policies](https://developers.cloudflare.com/cloudflare-one/policies/access/).

## Configuration

After deployment and setting your API token, access your dashboard using the Worker URL and click the **Settings** icon to configure:

### Account IDs

Enter your Cloudflare Account ID(s):

- **Account IDs**: Found in Cloudflare Dashboard URL or account settings
- Click **"+ Add Another Account"** to monitor multiple accounts

**💡 Multi-Account Support:**

- Monitor usage across multiple Cloudflare accounts
- Metrics are automatically aggregated (zones, requests, bandwidth, DNS queries)
- Your API token must have access to all accounts you want to monitor

### Contracted Thresholds

Set your contracted limits for **aggregated usage** across all accounts:

- **Enterprise Zones**: Total number of enterprise zones
- **HTTP Requests**: Total clean HTTP requests per month
- **Data Transfer**: Total clean data transfer per month
- **DNS Queries**: Total DNS queries per month
- **Application Security Products**: Configure thresholds for Bot Management, API Shield, Page Shield, and Advanced Rate Limiting

### Slack Notifications (Optional)

- **Slack Webhook URL**: Get from Slack's Incoming Webhooks app
- Alerts trigger when usage reaches 90% of any threshold
- One alert per metric per month (automatic deduplication)
- "Send Now" button for manual testing

### Automatic Threshold Monitoring

The dashboard includes a **Cloudflare Cron Trigger** that automatically checks thresholds every 6 hours:

- Runs at: 00:00, 06:00, 12:00, 18:00 UTC
- No dashboard access required
- Fetches current metrics from all configured accounts
- Sends Slack alerts if thresholds exceeded
- View logs: `npx wrangler tail --format pretty`

### Data Storage & Accuracy

- **KV Storage**: Configuration, thresholds, and historical data
- **Monthly snapshots**: Cached for 1 year for faster loading
- **Alert tracking**: Prevents duplicate notifications
- **Data source**: GraphQL Analytics API (same API that powers your Cloudflare dashboard)
- **Sampling**: This dashboard relies on sampled data - for billing purposes, always refer to official Cloudflare data and invoices
- **Confidence Levels**: Each metric includes a confidence indicator based on a 95% confidence interval from Cloudflare's adaptive sampling. Higher confidence percentages (closer to 100%) indicate more accurate estimates. Hover over the confidence badge to see detailed statistics including sample size and confidence range.

## Troubleshooting

### "Failed to fetch metrics" Error

- Verify your API token has the correct permissions
- Check that your Account ID is correct
- Ensure the API token hasn't expired

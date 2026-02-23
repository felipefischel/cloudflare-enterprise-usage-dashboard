/**
 * Service Categories and SKU Definitions
 * Defines the structure for multi-service dashboard
 */

export const SERVICE_CATEGORIES = {
  APPLICATION_SERVICES: 'application_services',
  CLOUDFLARE_ONE: 'cloudflare_one',
  NETWORK_SERVICES: 'network_services',
  DEVELOPER_PLATFORM: 'developer_platform',
};

export const SERVICE_METADATA = {
  [SERVICE_CATEGORIES.APPLICATION_SERVICES]: {
    id: SERVICE_CATEGORIES.APPLICATION_SERVICES,
    name: 'Application Services',
    description: 'WAF, DDoS, CDN, and application performance',
    icon: '🛡️',
  },
  [SERVICE_CATEGORIES.CLOUDFLARE_ONE]: {
    id: SERVICE_CATEGORIES.CLOUDFLARE_ONE,
    name: 'Cloudflare One',
    description: 'Zero Trust access, gateway, and WAN connectivity',
    icon: '🔐',
  },
  [SERVICE_CATEGORIES.NETWORK_SERVICES]: {
    id: SERVICE_CATEGORIES.NETWORK_SERVICES,
    name: 'Network Services',
    description: 'Magic Transit, DDoS protection, and network connectivity',
    icon: '🌐',
  },
  [SERVICE_CATEGORIES.DEVELOPER_PLATFORM]: {
    id: SERVICE_CATEGORIES.DEVELOPER_PLATFORM,
    name: 'Developer Platform',
    description: 'Workers, Pages, R2, and developer tools',
    icon: '⚡',
  },
};

// SKU Types
export const SKU_TYPES = {
  ACCOUNT_LEVEL: 'account',
  ZONE_LEVEL: 'zone',
};

// SKU Definitions for Application Services - Core (existing metrics)
export const APPLICATION_SERVICES_CORE_SKUS = {
  ENTERPRISE_ZONES: {
    id: 'enterprise_zones',
    name: 'Enterprise Zones',
    type: SKU_TYPES.ACCOUNT_LEVEL,
    unit: 'zones',
    category: SERVICE_CATEGORIES.APPLICATION_SERVICES,
    section: 'core',
  },
  HTTP_REQUESTS: {
    id: 'http_requests',
    name: 'Billable HTTP Requests',
    description: 'Clean traffic (excluding blocked requests)',
    type: SKU_TYPES.ACCOUNT_LEVEL,
    unit: 'M', // Millions
    category: SERVICE_CATEGORIES.APPLICATION_SERVICES,
    section: 'core',
  },
  DATA_TRANSFER: {
    id: 'data_transfer',
    name: 'Data Transfer',
    type: SKU_TYPES.ACCOUNT_LEVEL,
    unit: 'TB',
    category: SERVICE_CATEGORIES.APPLICATION_SERVICES,
    section: 'core',
  },
  DNS_QUERIES: {
    id: 'dns_queries',
    name: 'DNS Queries',
    type: SKU_TYPES.ACCOUNT_LEVEL,
    unit: 'M', // Millions
    category: SERVICE_CATEGORIES.APPLICATION_SERVICES,
    section: 'core',
  },
};

// SKU Definitions for Application Services - Add-ons
export const APPLICATION_SERVICES_ADDON_SKUS = {
  BOT_MANAGEMENT: {
    id: 'bot_management',
    name: 'Bot Management',
    description: 'Good Requests (Likely Human traffic with bot score > 30)',
    type: SKU_TYPES.ZONE_LEVEL,
    unit: 'M', // Millions
    category: SERVICE_CATEGORIES.APPLICATION_SERVICES,
    section: 'addons',
  },
  API_SHIELD: {
    id: 'api_shield',
    name: 'API Shield',
    description: 'HTTP Requests to API endpoints',
    type: SKU_TYPES.ZONE_LEVEL,
    unit: 'M', // Millions
    category: SERVICE_CATEGORIES.APPLICATION_SERVICES,
    section: 'addons',
  },
  PAGE_SHIELD: {
    id: 'page_shield',
    name: 'Page Shield',
    description: 'HTTP Requests to protected pages',
    type: SKU_TYPES.ZONE_LEVEL,
    unit: 'M', // Millions
    category: SERVICE_CATEGORIES.APPLICATION_SERVICES,
    section: 'addons',
  },
  ADVANCED_RATE_LIMITING: {
    id: 'advanced_rate_limiting',
    name: 'Advanced Rate Limiting',
    description: 'HTTP Requests processed by rate limiting rules',
    type: SKU_TYPES.ZONE_LEVEL,
    unit: 'M', // Millions
    category: SERVICE_CATEGORIES.APPLICATION_SERVICES,
    section: 'addons',
  },
};

// Combined Application Services SKUs
export const APPLICATION_SERVICES_SKUS = {
  ...APPLICATION_SERVICES_CORE_SKUS,
  ...APPLICATION_SERVICES_ADDON_SKUS,
};

// Zero Trust SKUs
export const ZERO_TRUST_SKUS = {
  SEATS: {
    id: 'seats',
    name: 'Zero Trust Seats',
    description: 'Active users consuming Access or Gateway seats',
    type: SKU_TYPES.ACCOUNT_LEVEL,
    unit: 'seats',
    category: SERVICE_CATEGORIES.ZERO_TRUST,
    section: 'core',
  },
};

export const CLOUDFLARE_ONE_SKUS = {
  ...ZERO_TRUST_SKUS,
  WAN: {
    id: 'magicWan',
    name: 'WAN',
    description: 'P95th bandwidth for WAN tunnels',
    type: SKU_TYPES.ACCOUNT_LEVEL,
    unit: 'Mbps',
    category: SERVICE_CATEGORIES.CLOUDFLARE_ONE,
    section: 'core',
  },
};

export const DEVELOPER_PLATFORM_SKUS = {};

/**
 * Get all SKUs for a service category
 */
export function getSKUsForService(serviceId) {
  switch (serviceId) {
    case SERVICE_CATEGORIES.APPLICATION_SERVICES:
      return APPLICATION_SERVICES_SKUS;
    case SERVICE_CATEGORIES.CLOUDFLARE_ONE:
      return CLOUDFLARE_ONE_SKUS;
    case SERVICE_CATEGORIES.NETWORK_SERVICES:
      return {};
    case SERVICE_CATEGORIES.DEVELOPER_PLATFORM:
      return DEVELOPER_PLATFORM_SKUS;
    default:
      return {};
  }
}

/**
 * Check if a service has zone-level SKUs
 */
export function serviceHasZoneLevelSKUs(serviceId) {
  const skus = getSKUsForService(serviceId);
  return Object.values(skus).some(sku => sku.type === SKU_TYPES.ZONE_LEVEL);
}

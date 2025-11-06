/**
 * Service Categories and SKU Definitions
 * Defines the structure for multi-service dashboard
 */

export const SERVICE_CATEGORIES = {
  APPLICATION_SERVICES: 'application_services',
  ZERO_TRUST: 'zero_trust',
  NETWORK_SERVICES: 'network_services',
  DEVELOPER_SERVICES: 'developer_services',
};

export const SERVICE_METADATA = {
  [SERVICE_CATEGORIES.APPLICATION_SERVICES]: {
    id: SERVICE_CATEGORIES.APPLICATION_SERVICES,
    name: 'Application Services',
    description: 'WAF, DDoS, CDN, and application performance',
    icon: '🛡️',
  },
  [SERVICE_CATEGORIES.ZERO_TRUST]: {
    id: SERVICE_CATEGORIES.ZERO_TRUST,
    name: 'Zero Trust Services',
    description: 'Access, Gateway, WARP, and device security',
    icon: '🔐',
  },
  [SERVICE_CATEGORIES.NETWORK_SERVICES]: {
    id: SERVICE_CATEGORIES.NETWORK_SERVICES,
    name: 'Network Services',
    description: 'Magic Transit, Spectrum, and network infrastructure',
    icon: '🌐',
  },
  [SERVICE_CATEGORIES.DEVELOPER_SERVICES]: {
    id: SERVICE_CATEGORIES.DEVELOPER_SERVICES,
    name: 'Developer Services',
    description: 'Workers, Pages, R2, D1, and developer platform',
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

// Placeholder for future SKUs
export const ZERO_TRUST_SKUS = {
  // Will be added later
  // Example:
  // GATEWAY_DNS_QUERIES: { ... }
  // ACCESS_SEATS: { ... }
};

export const NETWORK_SERVICES_SKUS = {
  // Will be added later
  // Example:
  // MAGIC_TRANSIT_BANDWIDTH: { ... }
  // SPECTRUM_BANDWIDTH: { ... }
};

export const DEVELOPER_SERVICES_SKUS = {
  // Will be added later
  // Example:
  // WORKERS_REQUESTS: { ... }
  // R2_STORAGE: { ... }
};

/**
 * Get all SKUs for a service category
 */
export function getSKUsForService(serviceId) {
  switch (serviceId) {
    case SERVICE_CATEGORIES.APPLICATION_SERVICES:
      return APPLICATION_SERVICES_SKUS;
    case SERVICE_CATEGORIES.ZERO_TRUST:
      return ZERO_TRUST_SKUS;
    case SERVICE_CATEGORIES.NETWORK_SERVICES:
      return NETWORK_SERVICES_SKUS;
    case SERVICE_CATEGORIES.DEVELOPER_SERVICES:
      return DEVELOPER_SERVICES_SKUS;
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

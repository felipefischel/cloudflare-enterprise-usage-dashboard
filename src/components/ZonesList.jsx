import React from 'react';

function ZonesList({ zones, zoneMetrics, usePreviousClassification = false, previousMonthMetrics = null }) {
  if (!zones || zones.length === 0) {
    return null;
  }

  // Merge zones with metrics
  const zonesWithMetrics = zones.map(zone => {
    const metrics = zoneMetrics?.find(m => m.zoneTag === zone.id);
    
    // If showing current month but using previous classification, get classification from previous month
    let isPrimary = metrics?.isPrimary;
    if (usePreviousClassification && previousMonthMetrics) {
      const prevMetrics = previousMonthMetrics.find(m => m.zoneTag === zone.id);
      isPrimary = prevMetrics?.isPrimary;
    }
    
    return {
      ...zone,
      ...metrics,
      isPrimary
    };
  });

  // Sort by bandwidth (highest first)
  const sortedZones = [...zonesWithMetrics].sort((a, b) => (b.bytes || 0) - (a.bytes || 0));

  const formatBandwidth = (bytes) => {
    if (!bytes) return '0 GB';
    
    // Use decimal units (1000-based) for bandwidth
    const tb = bytes / (1000 ** 4);
    const gb = bytes / (1000 ** 3);
    
    // Helper to round and remove unnecessary trailing zeros
    const cleanNumber = (num) => {
      const rounded = Math.round(num * 100) / 100; // Round to 2 decimals
      return parseFloat(rounded.toFixed(2)).toString();
    };
    
    if (tb >= 1) {
      return `${cleanNumber(tb)} TB`;
    }
    // Show 0 GB if value rounds to 0
    if (gb < 0.01) {
      return '0 GB';
    }
    return `${cleanNumber(gb)} GB`;
  };

  const formatRequests = (requests) => {
    if (!requests) return '0';
    
    // Helper to round and remove unnecessary trailing zeros
    const cleanNumber = (num) => {
      const rounded = Math.round(num * 100) / 100; // Round to 2 decimals
      return parseFloat(rounded.toFixed(2)).toString();
    };
    
    if (requests >= 1e6) {
      return `${cleanNumber(requests / 1e6)}M`;
    }
    if (requests >= 1e3) {
      return `${cleanNumber(requests / 1e3)}K`;
    }
    return requests.toString();
  };

  return (
    <div className="bg-gray-50 border border-gray-200 rounded-lg overflow-hidden">
      <div className="max-h-96 overflow-y-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-100 sticky top-0 z-10">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Zone
              </th>
              <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">
                Type
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Data Transfer
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                HTTP Requests
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                DNS Queries
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {sortedZones.map((zone) => (
              <tr key={zone.id} className="hover:bg-gray-50">
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="text-sm font-medium text-gray-900">{zone.name}</div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-center">
                  {zone.isPrimary !== undefined && (
                    <span className={`px-2 py-1 text-xs font-medium rounded ${
                      zone.isPrimary 
                        ? 'bg-green-100 text-green-700' 
                        : 'bg-blue-100 text-blue-700'
                    }`}>
                      {zone.isPrimary ? 'Primary' : 'Secondary'}
                    </span>
                  )}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 font-medium">
                  {formatBandwidth(zone.bytes)}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 font-medium">
                  {formatRequests(zone.requests || 0)}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 font-medium">
                  {formatRequests(zone.dnsQueries || 0)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default ZonesList;

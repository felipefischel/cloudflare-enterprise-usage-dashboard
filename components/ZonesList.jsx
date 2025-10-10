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
    const gb = bytes / (1024 ** 3);
    if (gb >= 1024) {
      return `${(gb / 1024).toFixed(2)} TB`;
    }
    return `${gb.toFixed(2)} GB`;
  };

  const formatRequests = (requests) => {
    if (!requests) return '0';
    if (requests >= 1e6) {
      return `${(requests / 1e6).toFixed(2)}M`;
    }
    if (requests >= 1e3) {
      return `${(requests / 1e3).toFixed(2)}K`;
    }
    return requests.toString();
  };

  return (
    <div className="max-h-96 overflow-y-auto pr-2 space-y-2">
      {sortedZones.map((zone, index) => (
        <div 
          key={zone.id} 
          className="flex items-center justify-between p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors"
        >
          <div className="flex items-center space-x-3 flex-1 min-w-0">
            <div className="flex-shrink-0 w-8 h-8 bg-gradient-to-br from-slate-600 to-slate-700 rounded-lg flex items-center justify-center">
              <span className="text-white text-sm font-semibold">
                {index + 1}
              </span>
            </div>
            <div className="flex-1 min-w-0">
              <div className="flex items-center space-x-2">
                <p className="font-medium text-gray-900 truncate">{zone.name}</p>
                {zone.isPrimary !== undefined && (
                  <span className={`px-2 py-0.5 text-xs font-medium rounded ${
                    zone.isPrimary 
                      ? 'bg-green-100 text-green-700' 
                      : 'bg-blue-100 text-blue-700'
                  }`}>
                    {zone.isPrimary ? 'Primary' : 'Secondary'}
                  </span>
                )}
              </div>
              <p className="text-xs text-gray-500 font-mono truncate">{zone.id}</p>
            </div>
          </div>
          {zoneMetrics && (
            <div className="flex items-center space-x-4 text-sm">
              <div className="text-right">
                <p className="text-xs text-gray-500">Bandwidth</p>
                <p className="font-semibold text-gray-900">{formatBandwidth(zone.bytes)}</p>
              </div>
              <div className="text-right">
                <p className="text-xs text-gray-500">Requests</p>
                <p className="font-semibold text-gray-900">{formatRequests(zone.requests)}</p>
              </div>
              <div className="text-right">
                <p className="text-xs text-gray-500">DNS Queries</p>
                <p className="font-semibold text-gray-900">{formatRequests(zone.dnsQueries || 0)}</p>
              </div>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

export default ZonesList;

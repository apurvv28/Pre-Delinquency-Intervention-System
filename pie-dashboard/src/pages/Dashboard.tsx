import { useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { pieApi } from '../api/pie';
import { normalizeRiskTier, riskTierStyles } from '../lib/risk';
import RiskTierBadge from '../components/RiskTierBadge';
import StatCard from '../components/StatCard';
import LoadingSpinner from '../components/LoadingSpinner';
import { useStreamWebSocket, type StreamEvent } from '../hooks/useStreamWebSocket';

export default function Dashboard() {
  const queryClient = useQueryClient();
  const [showRestartConfirm, setShowRestartConfirm] = useState(false);
  const [isRestarting, setIsRestarting] = useState(false);
  const [lastUpdate, setLastUpdate] = useState<StreamEvent | null>(null);
  
  // WebSocket for real-time transaction updates
  const { isConnected } = useStreamWebSocket((event: StreamEvent) => {
    setLastUpdate(event);
  });

  const { data: snapshot } = useQuery({
    queryKey: ['metricSnapshot'],
    queryFn: pieApi.getMetricSnapshot,
    staleTime: 15000,
    refetchOnWindowFocus: false,
  });

  const { data: registry = [] } = useQuery({
    queryKey: ['registryDashboard'],
    queryFn: pieApi.getRegistry,
    refetchInterval: 30000,
    staleTime: 15000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
  });

  const { data: customers = [] } = useQuery({
    queryKey: ['customersDashboard'],
    queryFn: pieApi.getCustomers,
    refetchInterval: 30000,
    staleTime: 30000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
  });

  const distribution = useMemo(() => {
    const seeded = {
      VERY_LOW: 0,
      LOW: 0,
      MEDIUM: 0,
      HIGH: 0,
      CRITICAL: 0,
      VERY_CRITICAL: 0,
    };

    registry.forEach((row) => {
      const tier = normalizeRiskTier(row.risk_bucket, row.risk_score);
      seeded[tier] += 1;
    });

    return Object.entries(seeded).map(([tier, count]) => ({
      tier,
      count,
      color: riskTierStyles(tier as Parameters<typeof riskTierStyles>[0]).ring,
    }));
  }, [registry]);

  const topAccounts = useMemo(() => {
    const latestByCustomer = new Map<string, (typeof registry)[number]>();

    registry.forEach((row) => {
      const current = latestByCustomer.get(row.customer_id);
      const rowTs = new Date(row.timestamp ?? row.created_at ?? 0).getTime();
      const currentTs = current ? new Date(current.timestamp ?? current.created_at ?? 0).getTime() : -1;
      if (!current || rowTs >= currentTs) {
        latestByCustomer.set(row.customer_id, row);
      }
    });

    return [...latestByCustomer.values()].sort((a, b) => b.risk_score - a.risk_score).slice(0, 10);
  }, [registry]);

  const liveRiskTrend = useMemo(() => {
    const byDay = new Map<string, { total: number; count: number }>();

    registry.forEach((row) => {
      const sourceTs = row.timestamp ?? row.created_at;
      if (!sourceTs) return;
      const day = new Date(sourceTs).toISOString().slice(0, 10);
      const current = byDay.get(day) ?? { total: 0, count: 0 };
      current.total += row.risk_score;
      current.count += 1;
      byDay.set(day, current);
    });

    return Array.from(byDay.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .slice(-12)
      .map(([day, stats]) => ({
        day,
        avgRisk: Number((stats.total / Math.max(stats.count, 1)).toFixed(2)),
      }));
  }, [registry]);

  const branchRiskData = useMemo(() => {
    const branchAgg = new Map<string, { total: number; count: number }>();

    customers.forEach((customer) => {
      if (customer.latest_risk_score == null) return;
      const current = branchAgg.get(customer.branch) ?? { total: 0, count: 0 };
      current.total += customer.latest_risk_score;
      current.count += 1;
      branchAgg.set(customer.branch, current);
    });

    return Array.from(branchAgg.entries())
      .map(([branch, stats]) => ({
        branch,
        score: Number((stats.total / Math.max(stats.count, 1)).toFixed(1)),
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 8);
  }, [customers]);

  const handleRestartStream = async () => {
    setIsRestarting(true);
    try {
      const result = await pieApi.restartStream();
      console.log('Stream restart successful:', result);
      
      // Invalidate all queries to refresh the dashboard
      await queryClient.invalidateQueries({ queryKey: ['metricSnapshot'] });
      await queryClient.invalidateQueries({ queryKey: ['registryDashboard'] });
      await queryClient.invalidateQueries({ queryKey: ['customersDashboard'] });
      
      setShowRestartConfirm(false);
    } catch (error) {
      console.error('Stream restart failed:', error);
      alert(`Failed to restart stream: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setIsRestarting(false);
    }
  };

  return (
    <section className="space-y-5 p-6">
      <div className="flex items-center justify-between gap-4 mb-4">
        <div className="flex items-center gap-3">
          <h1 className="font-syne text-3xl text-[#0F172A]">Dashboard</h1>
          <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-[#E2E6ED] bg-white text-xs">
            <div className={`h-2 w-2 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`} />
            <span className="font-medium text-[#475569]">{isConnected ? 'Live' : 'Offline'}</span>
          </div>
          {lastUpdate && (
            <div className="px-3 py-1.5 rounded-lg border border-[#E2E6ED] bg-[#EFF6FF] text-xs font-mono text-[#0057B8] max-w-xs truncate">
              {lastUpdate.type === 'transaction' 
                ? `Tx: ${lastUpdate.data.customer_id.slice(0, 8)}... ₹${lastUpdate.data.amount.toFixed(0)}`
                : lastUpdate.type === 'risk_score_update'
                  ? `Score: ${lastUpdate.data.customer_id.slice(0, 8)}... ${lastUpdate.data.risk_score.toFixed(2)}%`
                : 'Model update'}
            </div>
          )}
        </div>
        <button
          onClick={() => setShowRestartConfirm(true)}
          disabled={isRestarting}
          className="rounded-lg bg-[#003366] px-4 py-2.5 font-semibold text-white transition hover:bg-[#002244] disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
        >
          {isRestarting && <LoadingSpinner />}
          {isRestarting ? 'Restarting...' : 'Restart Stream'}
        </button>
      </div>

      {showRestartConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="rounded-xl border border-[#E2E6ED] bg-white p-6 shadow-lg max-w-md">
            <h3 className="font-syne text-xl text-[#0F172A]">Restart Stream?</h3>
            <p className="mt-2 text-sm text-[#475569]">
              This will delete all existing stream data and transactions, then restart from 0 transactions for all customers.
              The stream will reflow through the ML models (LightGBM + XGBoost fusion) to recalculate risk scores.
            </p>
            <div className="mt-4 flex gap-2 justify-end">
              <button
                onClick={() => setShowRestartConfirm(false)}
                disabled={isRestarting}
                className="rounded-lg border border-[#CBD5E1] px-3 py-2 text-sm text-[#334155] hover:bg-[#F4F6F9] disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                onClick={handleRestartStream}
                disabled={isRestarting}
                className="rounded-lg bg-red-600 px-3 py-2 text-sm text-white hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isRestarting ? 'Restarting...' : 'Restart'}
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-cols-4 gap-4">
        <StatCard title="Total Active Accounts Monitored" value={snapshot?.totalAccounts ?? 0} tone="cyan" />
        <StatCard title="High Risk Flagged (30d)" value={snapshot?.highRisk30d ?? 0} tone="amber" />
        <StatCard title="Critical Risk" value={snapshot?.criticalNow ?? 0} tone="red" />
        <StatCard title="Average Portfolio Risk" value={Number((registry.reduce((sum, item) => sum + item.risk_score, 0) / Math.max(registry.length, 1)).toFixed(2))} tone="neutral" />
      </div>

      <div className="grid grid-cols-12 gap-4">
        <article className="col-span-4 rounded-xl border border-[#E2E6ED] bg-white p-4">
          <h3 className="mb-3 font-syne text-xl text-[#0F172A]">Risk Distribution</h3>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={distribution} dataKey="count" nameKey="tier" innerRadius={66} outerRadius={100}>
                  {distribution.map((entry) => (
                    <Cell key={entry.tier} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{ backgroundColor: '#FFFFFF', border: '1px solid #E2E6ED', borderRadius: '8px', color: '#0F172A' }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="col-span-8 rounded-xl border border-[#E2E6ED] bg-white p-4">
          <h3 className="mb-3 font-syne text-xl text-[#0F172A]">Average Risk Trend (Latest 12 Days)</h3>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={liveRiskTrend} margin={{ left: -22, right: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#F0F2F5" />
                <XAxis dataKey="day" tick={{ fill: '#94A3B8', fontSize: 12 }} stroke="#334155" tickFormatter={(value) => value.slice(5)} />
                <YAxis tick={{ fill: '#94A3B8', fontSize: 12 }} stroke="#334155" />
                <Tooltip
                  contentStyle={{ backgroundColor: '#FFFFFF', border: '1px solid #E2E6ED', borderRadius: '8px', color: '#0F172A' }}
                />
                <Line type="monotone" dataKey="avgRisk" stroke="#003366" strokeWidth={2.6} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="col-span-8 rounded-xl border border-[#E2E6ED] bg-white p-4">
          <h3 className="mb-3 font-syne text-xl text-[#0F172A]">Top 10 Highest Risk Accounts</h3>
          <div className="max-h-72 overflow-auto">
            <table className="w-full text-left text-sm">
              <thead className="sticky top-0 bg-[#F4F6F9] text-[11px] uppercase tracking-[0.14em] text-[#94A3B8]">
                <tr>
                  <th className="px-2 py-2">Customer</th>
                  <th className="px-2 py-2">Score</th>
                  <th className="px-2 py-2">Tier</th>
                  <th className="px-2 py-2">Updated</th>
                </tr>
              </thead>
              <tbody>
                {topAccounts.map((item) => {
                  const tier = normalizeRiskTier(item.risk_bucket, item.risk_score);
                  const rowTone = tier === 'VERY_CRITICAL' ? 'animate-critical-pulse' : '';
                  return (
                    <tr key={`${item.customer_id}-${item.timestamp}`} className={`border-t border-[#F0F2F5] ${rowTone}`}>
                      <td className="px-2 py-2 font-dm-mono">{item.customer_id}</td>
                      <td className="px-2 py-2 font-dm-mono">{item.risk_score.toFixed(2)}%</td>
                      <td className="px-2 py-2">
                        <RiskTierBadge tier={tier} />
                      </td>
                      <td className="px-2 py-2 text-[#94A3B8]">{new Date(item.timestamp ?? item.created_at ?? Date.now()).toLocaleString()}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </article>

        <article className="col-span-4 rounded-xl border border-[#E2E6ED] bg-white p-4">
          <h3 className="mb-3 font-syne text-xl text-[#0F172A]">Recent High-Risk Signals</h3>
          <ul className="space-y-2 text-sm">
            {topAccounts.slice(0, 6).map((item, idx) => {
              const tier = normalizeRiskTier(item.risk_bucket, item.risk_score);
              return (
                <li key={`${item.customer_id}_${idx}`} className="rounded-lg border border-[#E2E6ED] bg-white px-3 py-2">
                  <p className="font-dm-mono text-[#0F172A]">{item.customer_id}</p>
                  <p className="text-xs text-[#94A3B8]">Risk tier: {tier} · Score {item.risk_score.toFixed(2)}%</p>
                </li>
              );
            })}
          </ul>
        </article>

        <article className="col-span-12 rounded-xl border border-[#E2E6ED] bg-white p-4">
          <h3 className="mb-3 font-syne text-xl text-[#0F172A]">Branch-Level Average Risk</h3>
          <div className="grid grid-cols-6 gap-3">
            {branchRiskData.map((item) => (
              <div
                key={item.branch}
                className="rounded-lg border border-[#E2E6ED] p-3"
                style={{
                  backgroundColor: '#FFFFFF',
                }}
              >
                <p className="text-xs uppercase tracking-[0.12em] text-[#475569]">{item.branch}</p>
                <p className="mt-2 font-dm-mono text-2xl text-[#0F172A]">{item.score}</p>
              </div>
            ))}
          </div>
        </article>
      </div>
    </section>
  );
}



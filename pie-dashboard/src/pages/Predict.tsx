import { useState } from 'react';
import { pieApi } from '../api/pie';
import RiskTierBadge from '../components/RiskTierBadge';
import RiskScoreRing from '../components/RiskScoreRing';
import { normalizeRiskTier } from '../lib/risk';
import { RiskScoreResponse } from '../types';

export default function Predict() {
  const [customerId, setCustomerId] = useState('CUST-102394');
  const [transactionAmount, setTransactionAmount] = useState(1200);
  const [transactionReason, setTransactionReason] = useState('utility payment');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<RiskScoreResponse | null>(null);
  const [history, setHistory] = useState<Array<{ ts: string; score: number; tier: string }>>([]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const response = await pieApi.predict(
        customerId,
        {
          amount: Number(transactionAmount),
          transaction_reason: transactionReason,
        },
        true,
      );
      const normalized: RiskScoreResponse = {
        ...response,
        risk_bucket: normalizeRiskTier(response.risk_bucket, response.risk_score),
      };
      setResult(normalized);
      const tier = normalizeRiskTier(normalized.risk_bucket, normalized.risk_score);
      setHistory((prev) => [{ ts: new Date().toISOString(), score: normalized.risk_score, tier }, ...prev].slice(0, 10));
    } catch (err) {
      setError((err as Error).message ?? 'Prediction failed.');
    } finally {
      setLoading(false);
    }
  };

  const resultTier = result ? normalizeRiskTier(result.risk_bucket, result.risk_score) : 'LOW';

  return (
    <section className="grid grid-cols-[1.6fr_1fr] gap-5 p-6">
      <article className="rounded-xl border border-[#E2E6ED] bg-white p-5">
        <h2 className="font-syne text-2xl text-[#0F172A]">Prediction Engine</h2>
        <p className="text-sm text-[#94A3B8]">Run a quick risk check using basic transaction inputs.</p>

        <form className="mt-5 space-y-4" onSubmit={handleSubmit}>
          <div>
            <label className="mb-1 block text-xs uppercase tracking-[0.14em] text-[#475569]">Customer ID</label>
            <input
              required
              value={customerId}
              onChange={(e) => setCustomerId(e.target.value)}
              className="w-full rounded-lg border border-[#E2E6ED] bg-white px-3 py-2 font-dm-mono text-[#0F172A] outline-none ring-[#0057B8]/20 focus:ring"
            />
          </div>

          <div>
            <label className="mb-1 block text-xs uppercase tracking-[0.14em] text-[#475569]">Transaction Amount</label>
            <input
              required
              min={0}
              type="number"
              step="0.01"
              value={transactionAmount}
              onChange={(e) => setTransactionAmount(Number(e.target.value))}
              className="w-full rounded-lg border border-[#E2E6ED] bg-white px-3 py-2 font-dm-mono text-[#0F172A] outline-none ring-[#0057B8]/20 focus:ring"
            />
          </div>

          <div>
            <label className="mb-1 block text-xs uppercase tracking-[0.14em] text-[#475569]">Transaction Reason</label>
            <input
              required
              value={transactionReason}
              onChange={(e) => setTransactionReason(e.target.value)}
              placeholder="e.g. rent, utility payment, electronics"
              className="w-full rounded-lg border border-[#E2E6ED] bg-white px-3 py-2 text-[#0F172A] outline-none ring-[#0057B8]/20 focus:ring"
            />
          </div>

          {error && <div className="rounded-lg border border-[#FECACA] bg-[#FEF2F2] p-3 text-sm text-[#7B0D0D]">{error}</div>}

          <button
            disabled={loading}
            className="rounded-lg bg-[#003366] px-4 py-2 text-sm font-semibold text-white disabled:opacity-40"
            type="submit"
          >
            {loading ? 'Running Inference...' : 'Run Prediction'}
          </button>
        </form>

        <div className="mt-5 rounded-lg border border-[#E2E6ED] bg-white p-4">
          <h3 className="mb-3 text-sm uppercase tracking-[0.14em] text-[#475569]">Prediction History (Session)</h3>
          <div className="max-h-44 overflow-y-auto">
            <table className="w-full text-left text-xs">
              <thead className="text-[#94A3B8]">
                <tr>
                  <th className="py-1">Timestamp</th>
                  <th className="py-1">Score</th>
                  <th className="py-1">Tier</th>
                </tr>
              </thead>
              <tbody>
                {history.map((entry) => (
                  <tr key={entry.ts} className="border-t border-[#E2E6ED] text-[#334155]">
                    <td className="py-1">{new Date(entry.ts).toLocaleTimeString()}</td>
                    <td className="py-1 font-dm-mono">{entry.score.toFixed(2)}%</td>
                    <td className="py-1">{entry.tier}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </article>

      <article className="rounded-xl border border-[#E2E6ED] bg-white p-5">
        <h3 className="font-syne text-xl text-[#0F172A]">Inference Output</h3>
        {!result ? (
          <p className="mt-12 text-center text-sm text-[#94A3B8]">Submit the form to render probability, tier, and model pipeline scores.</p>
        ) : (
          <div className="mt-4 space-y-4">
            <div className="flex justify-center">
              <RiskScoreRing score={result.risk_score} tier={resultTier} />
            </div>
            <div className="flex justify-center">
              <RiskTierBadge tier={resultTier} />
            </div>
            <div className="rounded-lg border border-[#E2E6ED] bg-white p-3">
              <p className="text-xs uppercase tracking-[0.14em] text-[#94A3B8]">Risk Summary</p>
              <p className="mt-1 text-sm text-[#0F172A]">
                {resultTier === 'VERY_CRITICAL'
                  ? 'Very critical risk detected. Immediate manual review is recommended.'
                  : resultTier === 'CRITICAL'
                    ? 'Critical risk detected. Prioritize case review in operations queue.'
                    : resultTier === 'HIGH'
                      ? 'High risk detected. Continue close monitoring and follow-up.'
                      : 'Low risk profile. Continue routine monitoring.'}
              </p>
            </div>
          </div>
        )}
      </article>
    </section>
  );
}



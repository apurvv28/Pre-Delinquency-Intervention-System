import { useState } from 'react';
import { pieApi } from '../api/pie';
import RiskTierBadge from '../components/RiskTierBadge';
import RiskScoreRing from '../components/RiskScoreRing';
import { normalizeRiskTier } from '../lib/risk';
import { RiskScoreResponse } from '../types';

export default function Predict() {
  const [customerId, setCustomerId] = useState('CUST-0001');
  const [transactionAmount, setTransactionAmount] = useState(1200);
  const [transactionReason, setTransactionReason] = useState('utilities');
  const [currentBalance, setCurrentBalance] = useState(25000);
  const [daysSinceLastPayment, setDaysSinceLastPayment] = useState(2);
  const [transactionType, setTransactionType] = useState('PURCHASE');
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
          merchant_category: transactionReason,
          event_type: transactionType,
          current_balance: Number(currentBalance),
          days_since_last_payment: Number(daysSinceLastPayment),
          previous_declines_24h: 0,
          is_international: false,
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
        <p className="text-sm text-[#94A3B8]">Run a risk check using the sequential LightGBM -&gt; XGBoost pipeline and review the final risk score only.</p>

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

          <div>
            <label className="mb-1 block text-xs uppercase tracking-[0.14em] text-[#475569]">Transaction Type</label>
            <select
              value={transactionType}
              onChange={(e) => setTransactionType(e.target.value)}
              className="w-full rounded-lg border border-[#E2E6ED] bg-white px-3 py-2 text-[#0F172A] outline-none ring-[#0057B8]/20 focus:ring"
            >
              <option value="PURCHASE">PURCHASE</option>
              <option value="CARD_SWIPE">CARD_SWIPE</option>
              <option value="ONLINE_PURCHASE">ONLINE_PURCHASE</option>
              <option value="PAYMENT">PAYMENT</option>
              <option value="INCOME_CREDIT">INCOME_CREDIT</option>
              <option value="PARTIAL_PAYMENT">PARTIAL_PAYMENT</option>
              <option value="SETTLEMENT_OFFER">SETTLEMENT_OFFER</option>
            </select>
          </div>

          <div>
            <label className="mb-1 block text-xs uppercase tracking-[0.14em] text-[#475569]">Current Balance</label>
            <input
              required
              min={0}
              type="number"
              step="0.01"
              value={currentBalance}
              onChange={(e) => setCurrentBalance(Number(e.target.value))}
              className="w-full rounded-lg border border-[#E2E6ED] bg-white px-3 py-2 font-dm-mono text-[#0F172A] outline-none ring-[#0057B8]/20 focus:ring"
            />
          </div>

          <div>
            <label className="mb-1 block text-xs uppercase tracking-[0.14em] text-[#475569]">Days Since Last Payment</label>
            <input
              required
              min={0}
              type="number"
              step="1"
              value={daysSinceLastPayment}
              onChange={(e) => setDaysSinceLastPayment(Number(e.target.value))}
              className="w-full rounded-lg border border-[#E2E6ED] bg-white px-3 py-2 font-dm-mono text-[#0F172A] outline-none ring-[#0057B8]/20 focus:ring"
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
          <p className="mt-12 text-center text-sm text-[#94A3B8]">Submit the form to render the final risk probability and tier.</p>
        ) : (
          <div className="mt-4 space-y-4">
            <div className="flex justify-center">
              <RiskScoreRing score={result.risk_score} tier={resultTier} />
            </div>
            <div className="flex justify-center">
              <RiskTierBadge tier={resultTier} />
            </div>
            <div className="rounded-lg border border-[#E2E6ED] bg-[#F4F6F9] p-3 text-sm text-[#475569]">
              <p className="text-[11px] uppercase tracking-[0.14em] text-[#94A3B8]">Final Risk Score</p>
              <p className="mt-1 font-dm-mono text-lg text-[#0F172A]">{result.final_model_risk_score != null ? `${result.final_model_risk_score.toFixed(2)}%` : `${result.risk_score.toFixed(2)}%`}</p>
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



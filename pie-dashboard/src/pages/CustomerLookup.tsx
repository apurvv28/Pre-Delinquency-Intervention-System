import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import ReactMarkdown from 'react-markdown';
import { pieApi } from '../api/pie';
import RiskBadge from '../components/RiskBadge';
import ScoreRing from '../components/ScoreRing';
import LoadingSpinner from '../components/LoadingSpinner';
import { CustomerProfileSummary, CustomerTransactionPoint } from '../types';

export default function CustomerLookup() {
  const { data: customers = [], isLoading: customersLoading } = useQuery({
    queryKey: ['customers'],
    queryFn: pieApi.getCustomers,
  });

  const [searchInput, setSearchInput] = useState('');
  const [selectedCustomerId, setSelectedCustomerId] = useState('');

  useEffect(() => {
    if (!selectedCustomerId && customers.length > 0) {
      setSelectedCustomerId(customers[0].customer_id);
      setSearchInput(customers[0].customer_id);
    }
  }, [customers, selectedCustomerId]);

  const selectedCustomer = useMemo<CustomerProfileSummary | null>(() => {
    if (!selectedCustomerId) return null;
    return customers.find((customer) => customer.customer_id.toLowerCase() === selectedCustomerId.toLowerCase()) ?? null;
  }, [customers, selectedCustomerId]);

  const { data, isLoading: loading, error: queryError } = useQuery({
    queryKey: ['customer', selectedCustomerId],
    queryFn: async () => {
      const [scoreData, historyData, txData] = await Promise.all([
        pieApi.getScore(selectedCustomerId),
        pieApi.getHistory(selectedCustomerId),
        pieApi.getCustomerTransactions(selectedCustomerId),
      ]);

      let explanationData = null;
      try {
        explanationData = await pieApi.getExplanation(selectedCustomerId);
      } catch (err) {
        console.warn('Failed to fetch XAI explanation:', err);
      }

      return {
        currentScore: scoreData,
        history: historyData || [],
        transactions: txData.transactions || [],
        profile: txData.profile,
        explanation: explanationData,
      };
    },
    enabled: !!selectedCustomerId,
    retry: false,
    refetchInterval: 5000,
  });

  const currentScore = data?.currentScore || null;
  const history = data?.history || [];
  const transactions = data?.transactions || [];
  const profile = data?.profile || selectedCustomer;

  const errorObj = queryError as { response?: { data?: { detail?: string } } } | undefined;
  const error = queryError
    ? errorObj?.response?.data?.detail || (queryError as Error).message || 'Customer not found or error occurred.'
    : '';

  const handleSearch = (event: React.FormEvent) => {
    event.preventDefault();
    const next = searchInput.trim();
    if (!next) return;
    setSelectedCustomerId(next);
  };

  const spendData = transactions.map((transaction: CustomerTransactionPoint) => ({
    transaction_index: transaction.transaction_index,
    amount: transaction.amount,
    balance_after: transaction.balance_after,
    transaction_time: transaction.transaction_time,
    risk_score: transaction.risk_score ?? null,
    is_seeded: transaction.is_seeded,
  }));

  const totalSpend = transactions.reduce((sum, item) => sum + item.amount, 0);
  const averageSpend = transactions.length ? totalSpend / transactions.length : 0;
  const liveTransactions = transactions.filter((item) => !item.is_seeded).length;

  return (
    <section className="space-y-5 p-6">
      <div className="flex items-end justify-between gap-4">
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-[#0057B8]">Customer Search</p>
          <h2 className="mt-2 font-syne text-3xl text-[#0F172A]">Customer Spend Graph and Risk History</h2>
          <p className="mt-2 max-w-3xl text-sm text-[#94A3B8]">
            Search a seeded loan taker to review 100 preloaded transactions, the current spending graph, and the live model output once the stream continues.
          </p>
        </div>
        <div className="rounded-xl border border-[#E2E6ED] bg-white px-4 py-3 text-right">
          <p className="text-[11px] uppercase tracking-[0.16em] text-[#94A3B8]">Seed Coverage</p>
          <p className="mt-1 font-dm-mono text-lg text-[#0F172A]">50 customers · 100 seeded transactions each</p>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-4">
        <aside className="col-span-3 rounded-xl border border-[#E2E6ED] bg-white p-4">
          <form onSubmit={handleSearch} className="space-y-3">
            <label className="block text-xs uppercase tracking-[0.16em] text-[#94A3B8]" htmlFor="customer-search">
              Search customer ID
            </label>
            <input
              id="customer-search"
              list="customer-list"
              value={searchInput}
              onChange={(event) => setSearchInput(event.target.value)}
              placeholder="CUST-0001"
              className="w-full rounded-lg border border-[#E2E6ED] bg-white px-3 py-2.5 font-dm-mono text-sm text-[#0F172A] outline-none ring-[#0057B8]/20 focus:ring"
            />
            <datalist id="customer-list">
              {customers.map((customer) => (
                <option key={customer.customer_id} value={customer.customer_id}>
                  {customer.name} · {customer.branch}
                </option>
              ))}
            </datalist>
            <button
              type="submit"
              className="w-full rounded-lg bg-[#003366] px-4 py-2.5 font-semibold text-white transition hover:bg-[#002244]"
            >
              Load customer
            </button>
          </form>

          <div className="mt-5 max-h-[540px] overflow-auto pr-1">
            {customersLoading ? (
              <div className="py-8">
                <LoadingSpinner />
              </div>
            ) : (
              <div className="space-y-2">
                {customers.map((customer) => {
                  const active = customer.customer_id === selectedCustomerId;
                  return (
                    <button
                      key={customer.customer_id}
                      type="button"
                      onClick={() => {
                        setSelectedCustomerId(customer.customer_id);
                        setSearchInput(customer.customer_id);
                      }}
                      className={`w-full rounded-xl border px-3 py-3 text-left transition ${
                        active
                          ? 'border-[#BFD4EA] bg-[#EFF6FF]'
                          : 'border-[#E2E6ED] bg-[#F4F6F9] hover:border-[#CBD5E1] hover:bg-white'
                      }`}
                    >
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <p className="font-dm-mono text-sm text-[#0F172A]">{customer.customer_id}</p>
                          <p className="text-xs text-[#94A3B8]">{customer.name}</p>
                        </div>
                        <RiskBadge bucket={(customer.latest_risk_bucket ?? 'LOW_RISK') as 'LOW_RISK' | 'HIGH_RISK' | 'CRITICAL' | 'VERY_CRITICAL'} />
                      </div>
                      <p className="mt-2 text-[11px] uppercase tracking-[0.14em] text-[#94A3B8]">
                        {customer.branch} · {customer.loan_type}
                      </p>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        </aside>

        <div className="col-span-9 space-y-4">
          {error && <div className="rounded-lg border border-[#FECACA] bg-[#FEF2F2] px-4 py-3 text-sm text-[#7B0D0D]">{error}</div>}

          {loading && <LoadingSpinner />}

          {!loading && currentScore && profile && (
            <div className="grid grid-cols-12 gap-4">
              {profile.pre_npa && (
                <article className="col-span-12 rounded-xl border border-[#FCA5A5] bg-[#FEF2F2] px-4 py-3 text-sm text-[#7F1D1D]">
                  Legal escalation active: this customer is currently tagged PRE_NPA and in collections workflow.
                </article>
              )}

              <article className="col-span-4 rounded-xl border border-[#E2E6ED] bg-white p-5">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-xs uppercase tracking-[0.16em] text-[#94A3B8]">Customer profile</p>
                    <h3 className="mt-1 font-syne text-2xl text-[#0F172A]">{profile.name}</h3>
                    <p className="mt-1 text-sm text-[#94A3B8]">{profile.branch} · {profile.loan_type}</p>
                  </div>
                  <RiskBadge bucket={(currentScore.risk_bucket as 'LOW_RISK' | 'HIGH_RISK' | 'CRITICAL' | 'VERY_CRITICAL')} />
                </div>

                <div className="mt-5 flex flex-col items-center rounded-2xl border border-[#E2E6ED] bg-[#F4F6F9] py-4">
                  <ScoreRing score={currentScore.risk_score} bucket={currentScore.risk_bucket as 'LOW_RISK' | 'HIGH_RISK' | 'CRITICAL' | 'VERY_CRITICAL'} />
                </div>

                <dl className="mt-5 grid grid-cols-2 gap-3 text-sm">
                  <div className="rounded-lg border border-[#E2E6ED] bg-[#F4F6F9] p-3">
                    <dt className="text-[11px] uppercase tracking-[0.12em] text-[#94A3B8]">Transactions</dt>
                    <dd className="mt-1 font-dm-mono text-lg text-[#0F172A]">{transactions.length}</dd>
                  </div>
                  <div className="rounded-lg border border-[#E2E6ED] bg-[#F4F6F9] p-3">
                    <dt className="text-[11px] uppercase tracking-[0.12em] text-[#94A3B8]">Live stream</dt>
                    <dd className="mt-1 font-dm-mono text-lg text-[#0057B8]">{liveTransactions}</dd>
                  </div>
                  <div className="rounded-lg border border-[#E2E6ED] bg-[#F4F6F9] p-3">
                    <dt className="text-[11px] uppercase tracking-[0.12em] text-[#94A3B8]">Total spend</dt>
                    <dd className="mt-1 font-dm-mono text-lg text-[#0F172A]">₹{totalSpend.toFixed(0)}</dd>
                  </div>
                  <div className="rounded-lg border border-[#E2E6ED] bg-[#F4F6F9] p-3">
                    <dt className="text-[11px] uppercase tracking-[0.12em] text-[#94A3B8]">Avg. spend</dt>
                    <dd className="mt-1 font-dm-mono text-lg text-[#0F172A]">₹{averageSpend.toFixed(0)}</dd>
                  </div>
                </dl>
              </article>

              <article className="col-span-8 rounded-xl border border-[#E2E6ED] bg-white p-5">
                <div className="flex items-center justify-between gap-3 border-b border-[#E2E6ED] pb-3">
                  <div>
                    <p className="text-xs uppercase tracking-[0.16em] text-[#94A3B8]">Spend graph</p>
                    <h3 className="mt-1 font-syne text-2xl text-[#0F172A]">Transaction amount timeline</h3>
                  </div>
                  <p className="text-xs uppercase tracking-[0.14em] text-[#94A3B8]">Seeded first 100, then live</p>
                </div>
                <div className="mt-4 h-80 w-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={spendData} margin={{ left: 4, right: 8, top: 8, bottom: 8 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                      <XAxis dataKey="transaction_index" tick={{ fill: '#94a3b8', fontSize: 12 }} stroke="#334155" />
                      <YAxis tick={{ fill: '#94a3b8', fontSize: 12 }} stroke="#334155" />
                      <Tooltip
                        contentStyle={{ backgroundColor: '#FFFFFF', border: '1px solid #E2E6ED', borderRadius: '8px', color: '#0F172A' }}
                        labelFormatter={(label) => `Transaction #${label}`}
                      />
                      <ReferenceLine x={100} stroke="#f59e0b" strokeDasharray="5 5" />
                      <Line type="monotone" dataKey="amount" stroke="#003366" strokeWidth={2.5} dot={false} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </article>

              <article className="col-span-12 rounded-xl border border-[#E2E6ED] bg-white p-5">
                <div className="grid grid-cols-12 gap-4">
                  <div className="col-span-5 rounded-xl border border-[#E2E6ED] bg-[#F4F6F9] p-4">
                    <h3 className="border-b border-[#E2E6ED] pb-2 font-syne text-xl text-[#0F172A]">Current risk profile</h3>
                    <div className="mt-4">
                      <div className="text-sm flex justify-between py-2">
                        <span className="text-[#94A3B8]">Last Updated</span>
                        <span className="font-mono text-xs text-[#475569]">{new Date(currentScore.timestamp || currentScore.created_at || '').toLocaleString()}</span>
                      </div>
                      <div className="text-sm flex justify-between py-2">
                        <span className="text-[#94A3B8]">Relationship manager</span>
                        <span className="text-[#475569]">{profile.relationship_manager}</span>
                      </div>
                    </div>
                  </div>

                  <div className="col-span-7 rounded-xl border border-[#E2E6ED] bg-[#F4F6F9] p-4">
                    <h3 className="border-b border-[#E2E6ED] pb-2 font-syne text-xl text-[#0F172A]">AI pre-default analysis</h3>
                    <div className="markdown-content mt-4 text-sm leading-6 text-[#475569]">
                      {data?.explanation ? <ReactMarkdown>{data.explanation.explanation}</ReactMarkdown> : <p>No explanation available yet.</p>}
                    </div>
                  </div>
                </div>
              </article>

              <article className="col-span-12 rounded-xl border border-[#E2E6ED] bg-white p-5">
                <h3 className="border-b border-[#E2E6ED] pb-2 font-syne text-xl text-[#0F172A]">Risk history</h3>
                <div className="mt-4 h-64 w-full">
                  {history.length > 0 ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={[...history].reverse()}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                        <XAxis dataKey="created_at" tickFormatter={(value) => new Date(value).toLocaleDateString()} tick={{ fill: '#94a3b8', fontSize: 12 }} stroke="#334155" />
                        <YAxis domain={[0, 100]} tick={{ fill: '#94a3b8', fontSize: 12 }} stroke="#334155" />
                        <Tooltip contentStyle={{ backgroundColor: '#FFFFFF', border: '1px solid #E2E6ED', borderRadius: '8px', color: '#0F172A' }} />
                        <ReferenceLine y={80} stroke="#ef4444" strokeDasharray="3 3" />
                        <Line type="monotone" dataKey="risk_score" stroke="#f59e0b" strokeWidth={2.5} dot={{ fill: '#f59e0b', r: 4 }} />
                      </LineChart>
                    </ResponsiveContainer>
                  ) : (
                    <p className="text-sm text-[#94A3B8]">No historical scores found.</p>
                  )}
                </div>
              </article>

              <article className="col-span-12 rounded-xl border border-[#E2E6ED] bg-white p-5">
                <h3 className="border-b border-[#E2E6ED] pb-2 font-syne text-xl text-[#0F172A]">Recent transactions</h3>
                <div className="mt-4 overflow-x-auto">
                  <table className="w-full text-left text-sm">
                    <thead className="bg-[#F4F6F9] text-xs uppercase tracking-[0.12em] text-[#94A3B8]">
                      <tr>
                        <th className="px-4 py-3">#</th>
                        <th className="px-4 py-3">Amount</th>
                        <th className="px-4 py-3">Balance</th>
                        <th className="px-4 py-3">DPD</th>
                        <th className="px-4 py-3">Merchant</th>
                        <th className="px-4 py-3">Time</th>
                      </tr>
                    </thead>
                    <tbody>
                      {[...transactions].slice(-12).reverse().map((item) => (
                        <tr key={`${item.transaction_index}-${item.transaction_time}`} className={`border-b border-[#E2E6ED] ${item.is_seeded ? 'text-[#475569]' : 'text-[#0057B8]'}`}>
                          <td className="px-4 py-3 font-dm-mono">{item.transaction_index}</td>
                          <td className="px-4 py-3 font-dm-mono">₹{item.amount.toFixed(0)}</td>
                          <td className="px-4 py-3 font-dm-mono">₹{item.balance_after.toFixed(0)}</td>
                          <td className="px-4 py-3 font-dm-mono">{item.days_since_last_payment}</td>
                          <td className="px-4 py-3">{item.merchant_category}</td>
                          <td className="px-4 py-3 font-mono text-xs text-[#94A3B8]">{new Date(item.transaction_time).toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </article>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}



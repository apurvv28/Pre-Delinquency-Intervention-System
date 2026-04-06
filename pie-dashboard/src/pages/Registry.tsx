import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import DataTable, { TableColumn } from '../components/DataTable';
import RiskTierBadge from '../components/RiskTierBadge';
import SlideOverDrawer from '../components/SlideOverDrawer';
import { pieApi } from '../api/pie';
import { normalizeRiskTier, riskTierStyles } from '../lib/risk';
import { CustomerProfileSummary } from '../types';

interface RegistryRow {
  customerId: string;
  name: string;
  occupation: string;
  loanType: string;
  loanAmount: number;
  spendingCulture: string;
  topSpendingReasons: string[];
  avgSpendRiskScore: number;
  branch: string;
  riskScore: number;
  riskTier: ReturnType<typeof normalizeRiskTier>;
  lastUpdated: string;
  monthlyIncome: number;
  accountAgeMonths: number;
  relationshipManager: string;
}

function exportCsv(rows: RegistryRow[]) {
  const header = ['Customer ID', 'Name', 'Occupation', 'Loan Type', 'Loan Amount', 'Spending Culture', 'Top Spending Reasons', 'Avg Spend Risk Score', 'Risk Score', 'Risk Tier', 'Branch', 'Last Updated'];
  const body = rows.map((row) => [
    row.customerId,
    row.name,
    row.occupation,
    row.loanType,
    row.loanAmount.toFixed(2),
    row.spendingCulture,
    row.topSpendingReasons.join(' | '),
    row.avgSpendRiskScore.toFixed(2),
    row.riskScore.toFixed(2),
    row.riskTier,
    row.branch,
    row.lastUpdated,
  ]);
  const csv = [header, ...body].map((line) => line.join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `pie_registry_${Date.now()}.csv`;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
}

export default function Registry() {
  const { data: customers = [] } = useQuery({ queryKey: ['registryRows'], queryFn: pieApi.getCustomers });
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [activeRow, setActiveRow] = useState<RegistryRow | null>(null);

  const rows = useMemo<RegistryRow[]>(
    () =>
      customers
        .filter((row) => row.latest_risk_score != null && row.latest_risk_bucket != null)
        .map((row: CustomerProfileSummary) => {
          const score = row.latest_risk_score ?? 0;
          const tier = normalizeRiskTier(row.latest_risk_bucket ?? 'LOW', score);
          return {
            customerId: row.customer_id,
            name: row.name,
            occupation: row.occupation ?? 'Unknown',
            loanType: row.loan_type,
            loanAmount: Number(row.loan_amount ?? 0),
            spendingCulture: row.spending_culture ?? 'Balanced',
            topSpendingReasons: row.top_spending_reasons ?? [],
            avgSpendRiskScore: Number(row.avg_spend_risk_score ?? 0),
            branch: row.branch,
            riskScore: score,
            riskTier: tier,
            lastUpdated: row.latest_transaction_time ?? new Date().toISOString(),
            monthlyIncome: row.monthly_income,
            accountAgeMonths: row.account_age_months,
            relationshipManager: row.relationship_manager,
          };
        }),
    [customers],
  );

  const columns: TableColumn<RegistryRow>[] = [
    { key: 'customerId', header: 'Customer ID', width: '15%' },
    { key: 'name', header: 'Name', width: '14%' },
    { key: 'occupation', header: 'Occupation', width: '12%' },
    { key: 'loanType', header: 'Loan Type', width: '12%' },
    {
      key: 'loanAmount',
      header: 'Loan Amount',
      render: (row) => <span className="font-dm-mono">{row.loanAmount.toLocaleString()}</span>,
      width: '12%',
    },
    { key: 'spendingCulture', header: 'Spending Culture', width: '12%' },
    {
      key: 'riskScore',
      header: 'Risk Score',
      render: (row) => <span className="font-dm-mono">{row.riskScore.toFixed(2)}%</span>,
      width: '10%',
    },
    {
      key: 'riskTier',
      header: 'Risk Tier',
      render: (row) => <RiskTierBadge tier={row.riskTier} />,
      width: '12%',
    },
    { key: 'lastUpdated', header: 'Last Updated', width: '14%' },
    { key: 'branch', header: 'Branch', width: '12%' },
  ];

  return (
    <section className="space-y-4 p-6">
      <header className="flex items-center justify-between">
        <div>
          <h2 className="font-syne text-2xl text-[#0F172A]">Customer Risk Registry</h2>
          <p className="text-sm text-[#94A3B8]">Sortable, filterable operational table with audit-ready actions.</p>
        </div>
        <div className="flex gap-2">
          <button className="rounded-lg border border-[#CBD5E1] px-3 py-2 text-xs text-[#334155]" onClick={() => exportCsv(rows)}>
            Export CSV
          </button>
        </div>
      </header>

      <DataTable
        caption="Registry table"
        columns={columns}
        rows={rows}
        pageSize={12}
        onRowClick={(row) => {
          setActiveRow(row);
          setDrawerOpen(true);
        }}
        rowClassName={(row) => {
          const tone = riskTierStyles(row.riskTier).row;
          return `${tone} ${row.riskTier === 'VERY_CRITICAL' ? 'animate-critical-pulse' : ''}`;
        }}
      />

      <SlideOverDrawer open={drawerOpen} title={activeRow ? `${activeRow.name} · ${activeRow.customerId}` : 'Customer Detail'} onClose={() => setDrawerOpen(false)}>
        {activeRow && (
          <div className="space-y-4 text-sm text-[#334155]">
            <div className="grid grid-cols-2 gap-3">
              <div className="rounded-lg border border-[#E2E6ED] bg-white p-3">Risk Score: <span className="font-dm-mono">{activeRow.riskScore.toFixed(2)}%</span></div>
              <div className="rounded-lg border border-[#E2E6ED] bg-white p-3">Monthly Income: <span className="font-dm-mono">{activeRow.monthlyIncome}</span></div>
              <div className="rounded-lg border border-[#E2E6ED] bg-white p-3">Loan Amount: <span className="font-dm-mono">{activeRow.loanAmount.toLocaleString()}</span></div>
              <div className="rounded-lg border border-[#E2E6ED] bg-white p-3">Occupation: <span className="font-dm-mono">{activeRow.occupation}</span></div>
              <div className="rounded-lg border border-[#E2E6ED] bg-white p-3">Spending Culture: <span className="font-dm-mono">{activeRow.spendingCulture}</span></div>
              <div className="rounded-lg border border-[#E2E6ED] bg-white p-3">Avg Spend Risk Score: <span className="font-dm-mono">{activeRow.avgSpendRiskScore.toFixed(2)}</span></div>
              <div className="rounded-lg border border-[#E2E6ED] bg-white p-3">Account Age: <span className="font-dm-mono">{activeRow.accountAgeMonths} months</span></div>
              <div className="rounded-lg border border-[#E2E6ED] bg-white p-3">RM: <span className="font-dm-mono">{activeRow.relationshipManager}</span></div>
            </div>
            <div className="rounded-lg border border-[#E2E6ED] bg-white p-3">
              <p className="mb-2 text-xs uppercase tracking-[0.14em] text-[#475569]">Top Spending Reasons</p>
              <p className="text-sm text-[#1E293B]">{activeRow.topSpendingReasons.length ? activeRow.topSpendingReasons.join(', ') : 'Not available'}</p>
            </div>
            <label className="block text-xs uppercase tracking-[0.14em] text-[#475569]">Manual Override Note</label>
            <textarea className="h-28 w-full rounded-lg border border-[#E2E6ED] bg-white p-3 text-sm" placeholder="Record override reason for compliance trail." />
            <button className="rounded-lg border border-[#BFD4EA] bg-[#EFF6FF] px-3 py-2 text-xs text-[#0057B8]">Save Override</button>
          </div>
        )}
      </SlideOverDrawer>
    </section>
  );
}



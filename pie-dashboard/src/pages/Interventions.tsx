import { useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { pieApi } from '../api/pie';
import { useAuth } from '../hooks/useAuth';

const tabs = [
  { key: 'PENDING', label: 'Pending' },
  { key: 'SENT', label: 'Sent' },
  { key: 'FAILED', label: 'Failed' },
  { key: 'AWAITING_CHECKER', label: 'Escalated' },
] as const;

export default function Interventions() {
  const { session } = useAuth();
  const adminId = session?.user.email || session?.user.name || 'admin';
  const queryClient = useQueryClient();

  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]['key']>('PENDING');
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [previewId, setPreviewId] = useState<string | null>(null);
  const [rejectReason, setRejectReason] = useState('Rejected by admin');

  const queueQuery = useQuery({
    queryKey: ['interventionQueue', activeTab],
    queryFn: () => pieApi.getInterventionQueue(activeTab),
    refetchInterval: 10000,
  });

  const historyQuery = useQuery({
    queryKey: ['interventionHistory'],
    queryFn: () => pieApi.getInterventionHistory(),
    refetchInterval: 20000,
  });

  const previewQuery = useQuery({
    queryKey: ['interventionPreview', previewId],
    queryFn: () => pieApi.getInterventionPreview(previewId || ''),
    enabled: !!previewId,
  });

  const orchestrateMutation = useMutation({
    mutationFn: () => pieApi.runInterventionOrchestrator(adminId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['interventionQueue'] });
      queryClient.invalidateQueries({ queryKey: ['interventionHistory'] });
    },
  });

  const approveMutation = useMutation({
    mutationFn: (payload: { id: string; checkerId?: string; comment?: string }) =>
      pieApi.approveIntervention({ interventionId: payload.id, admin_id: adminId, checker_id: payload.checkerId, comment: payload.comment }),
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ['interventionQueue'] });
      queryClient.invalidateQueries({ queryKey: ['interventionHistory'] });
      if (result?.delivery_status === 'SENT' || result?.status === 'SENT') {
        window.alert('Mail is sent to customer.');
        return;
      }
      if (result?.status === 'QUEUED') {
        window.alert('Mail has been scheduled and will be sent at the configured time.');
        return;
      }
      if (result?.status === 'AWAITING_CHECKER') {
        window.alert('Maker step submitted. Checker approval is required before mail is sent.');
      }
    },
    onError: (error: unknown) => {
      const maybeError = error as { response?: { data?: { detail?: string } }; message?: string };
      const detail = String(maybeError?.response?.data?.detail || maybeError?.message || '');
      const normalized = detail.toLowerCase();
      if (normalized.includes('already') || normalized.includes('sent') || normalized.includes('approved')) {
        window.alert('Mail is already sent to customer.');
        queryClient.invalidateQueries({ queryKey: ['interventionQueue'] });
        queryClient.invalidateQueries({ queryKey: ['interventionHistory'] });
        return;
      }
      window.alert(detail || 'Unable to approve intervention right now.');
    },
  });

  const rejectMutation = useMutation({
    mutationFn: (payload: { id: string; reason: string }) =>
      pieApi.rejectIntervention({ interventionId: payload.id, admin_id: adminId, reason: payload.reason }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['interventionQueue'] });
      queryClient.invalidateQueries({ queryKey: ['interventionHistory'] });
    },
  });

  const bulkApproveMutation = useMutation({
    mutationFn: () => pieApi.bulkApproveInterventions({ ids: selectedIds, admin_id: adminId }),
    onSuccess: () => {
      setSelectedIds([]);
      queryClient.invalidateQueries({ queryKey: ['interventionQueue'] });
      queryClient.invalidateQueries({ queryKey: ['interventionHistory'] });
    },
  });

  const testMailMutation = useMutation({
    mutationFn: (id: string) => pieApi.sendInterventionTestEmail({ interventionId: id, admin_id: adminId, admin_email: session?.user.email || 'admin@example.com' }),
  });

  const queueItems = queueQuery.data || [];

  const formatSla = (iso: string | null) => {
    if (!iso) return { text: '-', className: 'text-[#64748B]' };
    const due = new Date(iso).getTime();
    const diffMs = due - Date.now();
    const hours = Math.floor(Math.abs(diffMs) / (1000 * 60 * 60));
    const mins = Math.floor((Math.abs(diffMs) % (1000 * 60 * 60)) / (1000 * 60));
    if (diffMs < 0) {
      return { text: `Breached ${hours}h ${mins}m`, className: 'text-[#B91C1C]' };
    }
    if (diffMs < 1000 * 60 * 60 * 6) {
      return { text: `${hours}h ${mins}m left`, className: 'text-[#B45309]' };
    }
    return { text: `${hours}h ${mins}m left`, className: 'text-[#15803D]' };
  };

  const selectedItemSet = useMemo(() => new Set(selectedIds), [selectedIds]);

  const toggleSelected = (id: string) => {
    setSelectedIds((current) => (current.includes(id) ? current.filter((item) => item !== id) : [...current, id]));
  };

  return (
    <section className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs uppercase tracking-[0.16em] text-[#0057B8]">Intervention System</p>
          <h2 className="mt-1 font-syne text-3xl text-[#0F172A]">Queue & Maker-Checker Operations</h2>
        </div>
        <button
          onClick={() => orchestrateMutation.mutate()}
          className="rounded-lg bg-[#003366] px-4 py-2 text-sm font-semibold text-white hover:bg-[#002244]"
          disabled={orchestrateMutation.isPending}
        >
          {orchestrateMutation.isPending ? 'Scanning...' : 'Run Orchestrator'}
        </button>
      </div>

      <div>
        <a
          href={`${import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000'}/api/v1/interventions/history/export.csv`}
          target="_blank"
          rel="noreferrer"
          className="inline-flex rounded-lg border border-[#CBD5E1] bg-white px-3 py-1.5 text-sm font-semibold text-[#334155] hover:bg-[#F8FAFC]"
        >
          Export History CSV
        </a>
      </div>

      <article className="rounded-xl border border-[#E2E6ED] bg-white p-4">
        <div className="flex gap-2">
          {tabs.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`rounded-lg px-3 py-1.5 text-sm font-semibold ${
                activeTab === tab.key ? 'bg-[#0057B8] text-white' : 'bg-[#F1F5F9] text-[#475569]'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div className="mt-4 overflow-x-auto rounded-lg border border-[#E2E6ED]">
          <table className="w-full text-left text-sm">
            <thead className="bg-[#F8FAFC] text-xs uppercase tracking-[0.12em] text-[#64748B]">
              <tr>
                <th className="px-3 py-2">Select</th>
                <th className="px-3 py-2">Customer</th>
                <th className="px-3 py-2">Score</th>
                <th className="px-3 py-2">Tier</th>
                <th className="px-3 py-2">Engine</th>
                <th className="px-3 py-2">SLA Due</th>
                <th className="px-3 py-2">Status</th>
                <th className="px-3 py-2">Actions</th>
              </tr>
            </thead>
            <tbody>
              {queueItems.map((item) => (
                <tr key={item.id} className="border-t border-[#E2E6ED]">
                  <td className="px-3 py-2">
                    <input type="checkbox" checked={selectedItemSet.has(item.id)} onChange={() => toggleSelected(item.id)} />
                  </td>
                  <td className="px-3 py-2 font-dm-mono">{item.customer_name}</td>
                  <td className="px-3 py-2 font-dm-mono">{item.risk_score.toFixed(2)}</td>
                  <td className="px-3 py-2">{item.tier_label}</td>
                  <td className="px-3 py-2">Engine {item.engine_tier}</td>
                  <td className={`px-3 py-2 font-dm-mono ${formatSla(item.response_due_at).className}`}>{formatSla(item.response_due_at).text}</td>
                  <td className="px-3 py-2">{item.status}</td>
                  <td className="px-3 py-2">
                    <div className="flex gap-2">
                      <button onClick={() => setPreviewId(item.id)} className="rounded border border-[#CBD5E1] px-2 py-1 text-xs">Preview</button>
                      <button
                        onClick={() => approveMutation.mutate({ id: item.id })}
                        className="rounded bg-[#059669] px-2 py-1 text-xs text-white"
                        disabled={approveMutation.isPending}
                      >
                        Approve
                      </button>
                      <button
                        onClick={() => rejectMutation.mutate({ id: item.id, reason: rejectReason })}
                        className="rounded bg-[#B91C1C] px-2 py-1 text-xs text-white"
                        disabled={rejectMutation.isPending}
                      >
                        Reject
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="mt-4 flex items-center gap-3">
          <input
            value={rejectReason}
            onChange={(event) => setRejectReason(event.target.value)}
            className="w-80 rounded border border-[#CBD5E1] px-3 py-2 text-sm"
            placeholder="Rejection reason"
          />
          <button
            onClick={() => bulkApproveMutation.mutate()}
            disabled={bulkApproveMutation.isPending || selectedIds.length === 0}
            className="rounded-lg bg-[#003366] px-4 py-2 text-sm font-semibold text-white disabled:bg-[#94A3B8]"
          >
            Bulk Approve ({selectedIds.length})
          </button>
        </div>
      </article>

      <article className="rounded-xl border border-[#E2E6ED] bg-white p-4">
        <h3 className="font-syne text-xl text-[#0F172A]">Intervention History</h3>
        <div className="mt-3 max-h-64 overflow-auto rounded border border-[#E2E6ED]">
          <table className="w-full text-left text-sm">
            <thead className="bg-[#F8FAFC] text-xs uppercase tracking-[0.12em] text-[#64748B]">
              <tr>
                <th className="px-3 py-2">Customer</th>
                <th className="px-3 py-2">Engine</th>
                <th className="px-3 py-2">Status</th>
                <th className="px-3 py-2">Delivery</th>
                <th className="px-3 py-2">Sent At</th>
              </tr>
            </thead>
            <tbody>
              {(historyQuery.data || []).slice(0, 100).map((item) => (
                <tr key={item.id} className="border-t border-[#E2E6ED]">
                  <td className="px-3 py-2 font-dm-mono">{item.customer_id}</td>
                  <td className="px-3 py-2">Engine {item.engine_tier}</td>
                  <td className="px-3 py-2">{item.status}</td>
                  <td className="px-3 py-2">{item.delivery_status}</td>
                  <td className="px-3 py-2 font-dm-mono">{item.sent_at ? new Date(item.sent_at).toLocaleString() : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </article>

      {previewId && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
          <div className="max-h-[90vh] w-full max-w-5xl overflow-auto rounded-xl border border-[#E2E6ED] bg-white p-5">
            <div className="flex items-center justify-between">
              <h3 className="font-syne text-xl text-[#0F172A]">Email Preview</h3>
              <button onClick={() => setPreviewId(null)} className="rounded border border-[#CBD5E1] px-2 py-1 text-xs">Close</button>
            </div>
            {!previewQuery.data ? (
              <p className="mt-4 text-sm text-[#64748B]">Loading preview...</p>
            ) : (
              <>
                <p className="mt-2 font-dm-mono text-sm">{previewQuery.data.subject}</p>
                <div className="mt-4 rounded border border-[#E2E6ED] p-3" dangerouslySetInnerHTML={{ __html: previewQuery.data.html }} />
                <div className="mt-4 flex gap-2">
                  <button
                    onClick={() => testMailMutation.mutate(previewQuery.data.intervention_id)}
                    className="rounded border border-[#CBD5E1] px-3 py-1.5 text-sm"
                  >
                    Send Test Email
                  </button>
                  <button
                    onClick={() => approveMutation.mutate({
                      id: previewQuery.data.intervention_id,
                      checkerId: previewQuery.data.engine_tier === 3 ? window.prompt('Checker ID (required for Engine 3)') || '' : undefined,
                    })}
                    className="rounded bg-[#059669] px-3 py-1.5 text-sm text-white"
                  >
                    Send Now
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </section>
  );
}

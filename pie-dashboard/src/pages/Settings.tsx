import { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { pieApi } from '../api/pie';
import { useAuth } from '../hooks/useAuth';

export default function Settings() {
  const { session } = useAuth();
  const queryClient = useQueryClient();
  const { data: monitoring } = useQuery({
    queryKey: ['settingsMonitoring'],
    queryFn: pieApi.getModelMonitoring,
  });
  const { data: retrainConfig } = useQuery({
    queryKey: ['retrainConfig'],
    queryFn: pieApi.getRetrainConfig,
    refetchInterval: 15000,
  });

  const [colabUrl, setColabUrl] = useState('');
  const [bankThreshold, setBankThreshold] = useState('0.30');

  useEffect(() => {
    if (retrainConfig) {
      setColabUrl(retrainConfig.colab_ngrok_url || '');
      setBankThreshold(String(retrainConfig.bank_threshold ?? 0.3));
    }
  }, [retrainConfig]);

  const saveConfigMutation = useMutation({
    mutationFn: pieApi.updateRetrainConfig,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['retrainConfig'] });
    },
  });

  const latest = monitoring?.latestRun;

  const handleSave = async () => {
    await saveConfigMutation.mutateAsync({
      colab_ngrok_url: colabUrl,
      bank_threshold: Number(bankThreshold),
    });
  };

  return (
    <section className="grid grid-cols-12 gap-4 p-6">
      <article className="col-span-6 rounded-xl border border-[#E2E6ED] bg-white p-5">
        <h2 className="font-syne text-2xl text-[#0F172A]">Current Session</h2>
        <div className="mt-4 grid grid-cols-2 gap-3 text-sm text-[#475569]">
          <div className="rounded-lg border border-[#E2E6ED] p-3">Name: <span className="font-dm-mono">{session?.user.name ?? 'N/A'}</span></div>
          <div className="rounded-lg border border-[#E2E6ED] p-3">Role: <span className="font-dm-mono">{session?.user.role ?? 'N/A'}</span></div>
          <div className="rounded-lg border border-[#E2E6ED] p-3">Branch: <span className="font-dm-mono">{session?.user.branch ?? 'N/A'}</span></div>
          <div className="rounded-lg border border-[#E2E6ED] p-3">Email: <span className="font-dm-mono">{session?.user.email ?? 'N/A'}</span></div>
        </div>
      </article>

      <article className="col-span-6 rounded-xl border border-[#E2E6ED] bg-white p-5">
        <h3 className="font-syne text-xl text-[#0F172A]">Model Runtime Status</h3>
        <div className="mt-4 space-y-3 text-sm text-[#475569]">
          <div className="rounded-lg border border-[#E2E6ED] p-3">Status: <span className="font-dm-mono">{monitoring?.status ?? 'unknown'}</span></div>
          <div className="rounded-lg border border-[#E2E6ED] p-3">Latest Accuracy: <span className="font-dm-mono">{((latest?.accuracy ?? 0) * 100).toFixed(2)}%</span></div>
          <div className="rounded-lg border border-[#E2E6ED] p-3">Latest Drift: <span className="font-dm-mono">{monitoring?.drift.driftScore?.toFixed(3) ?? 'N/A'}</span></div>
          <div className="rounded-lg border border-[#E2E6ED] p-3">Retrain Recommendation: <span className="font-dm-mono">{monitoring?.shouldRetrain ? 'Yes' : 'No'}</span></div>
        </div>
      </article>

      <article className="col-span-12 rounded-xl border border-[#E2E6ED] bg-white p-5">
        <h3 className="font-syne text-xl text-[#0F172A]">Colab Retraining Connection</h3>
        <p className="mt-1 text-sm text-[#64748B]">Update the active ngrok URL after each Colab restart. PIE pings /health before retraining.</p>

        <div className="mt-4 grid grid-cols-12 gap-3 text-sm">
          <div className="col-span-8 rounded-lg border border-[#E2E6ED] p-3">
            <label className="text-xs font-semibold uppercase tracking-[0.12em] text-[#64748B]">Colab ngrok URL</label>
            <input
              value={colabUrl}
              onChange={(e) => setColabUrl(e.target.value)}
              placeholder="https://abc123.ngrok-free.app"
              className="mt-2 w-full rounded-md border border-[#CBD5E1] px-3 py-2 font-dm-mono text-sm text-[#0F172A]"
            />
          </div>
          <div className="col-span-2 rounded-lg border border-[#E2E6ED] p-3">
            <label className="text-xs font-semibold uppercase tracking-[0.12em] text-[#64748B]">Bank Threshold</label>
            <input
              value={bankThreshold}
              onChange={(e) => setBankThreshold(e.target.value)}
              className="mt-2 w-full rounded-md border border-[#CBD5E1] px-3 py-2 font-dm-mono text-sm text-[#0F172A]"
            />
          </div>
          <div className="col-span-2 rounded-lg border border-[#E2E6ED] p-3">
            <label className="text-xs font-semibold uppercase tracking-[0.12em] text-[#64748B]">Connection</label>
            <div className="mt-3">
              <span className={`rounded-full px-3 py-1 text-xs font-semibold ${retrainConfig?.colabConnection.connected ? 'bg-[#DCFCE7] text-[#166534]' : 'bg-[#FEE2E2] text-[#991B1B]'}`}>
                {retrainConfig?.colabConnection.connected ? 'Connected' : 'Disconnected'}
              </span>
            </div>
          </div>
        </div>

        <div className="mt-4 flex items-center gap-3">
          <button
            onClick={handleSave}
            disabled={saveConfigMutation.isPending}
            className="rounded-lg bg-[#0057B8] px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-[#003D82] disabled:bg-[#94A3B8]"
          >
            {saveConfigMutation.isPending ? 'Saving...' : 'Save Colab Config'}
          </button>
          {!retrainConfig?.colabConnection.connected && retrainConfig?.colabConnection.reason && (
            <p className="text-xs text-[#B91C1C]">{retrainConfig.colabConnection.reason}</p>
          )}
        </div>
      </article>
    </section>
  );
}



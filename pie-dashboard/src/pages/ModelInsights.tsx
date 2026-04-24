import { useEffect, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { pieApi } from '../api/pie';

function scoreColor(score: number): string {
  if (score > 0.8) return 'text-[#B91C1C]';
  if (score > 0.5) return 'text-[#B45309]';
  return 'text-[#15803D]';
}

function scoreBadge(score: number): string {
  if (score > 0.8) return 'bg-[#FEE2E2] text-[#B91C1C]';
  if (score > 0.5) return 'bg-[#FEF3C7] text-[#B45309]';
  return 'bg-[#DCFCE7] text-[#15803D]';
}

export default function ModelInsights() {
  const queryClient = useQueryClient();
  const [streamLogs, setStreamLogs] = useState<string[]>([]);

  const { data: driftStatus, isLoading: driftLoading } = useQuery({
    queryKey: ['driftStatus'],
    queryFn: pieApi.getDriftStatus,
    refetchInterval: 30000,
  });

  const { data: modelMonitoring, isLoading: modelLoading } = useQuery({
    queryKey: ['bothModelsMonitoring'],
    queryFn: pieApi.getBothModelsMonitoring,
    refetchInterval: 30000,
  });

  const { data: retrainStatus } = useQuery({
    queryKey: ['retrainStatus'],
    queryFn: () => pieApi.getRetrainingStatus(),
    refetchInterval: 30000,
    staleTime: 5000,
  });

  const { data: modelHistory } = useQuery({
    queryKey: ['modelHistory'],
    queryFn: pieApi.getModelHistory,
    refetchInterval: 30000,
  });

  const { data: predictionDistribution } = useQuery({
    queryKey: ['predictionDistribution7d'],
    queryFn: () => pieApi.getPredictionDistribution(7),
    refetchInterval: 30000,
  });

  const runDriftMutation = useMutation({
    mutationFn: pieApi.runDriftCheck,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['driftStatus'] });
      queryClient.invalidateQueries({ queryKey: ['retrainStatus'] });
      queryClient.invalidateQueries({ queryKey: ['modelHistory'] });
    },
  });

  const triggerRetrainMutation = useMutation({
    mutationFn: pieApi.triggerRetraining,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['retrainStatus'] });
    },
  });

  const activateModelMutation = useMutation({
    mutationFn: pieApi.activateModel,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['modelHistory'] });
      queryClient.invalidateQueries({ queryKey: ['bothModelsMonitoring'] });
      queryClient.invalidateQueries({ queryKey: ['driftStatus'] });
    },
  });

  const baseModel = modelMonitoring?.baseModel;
  const contextualModel = modelMonitoring?.contextualModel;
  const productionModel = modelHistory?.models.find((item) => item.status === 'production');
  const previousModel = modelHistory?.models.find((item) => item.status === 'retired');

  const ModelMetricsCard = ({
    model,
    title,
  }: {
    model: typeof baseModel | typeof contextualModel;
    title: string;
  }) => {
    if (!model) {
      return (
        <article className="rounded-xl border border-[#E2E6ED] bg-white p-5">
          <h3 className="font-syne text-xl text-[#0F172A]">{title}</h3>
          <p className="mt-3 text-sm text-[#64748B]">Model data unavailable.</p>
        </article>
      );
    }

    return (
      <article className="rounded-xl border border-[#E2E6ED] bg-white p-5">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="font-syne text-xl text-[#0F172A]">{title}</h3>
            <p className="text-xs uppercase tracking-[0.12em] text-[#64748B]">{model.type}</p>
          </div>
          <span className={`rounded-full px-3 py-1 text-xs font-semibold ${model.status === 'healthy' ? 'bg-[#DCFCE7] text-[#166534]' : 'bg-[#FEF3C7] text-[#92400E]'}`}>
            {model.status === 'healthy' ? 'Healthy' : 'Watch'}
          </span>
        </div>

        <div className="mt-4 grid grid-cols-4 gap-2">
          <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3">
            <p className="text-[11px] text-[#64748B]">AUC</p>
            <p className="mt-1 font-dm-mono text-lg text-[#0057B8]">{(model.latestRun?.auc ?? 0).toFixed(3)}</p>
          </div>
          <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3">
            <p className="text-[11px] text-[#64748B]">F1</p>
            <p className="mt-1 font-dm-mono text-lg text-[#0057B8]">{(model.latestRun?.f1 ?? 0).toFixed(3)}</p>
          </div>
          <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3">
            <p className="text-[11px] text-[#64748B]">Precision</p>
            <p className="mt-1 font-dm-mono text-lg text-[#0057B8]">{(model.latestRun?.precision ?? 0).toFixed(3)}</p>
          </div>
          <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3">
            <p className="text-[11px] text-[#64748B]">Recall</p>
            <p className="mt-1 font-dm-mono text-lg text-[#0057B8]">{(model.latestRun?.recall ?? 0).toFixed(3)}</p>
          </div>
        </div>

        <div className="mt-3 text-xs text-[#475569]">
          <p>Last Run: <span className="font-dm-mono text-[#0F172A]">{model.latestRun?.runAt ? new Date(model.latestRun.runAt).toLocaleString() : 'N/A'}</span></p>
          <p>Reason: <span className="font-dm-mono text-[#0F172A]">{model.latestRun?.triggerReason ?? 'N/A'}</span></p>
        </div>
      </article>
    );
  };

  const candidateFromStatus = useMemo(() => {
    const response = retrainStatus?.response as Record<string, unknown> | undefined;
    const result = (response?.result as Record<string, unknown> | undefined) ?? response;
    const metrics = (result?.metrics as Record<string, unknown> | undefined) ?? {};

    const version = String(result?.version ?? '').trim();
    const driveFileId = String(result?.drive_file_id ?? '').trim();
    if (!version || !driveFileId || retrainStatus?.status !== 'done') return null;

    return {
      version,
      drive_file_id: driveFileId,
      features_file_id: String(result?.features_file_id ?? ''),
      threshold_file_id: String(result?.threshold_file_id ?? ''),
      aucRoc: Number(metrics.auc_roc ?? 0),
      gini: Number(metrics.gini ?? 0),
      ksStat: Number(metrics.ks_stat ?? 0),
      precision: Number(metrics.precision ?? 0),
      recall: Number(metrics.recall ?? 0),
      f1: Number(metrics.f1 ?? 0),
    };
  }, [retrainStatus]);

  useEffect(() => {
    const activeJobId = retrainStatus?.jobId;
    const isRunning = retrainStatus?.status === 'pending' || retrainStatus?.status === 'running';
    if (!activeJobId || !isRunning) return;

    const baseUrl = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000';
    const source = new EventSource(`${baseUrl}/api/retrain/logs/stream?job_id=${encodeURIComponent(activeJobId)}`, {
      withCredentials: true,
    });

    source.onmessage = (event) => {
      const line = event.data as string;
      setStreamLogs((current) => [...current.slice(-149), line]);
    };

    source.onerror = () => {
      source.close();
    };

    return () => {
      source.close();
    };
  }, [retrainStatus?.jobId, retrainStatus?.status]);

  const triggerManualRetrain = async () => {
    if (!window.confirm('Trigger Colab retraining job now?')) return;
    await triggerRetrainMutation.mutateAsync();
  };

  const activateCandidate = async () => {
    if (!candidateFromStatus) return;
    if (!window.confirm(`Activate candidate model ${candidateFromStatus.version}?`)) return;

    const approverPrimary = window.prompt('Primary approver email/ID');
    const approverSecondary = window.prompt('Secondary approver email/ID (must be different)');
    if (!approverPrimary || !approverSecondary || approverPrimary === approverSecondary) {
      window.alert('Two distinct approvers are required for activation.');
      return;
    }

    await activateModelMutation.mutateAsync({
      version: candidateFromStatus.version,
      drive_file_id: candidateFromStatus.drive_file_id,
      approver_primary: approverPrimary,
      approver_secondary: approverSecondary,
      features_file_id: candidateFromStatus.features_file_id || undefined,
      threshold_file_id: candidateFromStatus.threshold_file_id || undefined,
    });
  };

  if (driftLoading || modelLoading) {
    return (
      <section className="p-6">
        <div className="rounded-xl border border-[#E2E6ED] bg-white p-6 text-[#64748B]">Loading model retraining insights...</div>
      </section>
    );
  }

  const driftScore = driftStatus?.compositeDriftScore ?? 0;

  return (
    <section className="space-y-6 p-6">
      <div className="rounded-xl border border-[#E2E6ED] bg-white p-5">
        <h2 className="font-syne text-2xl text-[#0F172A]">Sequential Model Insights</h2>
        <p className="mt-1 text-sm text-[#64748B]">LightGBM produces the upstream score and XGBoost turns that score into the final risk output.</p>

        <div className="mt-4 grid grid-cols-2 gap-4">
          <ModelMetricsCard model={baseModel} title="Stage 1: LightGBM Score" />
          <ModelMetricsCard model={contextualModel} title="Stage 2: XGBoost Finalizer" />
        </div>

        <div className="mt-4 grid grid-cols-4 gap-3">
          <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3 text-center">
            <p className="text-[11px] uppercase tracking-[0.12em] text-[#64748B]">Stage 1</p>
            <p className="mt-1 font-dm-mono text-sm text-[#0F172A]">Ingest</p>
          </div>
          <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3 text-center">
            <p className="text-[11px] uppercase tracking-[0.12em] text-[#64748B]">Stage 2</p>
            <p className="mt-1 font-dm-mono text-sm text-[#0F172A]">LightGBM</p>
          </div>
          <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3 text-center">
            <p className="text-[11px] uppercase tracking-[0.12em] text-[#64748B]">Stage 3</p>
            <p className="mt-1 font-dm-mono text-sm text-[#0F172A]">XGBoost</p>
          </div>
          <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3 text-center">
            <p className="text-[11px] uppercase tracking-[0.12em] text-[#64748B]">Stage 4</p>
            <p className="mt-1 font-dm-mono text-sm text-[#0F172A]">Final Score</p>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-4">
        <article className="col-span-4 rounded-xl border border-[#E2E6ED] bg-white p-5">
          <p className="text-xs uppercase tracking-[0.12em] text-[#64748B]">Composite Drift Score</p>
          <p className={`mt-2 font-dm-mono text-4xl font-semibold ${scoreColor(driftScore)}`}>{driftScore.toFixed(3)}</p>
          <div className="mt-3 flex items-center justify-between">
            <span className={`rounded-full px-3 py-1 text-xs font-semibold ${scoreBadge(driftScore)}`}>
              {driftStatus?.status?.toUpperCase() ?? 'STABLE'}
            </span>
            <button
              onClick={() => runDriftMutation.mutate()}
              disabled={runDriftMutation.isPending}
              className="rounded-lg bg-[#0057B8] px-3 py-1.5 text-xs font-semibold text-white hover:bg-[#003D82] disabled:bg-[#94A3B8]"
            >
              {runDriftMutation.isPending ? 'Running...' : 'Run Drift Check Now'}
            </button>
          </div>
          <div className="mt-4 space-y-2 text-xs text-[#475569]">
            <p>PSI: <span className="font-dm-mono text-[#0F172A]">{(driftStatus?.psiScore ?? 0).toFixed(3)}</span></p>
            <p>KS: <span className="font-dm-mono text-[#0F172A]">{(driftStatus?.ksScore ?? 0).toFixed(3)}</span></p>
            <p>JS: <span className="font-dm-mono text-[#0F172A]">{(driftStatus?.jsScore ?? 0).toFixed(3)}</span></p>
            <p>Data Quality: <span className="font-dm-mono text-[#0F172A]">{(driftStatus?.dataQualityScore ?? 0).toFixed(3)}</span></p>
            <p>Last Checked: <span className="font-dm-mono text-[#0F172A]">{driftStatus?.checkedAt ? new Date(driftStatus.checkedAt).toLocaleString() : 'N/A'}</span></p>
            <p>Next Scheduled Check: <span className="font-dm-mono text-[#0F172A]">{driftStatus?.nextScheduledCheck ? new Date(driftStatus.nextScheduledCheck).toLocaleString() : 'N/A'}</span></p>
          </div>
        </article>

        <article className="col-span-8 rounded-xl border border-[#E2E6ED] bg-white p-5">
          <h3 className="font-syne text-xl text-[#0F172A]">Per-Feature Drift Breakdown</h3>
          <div className="mt-4 max-h-64 overflow-auto rounded-lg border border-[#E2E6ED]">
            <table className="w-full text-left text-sm">
              <thead className="bg-[#F8FAFC] text-xs uppercase tracking-[0.12em] text-[#64748B]">
                <tr>
                  <th className="px-3 py-2">Feature</th>
                  <th className="px-3 py-2">PSI</th>
                  <th className="px-3 py-2">KS</th>
                </tr>
              </thead>
              <tbody>
                {(driftStatus?.featureBreakdown || []).map((row) => (
                  <tr key={row.feature} className="border-t border-[#E2E6ED]">
                    <td className="px-3 py-2 font-dm-mono text-[#0F172A]">{row.feature}</td>
                    <td className="px-3 py-2 font-dm-mono text-[#475569]">{row.psi.toFixed(4)}</td>
                    <td className="px-3 py-2 font-dm-mono text-[#475569]">{row.ks.toFixed(4)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>
      </div>

      <div className="grid grid-cols-12 gap-4">
        <article className="col-span-6 rounded-xl border border-[#E2E6ED] bg-white p-5">
          <h3 className="font-syne text-xl text-[#0F172A]">Model Health Panel</h3>
          <div className="mt-4 grid grid-cols-2 gap-3">
            <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3">
              <p className="text-xs text-[#64748B]">Production Version</p>
              <p className="mt-1 font-dm-mono text-lg text-[#0F172A]">{productionModel?.version ?? 'N/A'}</p>
            </div>
            <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3">
              <p className="text-xs text-[#64748B]">Trained Date</p>
              <p className="mt-1 font-dm-mono text-sm text-[#0F172A]">{productionModel?.trainedAt ? new Date(productionModel.trainedAt).toLocaleString() : 'N/A'}</p>
            </div>
            <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3">
              <p className="text-xs text-[#64748B]">AUC-ROC</p>
              <p className="mt-1 font-dm-mono text-lg text-[#0F172A]">{(productionModel?.aucRoc ?? baseModel?.latestRun?.auc ?? 0).toFixed(3)}</p>
            </div>
            <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3">
              <p className="text-xs text-[#64748B]">Gini</p>
              <p className="mt-1 font-dm-mono text-lg text-[#0F172A]">{(productionModel?.gini ?? 0).toFixed(3)}</p>
            </div>
            <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3">
              <p className="text-xs text-[#64748B]">KS Stat</p>
              <p className="mt-1 font-dm-mono text-lg text-[#0F172A]">{(productionModel?.ksStat ?? 0).toFixed(3)}</p>
            </div>
            <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-3">
              <p className="text-xs text-[#64748B]">AUC Delta vs Previous</p>
              <p className="mt-1 font-dm-mono text-lg text-[#0F172A]">{((productionModel?.aucRoc ?? 0) - (previousModel?.aucRoc ?? 0)).toFixed(3)}</p>
            </div>
          </div>

          <button
            onClick={triggerManualRetrain}
            disabled={triggerRetrainMutation.isPending}
            className="mt-4 rounded-lg bg-[#0057B8] px-4 py-2 text-sm font-semibold text-white hover:bg-[#003D82] disabled:bg-[#94A3B8]"
          >
            {triggerRetrainMutation.isPending ? 'Triggering...' : 'Trigger Retraining'}
          </button>
          <p className="mt-2 text-xs text-[#64748B]">Retraining target: Sequential XGBoost model fed by the LightGBM stage.</p>
        </article>

        <article className="col-span-6 rounded-xl border border-[#E2E6ED] bg-white p-5">
          <h3 className="font-syne text-xl text-[#0F172A]">Prediction Distribution (Last 7 Days)</h3>
          <div className="mt-4 space-y-3">
            {(predictionDistribution?.bins || []).map((bin) => {
              const maxCount = Math.max(...(predictionDistribution?.bins || []).map((item) => item.count), 1);
              const width = `${Math.max(4, (bin.count / maxCount) * 100)}%`;
              return (
                <div key={bin.bucket}>
                  <div className="mb-1 flex items-center justify-between text-xs text-[#475569]">
                    <span>{bin.bucket}</span>
                    <span className="font-dm-mono">{bin.count}</span>
                  </div>
                  <div className="h-2 rounded-full bg-[#E2E8F0]">
                    <div className="h-2 rounded-full bg-[#0057B8]" style={{ width }} />
                  </div>
                </div>
              );
            })}
          </div>
        </article>
      </div>

      {(retrainStatus?.status === 'pending' || retrainStatus?.status === 'running') && (
        <article className="rounded-xl border border-[#E2E6ED] bg-white p-5">
          <h3 className="font-syne text-xl text-[#0F172A]">Retraining Status Panel</h3>
          <div className="mt-3 grid grid-cols-4 gap-3 text-sm">
            <div className="rounded-lg border border-[#E2E6ED] p-3">Job: <span className="font-dm-mono">{retrainStatus.jobId}</span></div>
            <div className="rounded-lg border border-[#E2E6ED] p-3">Status: <span className="font-dm-mono">{retrainStatus.status}</span></div>
            <div className="rounded-lg border border-[#E2E6ED] p-3">Trigger: <span className="font-dm-mono">{retrainStatus.triggerType}</span></div>
            <div className="rounded-lg border border-[#E2E6ED] p-3">Drift at trigger: <span className="font-dm-mono">{(retrainStatus.driftScore ?? 0).toFixed(3)}</span></div>
          </div>
          <div className="mt-4 rounded-lg border border-[#1E293B] bg-[#0B1120] p-3 font-dm-mono text-xs text-[#E2E8F0]">
            <p className="text-[#94A3B8]">Live training logs</p>
            <div className="mt-2 max-h-44 overflow-auto whitespace-pre-wrap">
              {streamLogs.length === 0 ? 'Waiting for SSE logs...' : streamLogs.join('\n')}
            </div>
          </div>
        </article>
      )}

      {candidateFromStatus && (
        <article className="rounded-xl border border-[#E2E6ED] bg-white p-5">
          <h3 className="font-syne text-xl text-[#0F172A]">Model Activation Panel</h3>
          <div className="mt-4 grid grid-cols-2 gap-4">
            <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-4">
              <p className="text-xs uppercase tracking-[0.12em] text-[#64748B]">Current Production</p>
              <p className="mt-2 font-dm-mono text-sm">Version: {productionModel?.version ?? 'N/A'}</p>
              <p className="font-dm-mono text-sm">AUC: {(productionModel?.aucRoc ?? 0).toFixed(3)}</p>
              <p className="font-dm-mono text-sm">Gini: {(productionModel?.gini ?? 0).toFixed(3)}</p>
              <p className="font-dm-mono text-sm">KS: {(productionModel?.ksStat ?? 0).toFixed(3)}</p>
            </div>
            <div className="rounded-lg border border-[#E2E6ED] bg-[#F8FAFC] p-4">
              <p className="text-xs uppercase tracking-[0.12em] text-[#64748B]">Candidate Model</p>
              <p className="mt-2 font-dm-mono text-sm">Version: {candidateFromStatus.version}</p>
              <p className="font-dm-mono text-sm">AUC: {candidateFromStatus.aucRoc.toFixed(3)}</p>
              <p className="font-dm-mono text-sm">Gini: {candidateFromStatus.gini.toFixed(3)}</p>
              <p className="font-dm-mono text-sm">KS: {candidateFromStatus.ksStat.toFixed(3)}</p>
            </div>
          </div>
          <div className="mt-4 flex gap-3">
            <button
              onClick={activateCandidate}
              disabled={activateModelMutation.isPending}
              className="rounded-lg bg-[#059669] px-4 py-2 text-sm font-semibold text-white hover:bg-[#047857] disabled:bg-[#94A3B8]"
            >
              {activateModelMutation.isPending ? 'Activating...' : 'Activate New Model'}
            </button>
            <button
              onClick={() => queryClient.invalidateQueries({ queryKey: ['retrainStatus'] })}
              className="rounded-lg border border-[#CBD5E1] px-4 py-2 text-sm font-semibold text-[#334155] hover:bg-[#F8FAFC]"
            >
              Reject Candidate
            </button>
          </div>
        </article>
      )}

      <article className="rounded-xl border border-[#E2E6ED] bg-white p-5">
        <h3 className="font-syne text-xl text-[#0F172A]">Model Version History</h3>
        <div className="mt-4 overflow-auto rounded-lg border border-[#E2E6ED]">
          <table className="w-full text-left text-sm">
            <thead className="bg-[#F8FAFC] text-xs uppercase tracking-[0.12em] text-[#64748B]">
              <tr>
                <th className="px-3 py-2">Version</th>
                <th className="px-3 py-2">Trained</th>
                <th className="px-3 py-2">AUC</th>
                <th className="px-3 py-2">Gini</th>
                <th className="px-3 py-2">KS</th>
                <th className="px-3 py-2">Drift@Trigger</th>
                <th className="px-3 py-2">Status</th>
                <th className="px-3 py-2">Drive File</th>
              </tr>
            </thead>
            <tbody>
              {(modelHistory?.models || []).map((row) => (
                <tr key={row.version} className="border-t border-[#E2E6ED]">
                  <td className="px-3 py-2 font-dm-mono">{row.version}</td>
                  <td className="px-3 py-2 font-dm-mono">{row.trainedAt ? new Date(row.trainedAt).toLocaleString() : 'N/A'}</td>
                  <td className="px-3 py-2 font-dm-mono">{row.aucRoc.toFixed(3)}</td>
                  <td className="px-3 py-2 font-dm-mono">{row.gini.toFixed(3)}</td>
                  <td className="px-3 py-2 font-dm-mono">{row.ksStat.toFixed(3)}</td>
                  <td className="px-3 py-2 font-dm-mono">{(row.driftScoreAtTrigger ?? 0).toFixed(3)}</td>
                  <td className="px-3 py-2"><span className="rounded-full bg-[#E2E8F0] px-2 py-1 text-xs font-semibold text-[#1E293B]">{row.status}</span></td>
                  <td className="px-3 py-2 font-dm-mono text-xs">{row.driveFileId ?? '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </article>
    </section>
  );
}

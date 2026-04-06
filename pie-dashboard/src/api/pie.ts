import { apiClient } from './client';
import {
  RiskScoreResponse,
  HistoryScore,
  ExplanationResponse,
  CustomerProfileSummary,
  CustomerTransactionHistoryResponse,
  ModelMonitoringResponse,
  ContextualModelStatus,
  DualModelMonitoring,
  DriftStatusResponse,
  RetrainingStatusResponse,
  RetrainConfigResponse,
  ModelVersionHistory,
  PredictionDistributionResponse,
  InterventionQueueItem,
  InterventionPreview,
  InterventionHistoryItem,
} from '../types';
import { fromScore, normalizeRiskTier } from '../lib/risk';

export const pieApi = {
  predict: async (customerId: string, features: object, bypassCache: boolean = false): Promise<RiskScoreResponse> => {
    const response = await apiClient.post<RiskScoreResponse>('/api/v1/predict', {
      customer_id: customerId,
      features,
      bypass_cache: bypassCache,
    });
    return response.data;
  },

  getScore: async (id: string): Promise<RiskScoreResponse> => {
    const response = await apiClient.get<RiskScoreResponse>(`/api/v1/score/${id}`);
    return response.data;
  },

  getExplanation: async (id: string): Promise<ExplanationResponse> => {
    const response = await apiClient.get<ExplanationResponse>(`/api/v1/explain/${id}`);
    return response.data;
  },

  getCustomers: async (): Promise<CustomerProfileSummary[]> => {
    const response = await apiClient.get<{ customers: CustomerProfileSummary[] }>('/api/v1/customers');
    return response.data.customers || [];
  },

  getCustomerTransactions: async (id: string): Promise<CustomerTransactionHistoryResponse> => {
    const response = await apiClient.get<CustomerTransactionHistoryResponse>(`/api/v1/customers/${id}/transactions`);
    return response.data;
  },

  getHistory: async (id: string): Promise<HistoryScore[]> => {
    const response = await apiClient.get<{ history: HistoryScore[] }>(`/api/v1/history/${id}`);
    return response.data.history || [];
  },

  getAllScores: async (): Promise<RiskScoreResponse[]> => {
    const response = await apiClient.get<{ scores: RiskScoreResponse[] }>('/api/v1/all-scores');
    return response.data.scores || [];
  },

  getHealth: async () => {
    const response = await apiClient.get('/');
    return response.data;
  },

  getRegistry: async (): Promise<RiskScoreResponse[]> => {
    const scores = await pieApi.getAllScores();
    return scores.map((item) => ({
      ...item,
      risk_bucket: normalizeRiskTier(item.risk_bucket, item.risk_score),
    }));
  },

  getMetricSnapshot: async () => {
    const rows = await pieApi.getRegistry();
    const highRisk30d = rows.filter((row) => row.risk_score >= 65).length;
    const criticalNow = rows.filter((row) => row.risk_score >= 90).length;

    return {
      totalAccounts: rows.length,
      highRisk30d,
      criticalNow,
    };
  },

  getModelMonitoring: async (): Promise<ModelMonitoringResponse> => {
    const response = await apiClient.get<ModelMonitoringResponse>('/api/v1/model-monitoring');
    return response.data;
  },

  getBothModelsMonitoring: async (): Promise<DualModelMonitoring> => {
    const response = await apiClient.get<DualModelMonitoring>('/api/v1/model-monitoring/both');
    return response.data;
  },

  retrainModel: async (): Promise<{ status: string; monitoring: ModelMonitoringResponse }> => {
    const response = await apiClient.post<{ status: string; monitoring: ModelMonitoringResponse }>('/api/v1/model-monitoring/retrain');
    return response.data;
  },

  getContextualModelStatus: async (): Promise<ContextualModelStatus> => {
    const response = await apiClient.get<ContextualModelStatus>('/api/v1/model-monitoring/contextual');
    return response.data;
  },

  runDriftCheck: async (): Promise<{ drift: DriftStatusResponse; retraining?: Record<string, unknown> | null }> => {
    const response = await apiClient.post<{ drift: DriftStatusResponse; retraining?: Record<string, unknown> | null }>('/api/drift/check');
    return response.data;
  },

  getDriftStatus: async (): Promise<DriftStatusResponse> => {
    const response = await apiClient.get<DriftStatusResponse>('/api/drift/status');
    return response.data;
  },

  triggerRetraining: async (): Promise<{ jobId: string; status: string; colab?: Record<string, unknown> }> => {
    const response = await apiClient.post<{ jobId: string; status: string; colab?: Record<string, unknown> }>('/api/retrain/trigger', {
      triggerType: 'manual_admin',
      model: 'base',
    });
    return response.data;
  },

  getRetrainingStatus: async (jobId?: string): Promise<RetrainingStatusResponse> => {
    const response = await apiClient.get<RetrainingStatusResponse>('/api/retrain/status', {
      params: jobId ? { job_id: jobId } : undefined,
    });
    return response.data;
  },

  activateModel: async (payload: {
    version: string;
    drive_file_id: string;
    approver_primary: string;
    approver_secondary: string;
    features_file_id?: string;
    threshold_file_id?: string;
    preprocessor_file_id?: string;
  }): Promise<{ status: string; version: string; artifacts: Record<string, unknown> }> => {
    const response = await apiClient.post<{ status: string; version: string; artifacts: Record<string, unknown> }>('/api/model/activate', payload);
    return response.data;
  },

  getModelHistory: async (): Promise<{ models: ModelVersionHistory[] }> => {
    const response = await apiClient.get<{ models: ModelVersionHistory[] }>('/api/model/history');
    return response.data;
  },

  getPredictionDistribution: async (days: number = 7): Promise<PredictionDistributionResponse> => {
    const response = await apiClient.get<PredictionDistributionResponse>('/api/model/prediction-distribution', {
      params: { days },
    });
    return response.data;
  },

  getRetrainConfig: async (): Promise<RetrainConfigResponse> => {
    const response = await apiClient.get<RetrainConfigResponse>('/api/retrain/config');
    return response.data;
  },

  updateRetrainConfig: async (payload: { colab_ngrok_url?: string; bank_threshold?: number }): Promise<RetrainConfigResponse> => {
    const response = await apiClient.put<RetrainConfigResponse>('/api/retrain/config', payload);
    return response.data;
  },

  runInterventionOrchestrator: async (actor: string): Promise<{ scanned: number; created: number; passive: number }> => {
    const response = await apiClient.post<{ scanned: number; created: number; passive: number }>('/api/v1/interventions/orchestrate', { actor });
    return response.data;
  },

  getInterventionQueue: async (status?: string): Promise<InterventionQueueItem[]> => {
    const response = await apiClient.get<{ items: InterventionQueueItem[] }>('/api/v1/interventions/queue', {
      params: status ? { status } : undefined,
    });
    return response.data.items || [];
  },

  getInterventionHistory: async (engine_tier?: number, status?: string): Promise<InterventionHistoryItem[]> => {
    const response = await apiClient.get<{ items: InterventionHistoryItem[] }>('/api/v1/interventions/history', {
      params: { engine_tier, status },
    });
    return response.data.items || [];
  },

  getInterventionPreview: async (interventionId: string): Promise<InterventionPreview> => {
    const response = await apiClient.get<InterventionPreview>(`/api/v1/interventions/${interventionId}/preview`);
    return response.data;
  },

  approveIntervention: async (payload: {
    interventionId: string;
    admin_id: string;
    checker_id?: string;
    schedule_at?: string;
    comment?: string;
  }): Promise<{ status: string; delivery_status?: string; retry_count?: number; error?: string | null }> => {
    const response = await apiClient.post<{ status: string; delivery_status?: string; retry_count?: number; error?: string | null }>(`/api/v1/interventions/${payload.interventionId}/approve`, {
      admin_id: payload.admin_id,
      checker_id: payload.checker_id,
      schedule_at: payload.schedule_at,
      comment: payload.comment,
    });
    return response.data;
  },

  rejectIntervention: async (payload: { interventionId: string; admin_id: string; reason: string }): Promise<{ status: string }> => {
    const response = await apiClient.post<{ status: string }>(`/api/v1/interventions/${payload.interventionId}/reject`, {
      admin_id: payload.admin_id,
      reason: payload.reason,
    });
    return response.data;
  },

  sendInterventionTestEmail: async (payload: { interventionId: string; admin_id: string; admin_email: string }): Promise<{ status: string }> => {
    const response = await apiClient.post<{ status: string }>(`/api/v1/interventions/${payload.interventionId}/test-email`, {
      admin_id: payload.admin_id,
      admin_email: payload.admin_email,
    });
    return response.data;
  },

  bulkApproveInterventions: async (payload: { ids: string[]; admin_id: string }): Promise<{ items: Array<{ id: string; ok: boolean }> }> => {
    const response = await apiClient.post<{ items: Array<{ id: string; ok: boolean }> }>('/api/v1/interventions/bulk-approve', payload);
    return response.data;
  },

  toRiskTier: (score: number) => fromScore(score),
};

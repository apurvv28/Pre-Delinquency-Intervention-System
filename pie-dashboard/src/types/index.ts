export interface Features {
  P_2_last: number;
  P_2_mean: number;
  P_2_std: number;
  B_1_last: number;
  B_1_mean: number;
  B_2_last: number;
  D_39_last: number;
  D_41_last: number;
  R_1_last: number;
  S_3_last: number;
  util_ratio: number;
  pay_to_bal: number;
  delinq_trend_sum: number;
  bal_volatility: number;
  risk_composite: number;
  [key: string]: number;
}

export interface RiskScoreResponse {
  customer_id: string;
  risk_score: number;
  risk_bucket: RiskTier | 'LOW_RISK' | 'HIGH_RISK' | 'CRITICAL' | 'VERY_CRITICAL';
  base_model_risk_score?: number | null;
  base_model_risk_bucket?: string | null;
  context_model_risk_score?: number | null;
  context_model_risk_bucket?: string | null;
  final_model_risk_score?: number | null;
  final_model_risk_bucket?: string | null;
  pipeline_stage?: string;
  cached?: boolean;
  timestamp?: string;
  created_at?: string;
}

export type RiskTier =
  | 'VERY_LOW'
  | 'LOW'
  | 'MEDIUM'
  | 'HIGH'
  | 'CRITICAL'
  | 'VERY_CRITICAL';

export interface AdminUser {
  id: string;
  name: string;
  email: string;
  avatarUrl?: string;
  branch: string;
  role: 'ANALYST' | 'ADMIN' | 'CHECKER' | 'SUPER_ADMIN';
}

export interface AuthSession {
  token: string;
  refreshToken?: string;
  expiresAt: string;
  user: AdminUser;
}

export interface CustomerRecord {
  customerId: string;
  name: string;
  loanType: string;
  branch: string;
  riskScore: number;
  riskTier: RiskTier;
  dpd: number;
  lastUpdated: string;
  accountAgeMonths: number;
  monthlyIncome: number;
  emiBurden: number;
  preNpa?: boolean;
}

export interface NotificationItem {
  id: string;
  level: 'info' | 'warning' | 'critical';
  title: string;
  message: string;
  createdAt: string;
  read: boolean;
}

export interface MetricSnapshot {
  totalAccounts: number;
  highRisk30d: number;
  criticalNow: number;
}

export interface ModelMonitoringRun {
  runAt: string;
  modelVersion: string;
  threshold: number;
  accuracy: number;
  precision: number;
  recall: number;
  f1: number;
  auc: number;
  driftScore: number;
  triggerReason: string;
  datasetRows: number;
  featureCount?: number;
  modelType?: string;
  dataSource?: string;
}

export interface ModelDriftFeatureRow {
  feature: string;
  psi: number;
}

export interface ModelMonitoringResponse {
  status: 'healthy' | 'watch' | 'retrain';
  shouldRetrain: boolean;
  latestRun: ModelMonitoringRun | null;
  accuracyTimeline: ModelMonitoringRun[];
  drift: {
    driftScore: number;
    status: 'healthy' | 'watch' | 'retrain';
    shouldRetrain: boolean;
    baselineWindow: number;
    recentWindow: number;
    featureDrift: ModelDriftFeatureRow[];
  };
  historyCount: number;
  generatedAt: string;
}

export interface HistoryScore {
  customer_id: string;
  risk_score: number;
  risk_bucket: string;
  created_at: string;
}

export interface ExplanationResponse {
  customer_id: string;
  risk_score: number;
  risk_bucket: string;
  explanation: string;
}

export interface CustomerProfileSummary {
  customer_id: string;
  name: string;
  branch: string;
  loan_type: string;
  risk_segment: string;
  monthly_income: number;
  loan_amount?: number | null;
  occupation?: string | null;
  spending_culture?: string | null;
  intervention_status?: string | null;
  pre_npa?: boolean;
  account_age_months: number;
  relationship_manager: string;
  top_spending_reasons?: string[];
  avg_spend_risk_score?: number;
  transaction_count: number;
  latest_amount?: number | null;
  latest_transaction_time?: string | null;
  latest_risk_score?: number | null;
  latest_risk_bucket?: string | null;
}

export interface CustomerTransactionPoint {
  transaction_index: number;
  amount: number;
  balance_after: number;
  days_since_last_payment: number;
  previous_declines_24h: number;
  merchant_category: string;
  is_international: boolean;
  transaction_time: string;
  risk_score?: number | null;
  risk_bucket?: string | null;
  is_seeded: boolean;
}

export interface CustomerTransactionHistoryResponse {
  customer_id: string;
  profile: {
    customer_id: string;
    name: string;
    branch: string;
    loan_type: string;
    risk_segment: string;
    monthly_income: number;
    loan_amount?: number | null;
    occupation?: string | null;
    spending_culture?: string | null;
    intervention_status?: string | null;
    pre_npa?: boolean;
    account_age_months: number;
    relationship_manager: string;
  };
  transactions: CustomerTransactionPoint[];
}

export interface ContextualModelStatus {
  available: boolean;
  feature_count: number;
  threshold: number | null;
}

export interface DualModelMonitoring {
  baseModel: {
    name: string;
    type: 'lightgbm';
  } & ModelMonitoringResponse;
  contextualModel: {
    name: string;
    type: 'xgboost';
  } & ModelMonitoringResponse;
}

export interface DriftFeatureBreakdown {
  feature: string;
  psi: number;
  psiNormalized: number;
  ks: number;
}

export interface DriftStatusResponse {
  available: boolean;
  status: 'stable' | 'warning' | 'critical';
  compositeDriftScore: number;
  psiScore: number;
  ksScore: number;
  jsScore: number;
  dataQualityScore: number;
  featureBreakdown: DriftFeatureBreakdown[];
  qualityBreakdown: {
    nullRate: number;
    outOfRangeRate: number;
  };
  checkedAt: string | null;
  triggeredRetraining?: boolean;
  triggerMode?: string;
  nextScheduledCheck?: string | null;
  thresholds?: {
    warning: number;
    retrain: number;
  };
}

export interface RetrainingStatusResponse {
  available: boolean;
  jobId?: string;
  status: 'idle' | 'pending' | 'running' | 'done' | 'failed' | 'missing';
  triggerType?: string;
  triggeredBy?: string;
  driftScore?: number | null;
  createdAt?: string | null;
  updatedAt?: string | null;
  completedAt?: string | null;
  response?: Record<string, unknown>;
}

export interface RetrainConfigResponse {
  colab_ngrok_url: string;
  bank_threshold: number;
  scheduled_hour: number;
  colabConnection: {
    connected: boolean;
    url?: string;
    reason?: string;
    health?: Record<string, unknown>;
  };
}

export interface ModelVersionHistory {
  version: string;
  trainedAt: string | null;
  aucRoc: number;
  gini: number;
  ksStat: number;
  precision?: number | null;
  recall?: number | null;
  f1?: number | null;
  driftScoreAtTrigger?: number | null;
  driveFileId?: string | null;
  status: 'candidate' | 'production' | 'retired';
  modelPath?: string | null;
  preprocessorPath?: string | null;
  metadata?: Record<string, unknown>;
}

export interface PredictionDistributionResponse {
  days: number;
  bins: Array<{
    bucket: string;
    count: number;
  }>;
}

export interface InterventionQueueItem {
  id: string;
  customer_id: string;
  customer_name: string;
  engine_tier: 1 | 2 | 3;
  tier_label: 'HIGH_RISK' | 'CRITICAL' | 'VERY_CRITICAL';
  risk_score: number;
  status: string;
  delivery_status: string;
  created_at: string | null;
  sent_at: string | null;
  response_due_at: string | null;
  rm_escalation_flag: boolean;
  collections_flag: boolean;
}

export interface InterventionPreview {
  intervention_id: string;
  customer_id: string;
  engine_tier: 1 | 2 | 3;
  subject: string;
  variables: Record<string, unknown>;
  html: string;
  requires_dual_approval: boolean;
  case_file_id?: string;
}

export interface InterventionHistoryItem {
  id: string;
  customer_id: string;
  engine_tier: number;
  tier_label: string;
  risk_score: number;
  status: string;
  delivery_status: string;
  approved_by?: string | null;
  maker_id?: string | null;
  checker_id?: string | null;
  created_at: string | null;
  sent_at: string | null;
  retry_count: number;
  case_file_path?: string | null;
  subject?: string | null;
}

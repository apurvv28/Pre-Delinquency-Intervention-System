import { useMemo, useState } from 'react';
import { pieApi } from '../api/pie';
import { RiskScoreResponse } from '../types';
import { normalizeRiskTier } from '../lib/risk';

export interface PredictionInput {
  customerId: string;
  features: Record<string, number>;
}

export function useRiskPredictor(initial: PredictionInput) {
  const [payload, setPayload] = useState<PredictionInput>(initial);
  const [result, setResult] = useState<RiskScoreResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const runPrediction = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await pieApi.predict(payload.customerId, payload.features, true);
      const normalized: RiskScoreResponse = {
        ...response,
        risk_bucket: normalizeRiskTier(response.risk_bucket, response.risk_score),
      };
      setResult(normalized);
      return normalized;
    } catch (err) {
      const message = (err as Error).message ?? 'Prediction failed.';
      setError(message);
      throw err;
    } finally {
      setLoading(false);
    }
  };

  const setField = (key: string, value: number) => {
    setPayload((prev) => ({
      ...prev,
      features: {
        ...prev.features,
        [key]: value,
      },
    }));
  };

  return useMemo(
    () => ({
      payload,
      setPayload,
      setField,
      result,
      loading,
      error,
      runPrediction,
    }),
    [payload, result, loading, error],
  );
}

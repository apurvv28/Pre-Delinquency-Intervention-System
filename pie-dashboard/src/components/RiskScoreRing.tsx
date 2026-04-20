import { useMemo } from 'react';
import { RiskTier } from '../types';
import { riskTierStyles, riskTierLabel } from '../lib/risk';

interface RiskScoreRingProps {
  score: number;
  tier: RiskTier;
  size?: number;
}

export default function RiskScoreRing({ score, tier, size = 170 }: RiskScoreRingProps) {
  const radius = (size - 20) / 2;
  const circumference = 2 * Math.PI * radius;
  const progress = Math.max(0, Math.min(100, score));

  const color = useMemo(() => riskTierStyles(tier).ring, [tier]);
  const offset = circumference - (progress / 100) * circumference;

  return (
    <div className="relative inline-flex items-center justify-center">
      <svg className="-rotate-90" width={size} height={size} role="img" aria-label={`Risk score ${progress}`}>
        <circle cx={size / 2} cy={size / 2} r={radius} strokeWidth={10} stroke="rgba(148,163,184,0.2)" fill="none" />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          strokeWidth={10}
          strokeLinecap="round"
          stroke={color}
          fill="none"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          className="transition-[stroke-dashoffset] duration-700 ease-out"
        />
      </svg>
      <div className="absolute flex flex-col items-center">
        <span className="font-dm-mono text-4xl text-[#0F172A]">{progress.toFixed(2)}%</span>
        <span className="text-[10px] uppercase tracking-[0.2em] text-[#94A3B8]">{riskTierLabel(tier)}</span>
      </div>
    </div>
  );
}


import { useEffect, useState } from 'react';

interface ScoreRingProps {
  score: number;
  bucket: 'LOW_RISK' | 'HIGH_RISK' | 'CRITICAL' | 'VERY_CRITICAL';
}

export default function ScoreRing({ score, bucket }: ScoreRingProps) {
  const [animatedScore, setAnimatedScore] = useState(0);

  useEffect(() => {
    setAnimatedScore(score);
  }, [score]);

  const radius = 60;
  const circumference = 2 * Math.PI * radius;
  const strokeDashoffset = circumference - (animatedScore / 100) * circumference;

  const colorMap = {
    LOW_RISK: '#166534',
    HIGH_RISK: '#B45309',
    CRITICAL: '#C0392B',
    VERY_CRITICAL: '#7B0D0D',
  };

  const ringColor = colorMap[bucket] || colorMap.LOW_RISK;

  return (
    <div className="relative inline-flex items-center justify-center">
      <svg className="w-40 h-40 transform -rotate-90">
        <circle
          className="text-[#E2E6ED]"
          strokeWidth="12"
          stroke="currentColor"
          fill="transparent"
          r={radius}
          cx="80"
          cy="80"
        />
        <circle
          style={{
            strokeDasharray: circumference,
            strokeDashoffset: strokeDashoffset,
            transition: 'stroke-dashoffset 1s ease-out',
            stroke: ringColor,
          }}
          className="drop-shadow-lg"
          strokeWidth="12"
          strokeLinecap="round"
          fill="transparent"
          r={radius}
          cx="80"
          cy="80"
        />
      </svg>
      <div className="absolute flex flex-col items-center justify-center">
        <span className="text-4xl font-extrabold text-[#0F172A]">
          {animatedScore.toFixed(2)}
        </span>
        <span className="text-xs text-[#94A3B8] font-medium">/ 100</span>
      </div>
    </div>
  );
}


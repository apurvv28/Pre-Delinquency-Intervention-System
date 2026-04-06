import { RiskTier } from '../types';
import { riskTierLabel, riskTierStyles } from '../lib/risk';

interface RiskTierBadgeProps {
  tier: RiskTier;
}

export default function RiskTierBadge({ tier }: RiskTierBadgeProps) {
  const styles = riskTierStyles(tier);
  return (
    <span className={`inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-semibold tracking-[0.14em] ${styles.chip}`}>
      {riskTierLabel(tier)}
    </span>
  );
}

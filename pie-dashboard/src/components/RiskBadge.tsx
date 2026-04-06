interface RiskBadgeProps {
  bucket: 'LOW_RISK' | 'HIGH_RISK' | 'CRITICAL' | 'VERY_CRITICAL';
}

export default function RiskBadge({ bucket }: RiskBadgeProps) {
  const mapList = {
    LOW_RISK: { label: 'Low Risk', classes: 'bg-[#DCFCE7] text-[#166534] border-[#BBF7D0]' },
    HIGH_RISK: { label: 'High Risk', classes: 'bg-[#FEF3C7] text-[#B45309] border-[#FDE68A]' },
    CRITICAL: { label: 'Critical', classes: 'bg-[#FEE2E2] text-[#C0392B] border-[#FECACA]' },
    VERY_CRITICAL: { label: 'Very Critical', classes: 'bg-[#FEE2E2] text-[#7B0D0D] border-[#FECACA]' },
  };

  const { label, classes } = mapList[bucket] || mapList.LOW_RISK;

  return (
    <span className={`inline-flex items-center px-2.5 py-1 text-xs font-semibold uppercase tracking-wide border rounded-full ${classes}`}>
      {label}
    </span>
  );
}


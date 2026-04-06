interface MetricCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  color?: string;
}

export default function MetricCard({ title, value, subtitle, color = 'text-white' }: MetricCardProps) {
  return (
    <div className="bg-[#F4F6F9] rounded-xl border border-[#E2E6ED] p-6 shadow-sm hover:border-[#CBD5E1] hover:shadow-md transition-all">
      <h3 className="text-sm font-medium text-[#94A3B8] mb-2 uppercase tracking-wider">{title}</h3>
      <div className={`text-4xl font-bold ${color}`}>{value}</div>
      {subtitle && <p className="text-xs text-[#94A3B8] mt-2">{subtitle}</p>}
    </div>
  );
}



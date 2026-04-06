import { useEffect, useState } from 'react';

interface StatCardProps {
  title: string;
  value: number;
  suffix?: string;
  tone?: 'neutral' | 'cyan' | 'amber' | 'red';
}

const toneClass = {
  neutral: 'text-[#0F172A] border-[#E2E6ED]',
  cyan: 'text-[#0057B8] border-[#BFD4EA]',
  amber: 'text-[#B45309] border-[#FDE68A]',
  red: 'text-[#C0392B] border-[#FECACA]',
};

export default function StatCard({ title, value, suffix = '', tone = 'neutral' }: StatCardProps) {
  const [display, setDisplay] = useState(0);

  useEffect(() => {
    let frame = 0;
    const total = 24;
    const timer = window.setInterval(() => {
      frame += 1;
      setDisplay(Math.round((value * frame) / total));
      if (frame >= total) {
        window.clearInterval(timer);
      }
    }, 22);

    return () => window.clearInterval(timer);
  }, [value]);

  return (
    <div className={`rounded-lg border bg-white p-4 shadow-sm ${toneClass[tone]} animate-card-enter`}>
      <div className="mb-3 flex items-center justify-between">
        <p className="text-[11px] uppercase tracking-[0.08em] text-[#94A3B8]">{title}</p>
        <span className="inline-block rounded-md bg-[#EFF6FF] px-2 py-1 text-[10px] text-[#0057B8]">KPI</span>
      </div>
      <p className="font-dm-mono text-3xl font-bold">{display.toLocaleString()}{suffix}</p>
    </div>
  );
}



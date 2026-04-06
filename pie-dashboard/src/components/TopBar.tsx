import { useAppContext } from '../hooks/useAppContext';
import { useAuth } from '../hooks/useAuth';

interface TopBarProps {
  title: string;
}

export default function TopBar({ title }: TopBarProps) {
  const { unreadAlerts } = useAppContext();
  const { session } = useAuth();

  return (
    <header className="sticky top-0 z-30 flex h-16 items-center justify-between border-b border-[#E2E6ED] bg-white px-6">
      <h1 className="text-lg font-semibold text-[#0F172A]">{title}</h1>
      <div className="flex items-center gap-4">
        <input
          aria-label="Global customer search"
          placeholder="Search customer ID / name"
          className="w-72 rounded-md border border-[#E2E6ED] bg-[#F4F6F9] px-3 py-2 text-sm text-[#334155] outline-none focus:border-[#0057B8] focus:ring-1 focus:ring-[#0057B8]/20"
        />
        <button aria-label="Critical alerts" className="relative rounded-lg border border-[#E2E6ED] bg-white px-3 py-2 text-xs text-[#334155]">
          Alerts
          {unreadAlerts > 0 && (
            <span className="absolute -right-1 -top-1 rounded-full bg-red-500 px-1.5 py-0.5 text-[10px] text-white">{unreadAlerts}</span>
          )}
        </button>
        <div className="flex items-center gap-2 rounded-lg border border-[#E2E6ED] bg-white px-3 py-1.5">
          <img
            src={session?.user.avatarUrl ?? 'https://api.dicebear.com/9.x/thumbs/svg?seed=PIE'}
            alt="Admin avatar"
            className="h-8 w-8 rounded-full border border-[#CBD5E1]"
          />
          <div className="text-right">
            <p className="text-xs text-[#0F172A]">{session?.user.name ?? 'Bank Admin'}</p>
            <p className="text-[10px] uppercase tracking-[0.12em] text-[#0057B8]">
              {session?.user.branch ?? 'HQ'} · {session?.user.role ?? 'ADMIN'}
            </p>
          </div>
        </div>
      </div>
    </header>
  );
}


import { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth';

const links = [
  { to: '/', label: 'Dashboard', icon: '◉' },
  { to: '/customers', label: 'Customer Search', icon: '⌕' },
  { to: '/predict', label: 'Predict', icon: '⌁' },
  { to: '/registry', label: 'Registry', icon: '▦' },
  { to: '/interventions', label: 'Interventions', icon: '⚑' },
  { to: '/insights', label: 'Model Insights', icon: '∿' },
  { to: '/settings', label: 'Settings', icon: '⚙' },
];

export default function Sidebar() {
  const location = useLocation();
  const { session, logout } = useAuth();
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside
      className={`flex min-h-screen flex-col border-r border-[#002244] bg-[#003366] transition-[width] duration-300 ${
        collapsed ? 'w-[88px]' : 'w-[280px]'
      }`}
    >
      <div className="border-b border-[#002244] p-4">
        <div className="flex items-center justify-between">
          <div className={collapsed ? 'hidden' : 'block'}>
            <p className="font-syne text-2xl text-white">PIE</p>
            <p className="text-[10px] uppercase tracking-[0.15em] text-[#CBD5E1]">Pre-Delinquency Intelligence Engine</p>
          </div>
          <button
            className="rounded-md border border-[#335577] px-2 py-1 text-xs text-[#CBD5E1]"
            onClick={() => setCollapsed((prev) => !prev)}
            aria-label="Toggle sidebar"
          >
            {collapsed ? '>>' : '<<'}
          </button>
        </div>
      </div>

      <nav className="mt-4 flex-1 px-3">
        <ul className="space-y-1">
          {links.map((link) => {
            const active = location.pathname === link.to;
            return (
              <li key={link.to}>
                <Link
                  to={link.to}
                  className={`group flex items-center gap-3 rounded-lg border-l-[3px] px-3 py-2.5 text-sm transition-colors ${
                    active
                      ? 'border-l-[#0057B8] border-t-transparent border-r-transparent border-b-transparent bg-[rgba(255,255,255,0.08)] text-white'
                      : 'border-l-transparent border-t-transparent border-r-transparent border-b-transparent text-[#CBD5E1] hover:bg-[rgba(255,255,255,0.08)]'
                  }`}
                >
                  <span className="font-dm-mono text-sm">{link.icon}</span>
                  {!collapsed && <span>{link.label}</span>}
                </Link>
              </li>
            );
          })}
        </ul>
      </nav>

      <div className="border-t border-[#002244] p-3">
        {!collapsed && (
          <div className="mb-3 rounded-lg border border-[#335577] bg-[rgba(255,255,255,0.04)] px-3 py-2 text-xs">
            <p className="text-white">{session?.user.name ?? 'Bank Admin'}</p>
            <p className="text-[#CBD5E1]">{session?.user.branch ?? 'HQ'} · {session?.user.role ?? 'ADMIN'}</p>
          </div>
        )}
        <button onClick={logout} className="w-full rounded-lg border border-[#335577] px-3 py-2 text-xs text-white hover:bg-[rgba(255,255,255,0.08)]">
          Logout
        </button>
      </div>
    </aside>
  );
}


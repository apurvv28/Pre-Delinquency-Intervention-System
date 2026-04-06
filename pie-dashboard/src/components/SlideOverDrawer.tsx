interface SlideOverDrawerProps {
  open: boolean;
  title: string;
  children: React.ReactNode;
  onClose: () => void;
}

export default function SlideOverDrawer({ open, title, children, onClose }: SlideOverDrawerProps) {
  return (
    <>
      {open && <div className="fixed inset-0 z-40 bg-[#0F172A]/40 backdrop-blur-sm" onClick={onClose} />}
      <aside
        aria-hidden={!open}
        className={`fixed right-0 top-0 z-50 h-full w-[560px] max-w-full transform border-l border-[#E2E6ED] bg-white p-6 transition-transform duration-300 ${
          open ? 'translate-x-0' : 'translate-x-full'
        }`}
      >
        <div className="mb-5 flex items-start justify-between border-b border-[#E2E6ED] pb-4">
          <h3 className="font-syne text-2xl text-[#003366]">{title}</h3>
          <button className="rounded-md border border-[#CBD5E1] px-3 py-1 text-xs text-[#334155]" onClick={onClose}>
            Close
          </button>
        </div>
        <div className="h-[calc(100%-80px)] overflow-y-auto pr-1">{children}</div>
      </aside>
    </>
  );
}


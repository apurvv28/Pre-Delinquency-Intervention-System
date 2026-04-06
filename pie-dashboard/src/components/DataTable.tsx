import { useMemo, useState } from 'react';

export interface TableColumn<T> {
  key: keyof T;
  header: string;
  width?: string;
  render?: (row: T) => React.ReactNode;
}

interface DataTableProps<T extends { id?: string; customerId?: string }> {
  caption: string;
  columns: TableColumn<T>[];
  rows: T[];
  pageSize?: number;
  selectable?: boolean;
  onSelectionChange?: (rows: T[]) => void;
  rowClassName?: (row: T) => string;
  onRowClick?: (row: T) => void;
}

export default function DataTable<T extends { id?: string; customerId?: string }>({
  caption,
  columns,
  rows,
  pageSize = 10,
  selectable = false,
  onSelectionChange,
  rowClassName,
  onRowClick,
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<keyof T | null>(null);
  const [ascending, setAscending] = useState(true);
  const [page, setPage] = useState(1);
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const sortedRows = useMemo(() => {
    if (!sortKey) return rows;
    return [...rows].sort((a, b) => {
      const left = a[sortKey];
      const right = b[sortKey];
      if (left === right) return 0;
      const result = String(left).localeCompare(String(right), undefined, { numeric: true });
      return ascending ? result : -result;
    });
  }, [rows, sortKey, ascending]);

  const totalPages = Math.max(1, Math.ceil(sortedRows.length / pageSize));
  const safePage = Math.min(page, totalPages);
  const pagedRows = sortedRows.slice((safePage - 1) * pageSize, safePage * pageSize);

  const toggleSort = (key: keyof T) => {
    if (sortKey === key) {
      setAscending((prev) => !prev);
    } else {
      setSortKey(key);
      setAscending(true);
    }
  };

  const toggleSelect = (row: T, checked: boolean) => {
    const id = String(row.id ?? row.customerId ?? JSON.stringify(row));
    const next = new Set(selected);
    if (checked) {
      next.add(id);
    } else {
      next.delete(id);
    }
    setSelected(next);
    if (onSelectionChange) {
      onSelectionChange(rows.filter((item) => next.has(String(item.id ?? item.customerId ?? JSON.stringify(item)))));
    }
  };

  return (
    <div className="overflow-hidden rounded-lg border border-[#E2E6ED] bg-white">
      <table className="w-full text-left text-sm" aria-label={caption}>
        <caption className="sr-only">{caption}</caption>
        <thead className="bg-[#F4F6F9] text-[11px] uppercase tracking-widest text-[#94A3B8] border-b border-[#E2E6ED]">
          <tr>
            {selectable && <th className="px-3 py-3" scope="col">Select</th>}
            {columns.map((col) => (
              <th
                key={String(col.key)}
                scope="col"
                className="cursor-pointer px-3 py-3"
                style={{ width: col.width }}
                onClick={() => toggleSort(col.key)}
              >
                {col.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {pagedRows.map((row) => {
            const rowId = String(row.id ?? row.customerId ?? JSON.stringify(row));
            const selectedValue = selected.has(rowId);
            return (
              <tr
                key={rowId}
                className={`border-t border-[#F0F2F5] bg-white text-[#334155] ${selectedValue ? 'border-l-2 border-l-[#0057B8] bg-[#EFF6FF]' : ''} ${onRowClick ? 'cursor-pointer hover:bg-[#F8FAFC]' : ''} ${
                  rowClassName ? rowClassName(row) : ''
                }`}
                onClick={() => onRowClick?.(row)}
              >
                {selectable && (
                  <td className="px-3 py-2">
                    <input
                      aria-label={`Select row ${rowId}`}
                      type="checkbox"
                      checked={selectedValue}
                      onChange={(e) => toggleSelect(row, e.target.checked)}
                      onClick={(e) => e.stopPropagation()}
                    />
                  </td>
                )}
                {columns.map((col) => (
                  <td key={String(col.key)} className="px-3 py-2 align-middle">
                    {col.render ? col.render(row) : String(row[col.key] ?? '')}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
      <div className="flex items-center justify-between border-t border-[#E2E6ED] px-3 py-2 text-xs text-[#475569]">
        <span>
          Page {safePage} of {totalPages}
        </span>
        <div className="space-x-2">
          <button
            className="rounded-md border border-[#CBD5E1] bg-white px-2 py-1 disabled:opacity-40 hover:bg-[#F4F6F9]"
            onClick={() => setPage((prev) => Math.max(1, prev - 1))}
            disabled={safePage === 1}
          >
            Prev
          </button>
          <button
            className="rounded-md border border-[#CBD5E1] bg-white px-2 py-1 disabled:opacity-40 hover:bg-[#F4F6F9]"
            onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
            disabled={safePage === totalPages}
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
}


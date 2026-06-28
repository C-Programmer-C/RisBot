export const COMPARISON_LIMIT_OPTIONS = [1, 2, 3, 4, 5, 6, 8, 10] as const;

export const DEFAULT_MAX_COMPARISON_SELECTIONS = 2;

export const SLOT_PALETTE = [
  { badge: "bg-blue-600", border: "border-blue-300", header: "bg-blue-50", ring: "ring-blue-400", cell: "bg-blue-100 text-blue-900" },
  { badge: "bg-violet-600", border: "border-violet-300", header: "bg-violet-50", ring: "ring-violet-400", cell: "bg-violet-100 text-violet-900" },
  { badge: "bg-emerald-600", border: "border-emerald-300", header: "bg-emerald-50", ring: "ring-emerald-400", cell: "bg-emerald-100 text-emerald-900" },
  { badge: "bg-amber-600", border: "border-amber-300", header: "bg-amber-50", ring: "ring-amber-400", cell: "bg-amber-100 text-amber-900" },
  { badge: "bg-rose-600", border: "border-rose-300", header: "bg-rose-50", ring: "ring-rose-400", cell: "bg-rose-100 text-rose-900" },
  { badge: "bg-cyan-600", border: "border-cyan-300", header: "bg-cyan-50", ring: "ring-cyan-400", cell: "bg-cyan-100 text-cyan-900" },
  { badge: "bg-orange-600", border: "border-orange-300", header: "bg-orange-50", ring: "ring-orange-400", cell: "bg-orange-100 text-orange-900" },
  { badge: "bg-indigo-600", border: "border-indigo-300", header: "bg-indigo-50", ring: "ring-indigo-400", cell: "bg-indigo-100 text-indigo-900" },
  { badge: "bg-teal-600", border: "border-teal-300", header: "bg-teal-50", ring: "ring-teal-400", cell: "bg-teal-100 text-teal-900" },
  { badge: "bg-fuchsia-600", border: "border-fuchsia-300", header: "bg-fuchsia-50", ring: "ring-fuchsia-400", cell: "bg-fuchsia-100 text-fuchsia-900" },
] as const;

export type SlotStyles = (typeof SLOT_PALETTE)[number];

export function slotLabel(index: number): string {
  if (index < 26) {
    return String.fromCharCode(65 + index);
  }
  return String(index + 1);
}

export function getSlotStyles(index: number): SlotStyles {
  return SLOT_PALETTE[index % SLOT_PALETTE.length];
}

export function comparisonGridClass(count: number): string {
  if (count <= 1) return "grid-cols-1";
  if (count === 2) return "grid-cols-1 xl:grid-cols-2";
  if (count === 3) return "grid-cols-1 xl:grid-cols-2 2xl:grid-cols-3";
  return "grid-cols-1 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4";
}

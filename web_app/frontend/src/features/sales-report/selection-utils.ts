import type { DetailSelection } from "@/types/sales-report";

export function selectionKey(selection: DetailSelection): string {
  return `${selection.product}|${selection.year}|${selection.month}`;
}

export function selectionSlot(
  selections: DetailSelection[],
  candidate: DetailSelection,
): number {
  const key = selectionKey(candidate);
  return selections.findIndex((item) => selectionKey(item) === key);
}

export function toggleSelection(
  selections: DetailSelection[],
  candidate: DetailSelection,
  maxSelections: number,
): DetailSelection[] {
  const slot = selectionSlot(selections, candidate);
  if (slot >= 0) {
    return selections.filter((_, index) => index !== slot);
  }

  const max = Math.max(1, maxSelections);
  if (selections.length < max) {
    return [...selections, candidate];
  }

  return [...selections.slice(0, max - 1), candidate];
}

export function selectionLabel(selection: DetailSelection, monthNames: string[]): string {
  return `${selection.product}, ${monthNames[selection.month - 1]} ${selection.year}`;
}

export function trimSelections(
  selections: DetailSelection[],
  maxSelections: number,
): DetailSelection[] {
  return selections.slice(0, Math.max(1, maxSelections));
}

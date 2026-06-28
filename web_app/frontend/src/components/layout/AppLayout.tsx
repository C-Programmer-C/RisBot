import { Outlet } from "react-router-dom";
import { SettingsPanel } from "@/features/settings/SettingsPanel";
import { useSettings } from "@/features/settings/SettingsContext";

export function AppLayout() {
  const { openPanel } = useSettings();

  return (
    <div className="min-h-screen">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-4 sm:px-6">
          <div>
            <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
              Report
            </p>
            <h1 className="text-xl font-semibold text-slate-900">Отчёт</h1>
          </div>
          <button
            type="button"
            onClick={openPanel}
            className="rounded-lg border border-slate-300 px-4 py-2 text-sm text-slate-700 hover:bg-slate-50"
          >
            ⚙ Настройки
          </button>
        </div>
      </header>
      <main className="mx-auto max-w-7xl px-4 py-6 sm:px-6">
        <Outlet />
      </main>
      <SettingsPanel />
    </div>
  );
}

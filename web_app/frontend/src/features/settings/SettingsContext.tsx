import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import type { AppSettings } from "@/types/settings";
import { DEFAULT_SETTINGS, loadSettings, saveSettings } from "./settings-storage";

interface SettingsContextValue {
  settings: AppSettings;
  updateSettings: (patch: Partial<AppSettings>) => void;
  resetSettings: () => void;
  isPanelOpen: boolean;
  openPanel: () => void;
  closePanel: () => void;
}

const SettingsContext = createContext<SettingsContextValue | null>(null);

export function SettingsProvider({ children }: { children: ReactNode }) {
  const [settings, setSettings] = useState<AppSettings>(() => loadSettings());
  const [isPanelOpen, setIsPanelOpen] = useState(false);

  const updateSettings = useCallback((patch: Partial<AppSettings>) => {
    setSettings((prev) => {
      const next = { ...prev, ...patch };
      saveSettings(next);
      return next;
    });
  }, []);

  const resetSettings = useCallback(() => {
    setSettings(DEFAULT_SETTINGS);
    saveSettings(DEFAULT_SETTINGS);
  }, []);

  const value = useMemo(
    () => ({
      settings,
      updateSettings,
      resetSettings,
      isPanelOpen,
      openPanel: () => setIsPanelOpen(true),
      closePanel: () => setIsPanelOpen(false),
    }),
    [settings, updateSettings, resetSettings, isPanelOpen],
  );

  return <SettingsContext.Provider value={value}>{children}</SettingsContext.Provider>;
}

export function useSettings(): SettingsContextValue {
  const context = useContext(SettingsContext);
  if (!context) {
    throw new Error("useSettings must be used within SettingsProvider");
  }
  return context;
}

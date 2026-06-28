let resolvedApiUrl: string | null = null;

export async function getApiUrl(): Promise<string> {
  if (resolvedApiUrl !== null) {
    return resolvedApiUrl;
  }

  const fromEnv = import.meta.env.VITE_API_URL;
  if (fromEnv) {
    resolvedApiUrl = fromEnv.replace(/\/$/, "");
    return resolvedApiUrl;
  }

  const response = await fetch("/info");
  if (!response.ok) {
    throw new Error(`GET /info failed: ${response.status}`);
  }

  const payload = (await response.json()) as { api_url?: string };
  resolvedApiUrl = (payload.api_url ?? "").replace(/\/$/, "");
  return resolvedApiUrl;
}

import { createContext, useCallback, useEffect, useMemo, useState } from 'react';
import { apiClient } from '../api/client';
import { AuthSession } from '../types';

interface AuthContextValue {
  session: AuthSession | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  loginWithGoogle: () => void;
  logout: () => Promise<void>;
  refreshSession: () => Promise<void>;
}

const AUTH_SESSION_KEY = 'pie_auth_session';
const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000';
const loginUrl = import.meta.env.VITE_AUTH_LOGIN_URL ?? `${apiBaseUrl}/api/auth/sign-in/google`;
const sessionUrl = import.meta.env.VITE_AUTH_SESSION_URL ?? '/api/auth/session';
const logoutUrl = import.meta.env.VITE_AUTH_LOGOUT_URL ?? '/api/auth/sign-out';

export const AuthContext = createContext<AuthContextValue | null>(null);

function loadPersistedSession(): AuthSession | null {
  const raw = localStorage.getItem(AUTH_SESSION_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw) as AuthSession;
  } catch {
    localStorage.removeItem(AUTH_SESSION_KEY);
    return null;
  }
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [session, setSession] = useState<AuthSession | null>(() => loadPersistedSession());
  const [isLoading, setIsLoading] = useState(true);

  const persistSession = useCallback((next: AuthSession | null) => {
    setSession(next);
    if (next) {
      localStorage.setItem(AUTH_SESSION_KEY, JSON.stringify(next));
    } else {
      localStorage.removeItem(AUTH_SESSION_KEY);
    }
  }, []);

  const refreshSession = useCallback(async () => {
    try {
      const response = await apiClient.get<AuthSession>(sessionUrl, { withCredentials: true });
      persistSession(response.data);
    } catch {
      persistSession(null);
    } finally {
      setIsLoading(false);
    }
  }, [persistSession]);

  const loginWithGoogle = useCallback(() => {
    window.location.href = loginUrl;
  }, []);

  const logout = useCallback(async () => {
    try {
      await apiClient.post(logoutUrl, {}, { withCredentials: true });
    } catch {
      // Logout should always clear local state even if backend call fails.
    }
    persistSession(null);
    window.location.href = '/login';
  }, [persistSession]);

  useEffect(() => {
    refreshSession();
  }, [refreshSession]);

  useEffect(() => {
    if (!session?.expiresAt) return;
    const expiresAt = new Date(session.expiresAt).getTime();
    const now = Date.now();
    const refreshIn = expiresAt - now - 60_000;
    if (refreshIn <= 0) {
      refreshSession();
      return;
    }

    const timer = window.setTimeout(() => {
      refreshSession();
    }, refreshIn);

    return () => window.clearTimeout(timer);
  }, [session?.expiresAt, refreshSession]);

  const value = useMemo<AuthContextValue>(() => {
    return {
      session,
      isAuthenticated: Boolean(session?.token),
      isLoading,
      loginWithGoogle,
      logout,
      refreshSession,
    };
  }, [session, isLoading, loginWithGoogle, logout, refreshSession]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

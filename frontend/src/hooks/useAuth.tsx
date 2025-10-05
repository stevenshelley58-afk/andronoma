import { createContext, useContext, useMemo, useState } from "react";

import { loginRequest } from "../lib/api";

type AuthContextShape = {
  token: string | null;
  login: (email: string, password: string) => Promise<void>;
  setToken: (token: string | null) => void;
};

const AuthContext = createContext<AuthContextShape | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [token, setTokenState] = useState<string | null>(() => localStorage.getItem("andronoma-token"));

  const setToken = (value: string | null) => {
    setTokenState(value);
    if (value) {
      localStorage.setItem("andronoma-token", value);
    } else {
      localStorage.removeItem("andronoma-token");
    }
  };

  const login = async (email: string, password: string) => {
    const { access_token } = await loginRequest(email, password);
    setToken(access_token);
  };

  const value = useMemo(() => ({ token, login, setToken }), [token]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error("useAuth must be used within AuthProvider");
  }
  return ctx;
}

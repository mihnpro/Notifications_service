const TOKEN_KEY = "nm_auth_token";

export function getStoredToken() {
  try {
    return globalThis.localStorage?.getItem(TOKEN_KEY) || "";
  } catch {
    return "";
  }
}

export function setStoredToken(token) {
  try {
    if (!token) {
      globalThis.localStorage?.removeItem(TOKEN_KEY);
      return;
    }
    globalThis.localStorage?.setItem(TOKEN_KEY, token);
  } catch {
    // ignore storage errors in restricted environments
  }
}

export function clearStoredToken() {
  setStoredToken("");
}

import { FormEvent, useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { buildApiUrl } from "../../lib/api";
import { clearAuthUserCache, fetchAuthUser, primeAuthUser, type AuthUser } from "../../lib/auth";
import styles from "./AuthPage.module.css";

function nextPathFromSearch(search: string): string {
  const params = new URLSearchParams(search);
  return params.get("next") || "/";
}

export default function AuthPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const [status, setStatus] = useState<"idle" | "checking" | "error">("idle");
  const [message, setMessage] = useState("");
  const [identifier, setIdentifier] = useState("");
  const [password, setPassword] = useState("");
  const next = useMemo(() => nextPathFromSearch(location.search), [location.search]);

  useEffect(() => {
    let cancelled = false;
    fetchAuthUser()
      .then((user: AuthUser | null) => {
        if (cancelled) return;
        if (user) {
          navigate(next, { replace: true });
        }
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [navigate, next]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setStatus("checking");
    setMessage("");
    try {
      const res = await fetch(buildApiUrl("/api/auth/login"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ identifier, password }),
        cache: "no-store",
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data?.ok) {
        clearAuthUserCache();
        throw new Error(String(data?.message || "Не удалось выполнить вход"));
      }
      primeAuthUser(data.user || null);
      navigate(next, { replace: true });
    } catch (error) {
      setStatus("error");
      setMessage(error instanceof Error ? error.message : "Не удалось выполнить вход");
    }
  }

  return (
    <div className={styles.authPage}>
      <div className={styles.authCard}>
        <div className={styles.brand}>Аналитика данных</div>
        <h1 className={styles.title}>Доступ в панель</h1>
        <p className={styles.subtitle}>
          Войди под своим логином и паролем. После входа система создаст защищённую сессию и пустит на нужную страницу.
        </p>
        <form className={styles.form} onSubmit={handleSubmit}>
          <label className={styles.label} htmlFor="identifier">Логин</label>
          <input
            id="identifier"
            className={styles.input}
            value={identifier}
            onChange={(event) => setIdentifier(event.target.value)}
            autoComplete="username"
            placeholder="Например, manager"
          />
          <label className={styles.label} htmlFor="password">Пароль</label>
          <input
            id="password"
            className={styles.input}
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            autoComplete="current-password"
            placeholder="••••••••"
          />
          <button className={styles.submitButton} type="submit" disabled={status === "checking"}>
            {status === "checking" ? "Входим…" : "Войти"}
          </button>
          {status === "checking" ? <div className={styles.note}>Проверяем ссылку…</div> : null}
          {status === "error" ? <div className={styles.error}>{message}</div> : null}
        </form>
      </div>
    </div>
  );
}

import { Button, Input, LayerCard } from "@cloudflare/kumo";
import { useEffect, useRef, useState, type FormEvent } from "react";
import { base, call, Refusal } from "../../api";
import { Notice, refusalText } from "../../components/ui";
import { Mark } from "../../shell/Mark";

/** Where the sign-in returns to: `?next=`, a path inside the console, never another site or the login page. */
function destination(): string {
  const home = new URL(base, location.origin);
  try {
    const next = (new URLSearchParams(location.search).get("next") ?? "").replace(/^[\s/\\]+/, "");
    const url = new URL(next, home);
    if (url.origin === location.origin && url.pathname.startsWith(home.pathname) && url.pathname !== `${home.pathname}login`) {
      return url.pathname + url.search + url.hash;
    }
  } catch {
    /* a malformed next goes home */
  }
  return home.pathname;
}

export function LoginPage() {
  const [checking, setChecking] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const password = useRef<HTMLInputElement>(null);

  useEffect(() => {
    // Already signed in: straight on.
    call("GET", "session").then(
      () => location.replace(destination()),
      () => setChecking(false),
    );
  }, []);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await call("POST", "login", { password: password.current?.value ?? "" });
      return location.assign(destination());
    } catch (e) {
      setError(e instanceof Refusal && e.status === 401 ? "Wrong password." : refusalText(e));
    }
    setBusy(false);
    password.current?.select();
  }

  if (checking) return <main aria-busy="true" className="h-full" />;
  return (
    <main className="flex min-h-full flex-col items-center justify-center gap-6 p-4 pb-[12vh]">
      <span className="flex items-center gap-2.5 text-lg font-semibold">
        <Mark size={28} />
        edge workers
      </span>
      <LayerCard className="w-full max-w-[380px] p-7">
        <form onSubmit={submit} aria-labelledby="login-title" className="flex flex-col gap-5">
          <div className="flex flex-col gap-1">
            <h1 id="login-title" className="text-xl font-semibold tracking-tight">
              Sign in to edge workers
            </h1>
            <p className="text-base text-kumo-subtle">The console of the control node.</p>
          </div>
          <Input ref={password} label="Password" name="password" type="password" autoComplete="current-password" required autoFocus variant={error ? "error" : "default"} aria-invalid={!!error} />
          {error && <Notice error={error} />}
          <Button type="submit" variant="primary" loading={busy} className="w-full">
            Sign in
          </Button>
        </form>
      </LayerCard>
    </main>
  );
}

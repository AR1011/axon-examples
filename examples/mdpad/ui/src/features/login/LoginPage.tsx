import { Banner, Button, Input } from "@cloudflare/kumo";
import { WarningCircleIcon } from "@phosphor-icons/react";
import { useEffect, useState, type FormEvent } from "react";
import { at, call, message, Refusal, rememberUser, whoAmI } from "../../api";
import { Frame } from "../../shell/Frame";

/** Where the sign-in returns to: `?next=`, a path of this page, never another site or the login page. */
export function destination(search = location.search): string {
  const home = new URL(at("/"), location.origin);
  try {
    const next = (new URLSearchParams(search).get("next") ?? "").replace(/^[\s/\\]+/, "");
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

  useEffect(() => {
    // Already signed in: straight on.
    whoAmI().then(
      ({ user }) => {
        rememberUser(user);
        location.replace(destination());
      },
      () => setChecking(false),
    );
  }, []);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const [name, password] = [String(form.get("name") ?? ""), String(form.get("password") ?? "")];
    setBusy(true);
    setError(null);
    try {
      const { user } = await call<{ user: string }>("POST", "login", { name, password });
      rememberUser(user);
      return location.assign(destination());
    } catch (e) {
      setError(e instanceof Refusal && e.status === 401 ? "Wrong name or password." : message(e));
    }
    setBusy(false);
  }

  if (checking) return <main aria-busy="true" className="h-dvh" />;
  const shared = destination() !== at("/");
  return (
    <Frame title="Sign in" text={shared ? "You need an account. Ask your operator." : undefined}>
      <form onSubmit={submit} aria-label="Sign in" className="flex flex-col gap-5">
        <Input label="Name" name="name" autoComplete="username" required autoFocus />
        <Input label="Password" name="password" type="password" autoComplete="current-password" required />
        {error && <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={error} />}
        <Button type="submit" variant="primary" loading={busy} className="w-full">
          Sign in
        </Button>
      </form>
    </Frame>
  );
}

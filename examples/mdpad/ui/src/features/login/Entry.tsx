import { Banner, Button, Input, Loader } from "@cloudflare/kumo";
import { WarningCircleIcon } from "@phosphor-icons/react";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useState, type FormEvent } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { call, keepGuestPass, loginPath, message, Refusal, rememberUser, whoAmI } from "../../api";
import { Frame } from "../../shell/Frame";
import { destination } from "./LoginPage";

const Waiting = () => (
  <main className="flex h-dvh items-center justify-center" aria-busy="true">
    <Loader />
  </main>
);

const Failed = ({ error }: { error: string }) => <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={error} />;

/** `/i/:token`: a password from an operator's invite, then signed in. */
export function InvitePage() {
  const { token = "" } = useParams();
  const invite = useQuery({ queryKey: ["invite", token], queryFn: () => call<{ user: string; new: boolean }>("GET", `invite/${encodeURIComponent(token)}`) });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (invite.isPending) return <Waiting />;
  if (!invite.data) return <Frame title="This invite is not valid" text={invite.error instanceof Refusal && invite.error.status === 404 ? "Ask your operator for a new one." : message(invite.error)} />;
  const { user } = invite.data;

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const [password, again] = [String(form.get("password") ?? ""), String(form.get("again") ?? "")];
    if (password !== again) return setError("The passwords are not the same.");
    setBusy(true);
    setError(null);
    try {
      await call("POST", `invite/${encodeURIComponent(token)}`, { password });
      rememberUser(user);
      return location.assign(destination());
    } catch (e) {
      setError(message(e));
    }
    setBusy(false);
  }

  return (
    <Frame title={invite.data.new ? "Set your password" : "Set a new password"} text={`You are ${user}.`}>
      <form onSubmit={submit} className="flex flex-col gap-5">
        <input type="text" name="username" autoComplete="username" value={user} readOnly hidden />
        <Input label="Password" name="password" type="password" autoComplete="new-password" minLength={8} required autoFocus description="At least 8 characters." />
        <Input label="Password again" name="again" type="password" autoComplete="new-password" minLength={8} required />
        {error && <Failed error={error} />}
        <Button type="submit" variant="primary" loading={busy} className="w-full">
          Set password
        </Button>
      </form>
    </Frame>
  );
}

/** `/s/:token`: a share link. A user joins the document; without an account, a view link opens for a guest and an
 * edit link asks for a sign-in. */
export function LinkPage() {
  const { token = "" } = useParams();
  const navigate = useNavigate();
  const link = useQuery({ queryKey: ["link", token], queryFn: () => call<{ id: string; level: "view" | "edit" }>("GET", `s/${encodeURIComponent(token)}`) });
  const me = useQuery({ queryKey: ["me"], queryFn: whoAmI });
  const signedOut = me.error instanceof Refusal && me.error.status === 401;
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!link.data || !signedOut) return;
    if (link.data.level === "view") navigate(`/g/${token}`, { replace: true });
    else location.replace(loginPath());
  }, [link.data, navigate, signedOut, token]);

  if (link.isPending || me.isPending || (link.data && signedOut)) return <Waiting />;
  if (!link.data) {
    const gone = link.error instanceof Refusal && link.error.status === 404;
    return <Frame title="This link opens nothing" text={gone ? "The owner turned the link off or made a new one." : message(link.error)} />;
  }
  if (!me.data) return <Frame title="Cannot open the link" text={message(me.error)} />;
  const { id, level } = link.data;

  const join = async () => {
    setBusy(true);
    setError(null);
    try {
      await call("POST", `s/${encodeURIComponent(token)}/join`);
      navigate(`/d/${id}`, { replace: true });
    } catch (e) {
      setError(message(e));
      setBusy(false);
    }
  };
  return (
    <Frame title="Join a shared document" text={`The link lets you ${level === "edit" ? "edit" : "read"} it. It shows in your list after you join.`}>
      {error && <Failed error={error} />}
      <Button variant="primary" loading={busy} className="w-full" onClick={() => void join()}>
        Join
      </Button>
      <p className="text-sm text-kumo-subtle">Signed in as {me.data.user}.</p>
    </Frame>
  );
}

/** `/g/:token`: a guest of a view link gives a name, and reads the document in this tab. */
export function GuestPage() {
  const { token = "" } = useParams();
  const navigate = useNavigate();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const name = String(new FormData(event.currentTarget).get("name") ?? "").trim();
    setBusy(true);
    setError(null);
    try {
      const { id, token: pass } = await call<{ id: string; token: string }>("POST", "guest", { link: token, name });
      keepGuestPass(id, { token: pass, name, link: token });
      return navigate(`/d/${id}`, { replace: true });
    } catch (e) {
      setError(e instanceof Refusal && e.status === 404 ? "This link opens nothing. The owner turned it off or made a new one." : message(e));
    }
    setBusy(false);
  }

  return (
    <Frame title="Open as a guest" text="Others with the document open see this name.">
      <form onSubmit={submit} className="flex flex-col gap-5">
        <Input label="Your name" name="name" maxLength={40} autoComplete="nickname" required autoFocus />
        {error && <Failed error={error} />}
        <Button type="submit" variant="primary" loading={busy} className="w-full">
          Open
        </Button>
      </form>
      <p className="text-sm text-kumo-subtle">
        Have an account?{" "}
        <Link to={`/login?next=${encodeURIComponent(`/s/${token}`)}`} className="text-kumo-link hover:underline">
          Sign in
        </Link>
      </p>
    </Frame>
  );
}

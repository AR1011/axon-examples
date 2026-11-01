import { Banner, Button, Dialog, DropdownMenu, Input, useKumoToastManager } from "@cloudflare/kumo";
import { CaretDownIcon, WarningCircleIcon } from "@phosphor-icons/react";
import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router";
import { call, knownUser, loginPath, message, Refusal, rememberUser } from "../api";
import { clearUser, unsaved } from "../features/editor/copies";

/** Signs out, and removes this user's copies of their documents from this browser. */
async function signOut(user: string | null) {
  await call("POST", "logout").catch(() => undefined);
  const who = user ?? knownUser();
  if (who) await clearUser(who).catch(() => undefined);
  rememberUser(null);
  location.assign(loginPath("/"));
}

/** The account menu of the top bar: change the password, the admin page for an operator, sign out. */
export function Account({ user, operator }: { user: string | null; operator: boolean }) {
  const navigate = useNavigate();
  const [dialog, setDialog] = useState<"password" | "sign-out" | null>(null);
  const [waiting, setWaiting] = useState(0);
  const ask = async () => {
    // Copies with changes the server does not have are lost at sign-out: ask first.
    const who = user ?? knownUser();
    const count = who ? await unsaved(who).catch(() => 0) : 0;
    if (!count) return signOut(user);
    setWaiting(count);
    setDialog("sign-out");
  };
  return (
    <>
      <DropdownMenu>
        <DropdownMenu.Trigger
          render={
            <Button variant="ghost" size="sm" aria-label="Account">
              <span className="max-w-40 truncate">{user ?? "Account"}</span>
              <CaretDownIcon size={12} />
            </Button>
          }
        />
        <DropdownMenu.Content align="end">
          <DropdownMenu.Item onClick={() => setDialog("password")}>Change password</DropdownMenu.Item>
          {operator && <DropdownMenu.Item onClick={() => navigate("/admin")}>Admin</DropdownMenu.Item>}
          <DropdownMenu.Separator />
          <DropdownMenu.Item onClick={() => void ask()}>Sign out</DropdownMenu.Item>
        </DropdownMenu.Content>
      </DropdownMenu>
      {dialog === "password" && <Password onClose={() => setDialog(null)} />}
      <Dialog.Root role="alertdialog" open={dialog === "sign-out"} onOpenChange={(open) => !open && setDialog(null)}>
        <Dialog size="base" className="p-6">
          <Dialog.Title className="text-lg font-semibold">
            {waiting === 1 ? "1 document has unsaved changes." : `${waiting} documents have unsaved changes.`} Sign out anyway?
          </Dialog.Title>
          <Dialog.Description className="mt-2 text-base text-kumo-subtle">Sign-out removes the changes from this browser.</Dialog.Description>
          <div className="mt-6 flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setDialog(null)}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={() => void signOut(user)}>
              Sign out
            </Button>
          </div>
        </Dialog>
      </Dialog.Root>
    </>
  );
}

function Password({ onClose }: { onClose: () => void }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const toasts = useKumoToastManager();
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const [old, next, again] = ["old", "new", "again"].map((key) => String(form.get(key) ?? ""));
    if (next !== again) return setError("The new passwords are not the same.");
    setBusy(true);
    setError(null);
    try {
      await call("POST", "me/password", { old, new: next });
      toasts.add({ title: "Password changed. Your other sign-ins stopped.", variant: "success" });
      onClose();
    } catch (e) {
      setError(e instanceof Refusal && e.status === 403 ? "The current password is wrong." : message(e));
      setBusy(false);
    }
  }
  return (
    <Dialog.Root open onOpenChange={(open) => !open && onClose()}>
      <Dialog size="base" className="p-6">
        <Dialog.Title className="text-lg font-semibold">Change password</Dialog.Title>
        <form onSubmit={submit} className="mt-4 flex flex-col gap-4">
          <Input label="Current password" name="old" type="password" autoComplete="current-password" required autoFocus />
          <Input label="New password" name="new" type="password" autoComplete="new-password" minLength={8} required description="At least 8 characters." />
          <Input label="New password again" name="again" type="password" autoComplete="new-password" minLength={8} required />
          {error && <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={error} />}
          <div className="mt-2 flex justify-end gap-2">
            <Button type="button" variant="secondary" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit" variant="primary" loading={busy}>
              Change password
            </Button>
          </div>
        </form>
      </Dialog>
    </Dialog.Root>
  );
}

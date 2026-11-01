import { Badge, Banner, Button, ClipboardText, Empty, Input, Loader, Table, useKumoToastManager } from "@cloudflare/kumo";
import { WarningCircleIcon } from "@phosphor-icons/react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState, type FormEvent } from "react";
import { Link } from "react-router";
import { bytes, call, message, pageUrl, signInOn401 } from "../../api";
import { TopBar, useMe } from "../../shell/TopBar";

interface User {
  name: string;
  disabled: boolean;
  operator: boolean;
}

interface Home {
  home: string;
  error?: string;
  open?: number;
  docs?: { id: string; title: string; owner: string }[];
  more?: boolean;
  stored?: number;
  log_bytes?: number;
}

/** The operators' page: the users, and the documents open on each home. Nodes and events are in axon's console. */
export function AdminPage() {
  const me = useMe();
  if (me.isPending) return <main className="h-dvh" aria-busy="true" />;
  return (
    <div className="flex min-h-dvh flex-col">
      <TopBar me={me.data} />
      {me.data?.operator ? (
        <main className="mx-auto flex w-full max-w-[1040px] flex-col gap-8 px-4 py-6">
          <h1 className="text-xl font-semibold">Admin</h1>
          <Users />
          <Owner />
          <Homes />
        </main>
      ) : (
        <main className="flex flex-1 items-center justify-center p-4">
          <Empty title="Operators only" description={me.error ? message(me.error) : "Ask an operator for this page."} contents={<Link to="/">Documents</Link>} />
        </main>
      )}
    </div>
  );
}

function Users() {
  const me = useMe();
  const queries = useQueryClient();
  const toasts = useKumoToastManager();
  const users = useQuery({ queryKey: ["admin", "users"], queryFn: signInOn401(() => call<{ users: User[] }>("GET", "admin/users")) });
  const set = useMutation({
    mutationFn: ({ name, disabled }: { name: string; disabled: boolean }) => call("PUT", `admin/users/${encodeURIComponent(name)}`, { disabled }),
    onSuccess: (_, { name, disabled }) => toasts.add({ title: disabled ? `${name} disabled` : `${name} enabled`, variant: "success" }),
    onError: (e) => toasts.add({ title: "Not changed", description: message(e), variant: "error" }),
    onSettled: () => queries.invalidateQueries({ queryKey: ["admin", "users"] }),
  });
  return (
    <section className="flex flex-col gap-3">
      <h2 className="text-base font-semibold">Users</h2>
      <Invite onDone={() => queries.invalidateQueries({ queryKey: ["admin", "users"] })} />
      {users.error && <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} description={message(users.error)} />}
      {users.isPending ? (
        <Loader />
      ) : (
        <Table aria-label="Users">
          <Table.Header>
            <Table.Row>
              <Table.Head>Name</Table.Head>
              <Table.Head>Status</Table.Head>
              <Table.Head className="w-32">
                <span className="sr-only">Change</span>
              </Table.Head>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {users.data?.users.map((user) => (
              <Table.Row key={user.name}>
                <Table.Cell className="font-medium">
                  {user.name}
                  {user.operator && (
                    <Badge variant="outline" className="ml-2">
                      Operator
                    </Badge>
                  )}
                </Table.Cell>
                <Table.Cell>{user.disabled ? <Badge variant="error">Disabled</Badge> : <span className="text-kumo-subtle">Active</span>}</Table.Cell>
                <Table.Cell className="text-right">
                  {/* Another operator changes your account: the server refuses it from you. */}
                  {user.name !== me.data?.user && (
                    <Button
                      size="sm"
                      variant={user.disabled ? "secondary" : "secondary-destructive"}
                      aria-label={`${user.disabled ? "Enable" : "Disable"} ${user.name}`}
                      disabled={set.isPending}
                      onClick={() => set.mutate({ name: user.name, disabled: !user.disabled })}
                    >
                      {user.disabled ? "Enable" : "Disable"}
                    </Button>
                  )}
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table>
      )}
    </section>
  );
}

/** Adds a user: the answer is an invite link, which sets the user's password once. */
function Invite({ onDone }: { onDone: () => void }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [made, setMade] = useState<{ name: string; path: string } | null>(null);
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    const name = String(new FormData(form).get("name") ?? "").trim();
    setBusy(true);
    setError(null);
    try {
      setMade(await call<{ name: string; path: string }>("POST", "admin/users", { name }));
      form.reset();
      onDone();
    } catch (e) {
      setMade(null);
      setError(message(e));
    }
    setBusy(false);
  }
  return (
    <form onSubmit={submit} className="flex flex-col gap-2">
      <div className="flex flex-wrap items-end gap-2">
        <div className="min-w-0 flex-[0_1_16rem]">
          <Input label="New user" name="name" placeholder="User name" autoComplete="off" autoCapitalize="none" spellCheck={false} className="w-full" required />
        </div>
        <Button type="submit" variant="primary" loading={busy}>
          Invite
        </Button>
      </div>
      {error && <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={error} />}
      {made && (
        <div className="flex flex-col gap-1">
          <p className="text-sm text-kumo-subtle">Send this link to {made.name}. It sets a password once, in the next 7 days.</p>
          <ClipboardText size="base" text={pageUrl(made.path)} labels={{ copyAction: "Copy link" }} />
        </div>
      )}
    </form>
  );
}

/** Gives a document a new owner. The owner before stays, as an edit member. */
function Owner() {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const toasts = useKumoToastManager();
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    const data = new FormData(form);
    const [id, user] = ["id", "user"].map((key) => String(data.get(key) ?? "").trim());
    setBusy(true);
    setError(null);
    try {
      await call("PUT", `admin/docs/${encodeURIComponent(id)}/owner`, { user });
      toasts.add({ title: `${id} is ${user}'s now.`, variant: "success" });
      form.reset();
    } catch (e) {
      setError(message(e));
    }
    setBusy(false);
  }
  return (
    <section className="flex flex-col gap-3">
      <h2 className="text-base font-semibold">Change the owner of a document</h2>
      <form onSubmit={submit} className="flex flex-col gap-2">
        <div className="flex flex-wrap items-end gap-2">
          <div className="min-w-0 flex-[0_1_16rem]">
            <Input label="Document id" name="id" placeholder="h1-…" autoComplete="off" spellCheck={false} className="w-full" required />
          </div>
          <div className="min-w-0 flex-[0_1_12rem]">
            <Input label="New owner" name="user" placeholder="User name" autoComplete="off" autoCapitalize="none" spellCheck={false} className="w-full" required />
          </div>
          <Button type="submit" variant="secondary" loading={busy}>
            Change owner
          </Button>
        </div>
        {error && <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={error} />}
      </form>
    </section>
  );
}

/** The documents open on each home, from that home's catalog, read again each 5 s. */
function Homes() {
  const homes = useQuery({ queryKey: ["admin", "homes"], queryFn: () => call<{ homes: Home[] }>("GET", "admin/homes"), refetchInterval: 5_000 });
  return (
    <section className="flex flex-col gap-3">
      <h2 className="text-base font-semibold">Open documents</h2>
      {homes.error && <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} description={message(homes.error)} />}
      {homes.isPending && <Loader />}
      {homes.data?.homes.map((home) =>
        home.error !== undefined ? (
          <Banner key={home.home} size="sm" variant="alert" icon={<WarningCircleIcon weight="fill" />} description={`${home.home} did not answer: ${home.error}`} />
        ) : (
          <div key={home.home} className="flex flex-col gap-1.5">
            <p className="text-sm text-kumo-subtle">
              <span className="font-medium text-kumo-default">{home.home}</span>: {home.open} open, {home.stored} stored, log {bytes(home.log_bytes ?? 0)}
            </p>
            {home.docs?.length ? (
              // Fixed widths, so the tables of every home line up.
              <Table aria-label={`Open documents on ${home.home}`} className="table-fixed">
                <Table.Header>
                  <Table.Row>
                    <Table.Head>Title</Table.Head>
                    <Table.Head className="w-[22%]">Owner</Table.Head>
                    <Table.Head className="w-[42%]">Id</Table.Head>
                  </Table.Row>
                </Table.Header>
                <Table.Body>
                  {home.docs.map((doc) => (
                    <Table.Row key={doc.id}>
                      <Table.Cell className="truncate font-medium">{doc.title || "Untitled"}</Table.Cell>
                      <Table.Cell className="truncate">{doc.owner}</Table.Cell>
                      <Table.Cell className="font-mono text-sm break-all text-kumo-subtle">{doc.id}</Table.Cell>
                    </Table.Row>
                  ))}
                </Table.Body>
              </Table>
            ) : null}
            {home.more && <p className="text-sm text-kumo-subtle">More are open. mdpad cluster gives the count.</p>}
          </div>
        ),
      )}
    </section>
  );
}

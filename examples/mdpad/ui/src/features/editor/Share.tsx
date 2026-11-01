import { Banner, Button, ClipboardText, Dialog, Input, Loader, Radio, Select, Switch, Table, Tooltip, useKumoToastManager } from "@cloudflare/kumo";
import { WarningCircleIcon, XIcon } from "@phosphor-icons/react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState, type FormEvent } from "react";
import { call, message, pageUrl, Refusal, type Share as Sharing } from "../../api";

const LEVELS = [
  { value: "view", label: "Can view" },
  { value: "edit", label: "Can edit" },
];

/** Who has the document: the link, the people. The owner changes them; the members see them. */
export function Share({ id, onClose }: { id: string; onClose: () => void }) {
  const path = `docs/${encodeURIComponent(id)}`;
  const queries = useQueryClient();
  const toasts = useKumoToastManager();
  const share = useQuery({ queryKey: ["share", id], queryFn: () => call<Sharing>("GET", `${path}/share`), refetchOnMount: "always" });
  const change = useMutation({
    mutationFn: ({ method, to, body }: { method: string; to: string; body?: unknown; done?: string }) => call(method, `${path}/${to}`, body),
    onSuccess: (_, { done }) => done && toasts.add({ title: done, variant: "success" }),
    onError: (e) => toasts.add({ title: "Not changed", description: message(e), variant: "error" }),
    onSettled: () => queries.invalidateQueries({ queryKey: ["share", id] }),
  });
  const data = share.data;
  const owner = data?.token != null;
  const viaLink = data?.members.filter((m) => m.via === "link") ?? [];

  return (
    <Dialog.Root open onOpenChange={(open) => !open && onClose()}>
      <Dialog size="lg" className="flex max-h-[calc(100dvh-4rem)] flex-col sm:top-8">
        <div className="flex shrink-0 items-center gap-3 border-b border-kumo-hairline px-5 py-3">
          <Dialog.Title className="text-lg font-semibold">Share</Dialog.Title>
          <Dialog.Close render={<Button variant="ghost" shape="square" size="sm" aria-label="Close" icon={XIcon} className="ml-auto" />} />
        </div>
        <div className="flex min-h-0 flex-1 flex-col gap-6 overflow-y-auto p-5">
          {share.error && <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={message(share.error)} />}
          {!data ? (
            !share.error && (
              <div className="flex justify-center p-6">
                <Loader />
              </div>
            )
          ) : (
            <>
              {!owner && <p className="text-base text-kumo-subtle">Only {data.owner} can change who has access.</p>}
              <section className="flex flex-col gap-3">
                <Radio.Group
                  legend="Link"
                  orientation="horizontal"
                  value={data.link}
                  disabled={!owner || change.isPending}
                  onValueChange={(link) => change.mutate({ method: "PUT", to: "share", body: { link } })}
                >
                  <Radio.Item label="Off" value="off" />
                  <Radio.Item label="Can view" value="view" />
                  <Radio.Item label="Can edit" value="edit" />
                </Radio.Group>
                {owner && data.link !== "off" && (
                  <div className="flex flex-wrap items-center gap-2">
                    <ClipboardText size="base" text={pageUrl(`s/${data.token}`)} className="min-w-0 flex-1" labels={{ copyAction: "Copy link" }} />
                    <Tooltip
                      content="Make a new link. The old link stops, and guests of it close."
                      render={<Button variant="secondary" disabled={change.isPending} onClick={() => change.mutate({ method: "POST", to: "share/reset", done: "New link made" })} />}
                    >
                      Reset link
                    </Tooltip>
                  </div>
                )}
              </section>
              <section className="flex flex-col gap-3">
                <Switch
                  label="Publish"
                  labelTooltip="Anyone with the address can read the text, with no sign-in. They cannot edit it."
                  checked={data.published}
                  disabled={!owner || change.isPending}
                  onCheckedChange={(published) => change.mutate({ method: "PUT", to: "share", body: { published } })}
                />
                {data.published && <ClipboardText size="base" text={pageUrl(`p/${id}`)} labels={{ copyAction: "Copy address" }} />}
              </section>
              {owner && <Invite onInvite={(user, level) => call("PUT", `${path}/members/${encodeURIComponent(user)}`, { level })} onDone={() => queries.invalidateQueries({ queryKey: ["share", id] })} />}
              <section className="flex flex-col gap-2">
                <div className="flex items-center gap-2">
                  <h3 className="mr-auto text-base font-semibold">People</h3>
                  {owner && viaLink.length > 0 && (
                    <Button size="sm" variant="secondary-destructive" disabled={change.isPending} onClick={() => change.mutate({ method: "POST", to: "share/remove-link-members", done: "People via link removed" })}>
                      Remove all via link
                    </Button>
                  )}
                </div>
                <Table>
                  <Table.Header>
                    <Table.Row>
                      <Table.Head>User</Table.Head>
                      <Table.Head>Access</Table.Head>
                      <Table.Head>How</Table.Head>
                      {owner && (
                        <Table.Head className="w-12">
                          <span className="sr-only">Remove</span>
                        </Table.Head>
                      )}
                    </Table.Row>
                  </Table.Header>
                  <Table.Body>
                    <Table.Row>
                      <Table.Cell className="font-medium">{data.owner}</Table.Cell>
                      <Table.Cell>Owner</Table.Cell>
                      <Table.Cell />
                      {owner && <Table.Cell />}
                    </Table.Row>
                    {data.members.map((m) => (
                      <Table.Row key={m.user} data-member={m.user}>
                        <Table.Cell className="font-medium">{m.user}</Table.Cell>
                        <Table.Cell>
                          {owner ? (
                            <Select
                              size="sm"
                              aria-label={`Access of ${m.user}`}
                              value={m.level}
                              items={LEVELS}
                              disabled={change.isPending}
                              onValueChange={(level) => level && level !== m.level && change.mutate({ method: "PUT", to: `members/${encodeURIComponent(m.user)}`, body: { level } })}
                            />
                          ) : m.level === "edit" ? (
                            "Can edit"
                          ) : (
                            "Can view"
                          )}
                        </Table.Cell>
                        <Table.Cell className="text-kumo-subtle">{m.via === "link" ? "via link" : "invited"}</Table.Cell>
                        {owner && (
                          <Table.Cell className="text-right">
                            <Button
                              variant="ghost"
                              size="sm"
                              shape="square"
                              icon={XIcon}
                              aria-label={`Remove ${m.user}`}
                              disabled={change.isPending}
                              onClick={() => change.mutate({ method: "DELETE", to: `members/${encodeURIComponent(m.user)}`, done: `${m.user} removed` })}
                            />
                          </Table.Cell>
                        )}
                      </Table.Row>
                    ))}
                  </Table.Body>
                </Table>
              </section>
            </>
          )}
        </div>
      </Dialog>
    </Dialog.Root>
  );
}

function Invite({ onInvite, onDone }: { onInvite: (user: string, level: string) => Promise<unknown>; onDone: () => void }) {
  const [level, setLevel] = useState("edit");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const toasts = useKumoToastManager();
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    const user = String(new FormData(form).get("user") ?? "").trim();
    if (!user) return;
    setBusy(true);
    setError(null);
    try {
      await onInvite(user, level);
      form.reset();
      toasts.add({ title: `${user} added`, variant: "success" });
      onDone();
    } catch (e) {
      setError(e instanceof Refusal && e.code === "not_found" ? "No such user." : message(e));
    }
    setBusy(false);
  }
  return (
    <form onSubmit={submit} className="flex flex-col gap-2">
      <div className="flex flex-wrap items-end gap-2">
        <div className="min-w-0 flex-[1_1_12rem]">
          <Input label="Invite a user" name="user" placeholder="User name" autoComplete="off" autoCapitalize="none" spellCheck={false} className="w-full" required />
        </div>
        <Select aria-label="Access of the new user" value={level} items={LEVELS} onValueChange={(next) => next && setLevel(String(next))} className="w-32" />
        <Button type="submit" variant="primary" loading={busy}>
          Invite
        </Button>
      </div>
      {error && <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={error} />}
    </form>
  );
}

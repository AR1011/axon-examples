import { Table } from "@cloudflare/kumo";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { call, get, seg, workerPath, type ObjectRow, type ObjectStorage } from "../../api";
import { Card, CLIP, Confirm, Empty, Failed, Field, Loading, Modal, MONO, Notice, NUM, Status, useNotify } from "../../components/ui";
import { bytes } from "../../format";

type Picked = { class: string; id: string };
const same = (a: Picked, b: Picked | null) => !!b && a.class === b.class && a.id === b.id;

function Storage({ name, picked, onClose, onCleared }: { name: string; picked: Picked; onClose: () => void; onCleared: () => void }) {
  const path = workerPath(name, `/objects/${seg(picked.class)}/${seg(picked.id)}`);
  const storage = useQuery({ queryKey: ["object", name, picked.class, picked.id], queryFn: ({ signal }) => get<ObjectStorage>(path, signal) });
  const clear = useMutation({ mutationFn: () => call<{ cleared: Picked }>("DELETE", path), onSuccess: onCleared });
  return (
    <Modal title={`${picked.class} ${picked.id}`} onClose={onClose}>
      {storage.isPending ? (
        <Loading what="the storage" />
      ) : storage.error ? (
        <Failed what="the storage" error={storage.error} />
      ) : (
        <>
          <dl>
            <Field label="Class">{storage.data.class}</Field>
            <Field label="ID">{storage.data.id}</Field>
            <Field label="Epoch of the last write">{storage.data.epoch}</Field>
            <Field label="Size">{bytes(storage.data.bytes)}</Field>
            <Field label="Keys">{storage.data.entries.length}</Field>
          </dl>
          {storage.data.entries.length === 0 ? (
            <Empty title="No keys" detail="This object stores nothing." />
          ) : (
            <div className="shrink-0 overflow-hidden rounded-lg ring ring-kumo-line">
              <Table aria-label="Storage keys" layout="fixed">
                <Table.Header>
                  <Table.Row>
                    <Table.Head className="w-[40%]">Key</Table.Head>
                    <Table.Head>Value</Table.Head>
                  </Table.Row>
                </Table.Header>
                <Table.Body>
                  {storage.data.entries.map((entry) => {
                    const value = JSON.stringify(entry.value);
                    return (
                      <Table.Row key={entry.key}>
                        <Table.Cell className={`${MONO} truncate`} title={entry.key}>
                          {entry.key}
                        </Table.Cell>
                        <Table.Cell className={`${MONO} truncate`} title={value}>
                          {value}
                        </Table.Cell>
                      </Table.Row>
                    );
                  })}
                </Table.Body>
              </Table>
            </div>
          )}
          <Notice error={clear.error} />
          <div className="flex items-center gap-4">
            <p className="text-sm text-kumo-subtle">If the object runs, its next request fails and stops it. The request after that starts a new object with no storage.</p>
            <span className="ml-auto shrink-0">
              <Confirm label="Delete storage" size="base" question={`Delete all that ${picked.class} ${picked.id} stores?`} detail="You cannot get it back." yes="Delete storage" busy={clear.isPending} onConfirm={() => clear.mutate()} />
            </span>
          </div>
        </>
      )}
    </Modal>
  );
}

export function Objects({ name }: { name: string }) {
  const queries = useQueryClient();
  const notify = useNotify();
  const objects = useQuery({ queryKey: ["objects", name], queryFn: ({ signal }) => get<{ objects: ObjectRow[]; silent: string[] }>(workerPath(name, "/objects"), signal), refetchInterval: 5000 });
  const [picked, setPicked] = useState<Picked | null>(null);
  const rows = objects.data?.objects ?? [];
  return (
    <>
      {objects.data && objects.data.silent.length > 0 && <Notice tone="warning">These runners did not answer: {objects.data.silent.join(", ")}. Their objects are not in the list.</Notice>}
      <Card label="Objects">
        {objects.isPending ? (
          <Loading what="the objects" />
        ) : objects.error ? (
          <Failed what="the objects" error={objects.error} />
        ) : rows.length === 0 ? (
          <Empty title="No objects" detail="No Durable Object of this Worker runs or stores data. An object starts at its first request." />
        ) : (
          <div className="overflow-x-auto">
            <Table className="min-w-[760px]">
              <Table.Header>
                <Table.Row>
                  <Table.Head>Class</Table.Head>
                  <Table.Head className="w-[30%]">ID</Table.Head>
                  <Table.Head>State</Table.Head>
                  <Table.Head>Runner</Table.Head>
                  <Table.Head className="text-right">Epoch</Table.Head>
                  <Table.Head>Storage</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {rows.map((row) => (
                  <Table.Row key={`${row.class}/${row.id}`} tabIndex={0} variant={same(row, picked) ? "selected" : "default"} className="cursor-pointer hover:bg-kumo-tint" onClick={() => setPicked(row)} onKeyDown={(e) => e.key === "Enter" && setPicked(row)}>
                    <Table.Cell className={MONO}>{row.class}</Table.Cell>
                    <Table.Cell className={`${MONO} ${CLIP}`} title={row.id}>
                      {row.id}
                    </Table.Cell>
                    <Table.Cell>
                      <Status tone={row.node ? "success" : "muted"}>{row.node ? "Running" : "Not running"}</Status>
                    </Table.Cell>
                    <Table.Cell className={MONO}>{row.node ?? "—"}</Table.Cell>
                    <Table.Cell className={NUM}>{row.epoch ?? "—"}</Table.Cell>
                    <Table.Cell className={row.stored ? "" : "text-kumo-subtle"}>{row.stored ? "Has data" : "Empty"}</Table.Cell>
                  </Table.Row>
                ))}
              </Table.Body>
            </Table>
          </div>
        )}
      </Card>
      {picked && (
        <Storage
          name={name}
          picked={picked}
          onClose={() => setPicked(null)}
          onCleared={() => {
            notify(`The storage of ${picked.class} ${picked.id} is deleted.`);
            setPicked(null);
            void queries.invalidateQueries({ queryKey: ["objects", name] });
            queries.removeQueries({ queryKey: ["object", name, picked.class, picked.id] });
          }}
        />
      )}
    </>
  );
}

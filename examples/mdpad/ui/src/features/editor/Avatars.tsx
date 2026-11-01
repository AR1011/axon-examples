import { Popover } from "@cloudflare/kumo";
import type { Peer } from "./session";

/** At most this many avatars; the rest are "+N". */
const SHOWN = 6;

const counted = (n: number, one: string) => `${n} ${one}${n === 1 ? "" : "s"}`;

const initials = (name: string) =>
  name
    .split(/[\s._-]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => [...part][0].toUpperCase())
    .join("") || "?";

function Avatar({ name, colour }: { name: string; colour: string }) {
  return (
    <span className="flex size-7 shrink-0 items-center justify-center rounded-full text-[11px] font-semibold text-white ring-2 ring-kumo-base" style={{ backgroundColor: colour }} aria-hidden="true">
      {initials(name)}
    </span>
  );
}

/** The others with the document open, one avatar per user; a list of them, and the viewers' count, on a click. */
export function Avatars({ peers, viewers }: { peers: Peer[]; viewers: number }) {
  if (!peers.length && !viewers) return null;
  const more = peers.length - SHOWN;
  const label = [peers.length && counted(peers.length, "other editor"), viewers && counted(viewers, "viewer")].filter(Boolean).join(", ");
  return (
    <Popover>
      <Popover.Trigger
        render={
          <button type="button" className="flex items-center rounded-full p-0.5 -space-x-1.5 outline-offset-2 hover:opacity-90" aria-label={`People here: ${label}`} data-testid="avatars">
            {peers.slice(0, SHOWN).map((peer) => (
              <Avatar key={peer.name} {...peer} />
            ))}
            {more > 0 && <span className="flex size-7 items-center justify-center rounded-full bg-kumo-recessed text-[11px] font-medium ring-2 ring-kumo-base">+{more}</span>}
            {!peers.length && <span className="px-1 text-sm text-kumo-subtle">{viewers} viewing</span>}
          </button>
        }
      />
      <Popover.Content className="w-60">
        <Popover.Title>People here</Popover.Title>
        {peers.length > 0 && (
          <ul className="mt-2 flex max-h-72 flex-col gap-1.5 overflow-auto">
            {peers.map((peer) => (
              <li key={peer.name} className="flex items-center gap-2 text-sm">
                <Avatar {...peer} />
                <span className="truncate">{peer.name}</span>
              </li>
            ))}
          </ul>
        )}
        {viewers > 0 && <p className="mt-2 text-sm text-kumo-subtle">{`${peers.length ? "and " : ""}${counted(viewers, "viewer")}`}</p>}
      </Popover.Content>
    </Popover>
  );
}

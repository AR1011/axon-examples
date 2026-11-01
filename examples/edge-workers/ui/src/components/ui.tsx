import { Badge, Banner, Button, cn, Dialog, Empty as KumoEmpty, LayerCard, Link, Loader, Select, Tabs, Tooltip, useKumoToastManager } from "@cloudflare/kumo";
import { CheckIcon, CopyIcon, InfoIcon, WarningCircleIcon, WarningIcon, XIcon } from "@phosphor-icons/react";
import { useState, type ComponentProps, type ReactNode } from "react";
import { Refusal, type WorkerUrl } from "../api";
import { ago, clock, type Tone } from "../format";
import { useNow } from "../hooks";

const BADGE = { success: "success", warning: "warning", danger: "error", muted: "neutral" } as const;
export const TONES: Record<Tone, string> = { success: "text-kumo-success", warning: "text-kumo-warning", danger: "text-kumo-danger", muted: "text-kumo-subtle" };

/** A state: a dot in the state's colour, and its name. */
export function Status({ tone, children }: { tone: Tone; children: ReactNode }) {
  return (
    <Badge variant={BADGE[tone]} appearance="dot" className="whitespace-nowrap">
      {children}
    </Badge>
  );
}

/** A response status, or another short value in a state's colour. */
export function Code({ tone, children }: { tone: Tone; children: ReactNode }) {
  return (
    <Badge variant={tone === "muted" ? "outline" : BADGE[tone]} className="font-mono whitespace-nowrap">
      {children}
    </Badge>
  );
}

export function Empty({ title, detail, children }: { title: string; detail?: string; children?: ReactNode }) {
  return <KumoEmpty size="sm" title={title} description={detail} contents={children} className="border-0 bg-transparent shadow-none ring-0" />;
}

export function Loading({ what }: { what: string }) {
  return (
    <div className="flex min-h-[120px] flex-1 items-center justify-center gap-2 p-4 text-sm text-kumo-subtle">
      <Loader size="sm" />
      Loading {what}…
    </div>
  );
}

/** What the API refused, as it said it: the status, its code and its sentence. */
export function refusalText(error: unknown): string {
  if (error instanceof Refusal) return error.status ? `${error.status} ${error.code}: ${error.message}` : error.message;
  return error instanceof Error ? error.message : String(error);
}

const BANNER = { danger: ["error", WarningCircleIcon], warning: ["alert", WarningIcon], success: ["default", InfoIcon], muted: ["secondary", InfoIcon] } as const;

/** A message in the page: a refusal of the API, a warning, or a fact to know. */
export function Notice({ error, tone = "danger", children, action, className }: { error?: unknown; tone?: Tone; children?: ReactNode; action?: ReactNode; className?: string }) {
  if (!error && !children) return null;
  const [variant, Icon] = BANNER[error ? "danger" : tone];
  return <Banner size="sm" variant={variant} icon={<Icon weight="fill" />} role={error ? "alert" : "status"} description={error ? refusalText(error) : children} action={action} className={cn("break-words whitespace-pre-wrap", className)} />;
}

/** A view that failed to load: the refusal, in the place of the rows. */
export function Failed({ what, error }: { what: string; error: unknown }) {
  return (
    <div className="flex min-h-[120px] flex-1 flex-col items-center justify-center gap-3 p-4 text-center">
      <p className="text-base font-medium">The console cannot load {what}.</p>
      <Notice error={error} className="w-auto max-w-full text-left" />
    </div>
  );
}

/** A success that needs no place in the page: a toast. */
export function useNotify() {
  const toasts = useKumoToastManager();
  return (title: string) => void toasts.add({ title, variant: "success" });
}

export function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex min-w-0 items-baseline justify-between gap-4 border-b border-kumo-hairline py-2 last:border-0">
      <dt className="shrink-0 text-sm text-kumo-subtle">{label}</dt>
      <dd className="min-w-0 truncate text-right font-mono text-sm">{children}</dd>
    </div>
  );
}

/** A number with its name above it. Put several in a `Stats`. */
export function Stat({ label, value, sub, tone, hint }: { label: string; value: ReactNode; sub?: string; tone?: Tone; hint?: string }) {
  const name = <span className="truncate text-sm text-kumo-subtle">{label}</span>;
  return (
    <div className="flex min-w-0 flex-col gap-1 bg-kumo-base px-4 py-3">
      {hint ? <Tooltip content={hint} render={<span className="w-fit max-w-full cursor-help truncate text-sm text-kumo-subtle underline decoration-dotted underline-offset-4" />}>{label}</Tooltip> : name}
      <span className={cn("truncate text-xl font-semibold tabular-nums", tone && TONES[tone])}>
        {value}
        {sub && <span className="ml-2 text-sm font-normal text-kumo-subtle">{sub}</span>}
      </span>
    </div>
  );
}

export function Stats({ label, className, children }: { label: string; className: string; children: ReactNode }) {
  return (
    <LayerCard render={<section aria-label={label} />} className={cn("grid gap-px bg-kumo-line", className)}>
      {children}
    </LayerCard>
  );
}

/** A table's time and its age below it. */
export function Stamp({ ms }: { ms: number }) {
  const now = useNow();
  return (
    <>
      <time className="block font-mono text-sm leading-tight" dateTime={new Date(ms).toISOString()} title={new Date(ms).toLocaleString()}>
        {clock(ms, true)}
      </time>
      <span className="block text-xs leading-tight text-kumo-subtle">{ago(now - ms)}</span>
    </>
  );
}

interface CardProps {
  title?: ReactNode;
  controls?: ReactNode;
  footer?: ReactNode;
  children: ReactNode;
  label?: string;
  className?: string;
}

/** A card of the page: a header with its name and controls, the content, and a note below. */
export function Card({ title, controls, footer, children, label, className }: CardProps) {
  return (
    <LayerCard render={<section aria-label={label} />} className={cn("flex min-w-0 flex-col", className)}>
      {(title || controls) && (
        <header className="flex min-h-13 shrink-0 flex-wrap items-center gap-x-3 gap-y-2 border-b border-kumo-line px-4 py-2">
          {title && <h2 className="flex min-w-0 items-center gap-2 text-base font-semibold">{title}</h2>}
          <div className="ml-auto flex flex-wrap items-center gap-2">{controls}</div>
        </header>
      )}
      {children}
      {footer && <footer className="shrink-0 border-t border-kumo-line bg-kumo-elevated px-4 py-2.5 text-sm text-kumo-subtle">{footer}</footer>}
    </LayerCard>
  );
}

const SHEET =
  "!top-0 right-0 !left-auto h-dvh !w-[600px] !max-w-full !translate-x-0 rounded-none transition-transform ease-out data-ending-style:!translate-x-full data-ending-style:scale-100 data-ending-style:opacity-100 data-starting-style:!translate-x-full data-starting-style:scale-100 data-starting-style:opacity-100";

/** A dialog over the page, or a sheet at its right side. Escape, the button and a click outside close it. */
export function Modal({ title, onClose, children, size = "lg", sheet }: { title: string; onClose: () => void; children: ReactNode; size?: "base" | "lg" | "xl"; sheet?: boolean }) {
  return (
    <Dialog.Root open onOpenChange={(open) => !open && onClose()}>
      <Dialog size={size} className={cn("flex flex-col", sheet ? SHEET : "max-h-[calc(100dvh-4rem)] sm:top-8")}>
        <div className="flex shrink-0 items-center gap-3 border-b border-kumo-line px-5 py-3">
          <Dialog.Title className="min-w-0 truncate text-lg font-semibold">{title}</Dialog.Title>
          <Dialog.Close render={<Button variant="ghost" shape="square" size="sm" aria-label="Close" icon={XIcon} className="ml-auto" />} />
        </div>
        <div className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto p-5">{children}</div>
      </Dialog>
    </Dialog.Root>
  );
}

interface ConfirmProps {
  label: string;
  question: string;
  detail?: string;
  yes: string;
  busy?: boolean;
  onConfirm: () => void;
  /** A change that loses nothing asks with a plain button. */
  plain?: boolean;
  size?: ComponentProps<typeof Button>["size"];
}

/** A button that asks before it acts, in a dialog of the page. */
export function Confirm({ label, question, detail, yes, busy, onConfirm, plain, size = "sm" }: ConfirmProps) {
  const [open, setOpen] = useState(false);
  return (
    <Dialog.Root role="alertdialog" open={open} onOpenChange={setOpen}>
      <Button variant={plain ? "secondary" : "secondary-destructive"} size={size} disabled={busy} onClick={() => setOpen(true)}>
        {label}
      </Button>
      <Dialog size="base" className="p-6">
        <Dialog.Title className="text-lg font-semibold">{question}</Dialog.Title>
        {detail && <Dialog.Description className="mt-2 text-base text-kumo-subtle">{detail}</Dialog.Description>}
        <div className="mt-6 flex justify-end gap-2">
          <Button variant="secondary" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button
            variant={plain ? "primary" : "destructive"}
            onClick={() => {
              setOpen(false);
              onConfirm();
            }}
          >
            {yes}
          </Button>
        </div>
      </Dialog>
    </Dialog.Root>
  );
}

export function Segmented<T extends string>({ label, value, options, onChange }: { label: string; value: T; options: { value: T; label: string }[]; onChange: (value: T) => void }) {
  return (
    <div role="group" aria-label={label}>
      <Tabs variant="segmented" tabs={options} value={value} onValueChange={(next) => onChange(next as T)} />
    </div>
  );
}

/** One choice of a short list. */
export function Pick({ label, value, options, onChange, className }: { label: string; value: string; options: { value: string; label: string }[]; onChange: (value: string) => void; className?: string }) {
  return <Select aria-label={label} value={value} onValueChange={(next) => next != null && onChange(String(next))} items={options} className={className} />;
}

export function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <Tooltip
      content={copied ? "Copied" : "Copy"}
      render={
        <Button
          variant="ghost"
          shape="square"
          size="xs"
          aria-label={label}
          icon={copied ? CheckIcon : CopyIcon}
          onClick={(event) => {
            event.stopPropagation();
            void navigator.clipboard?.writeText(text).then(
              () => {
                setCopied(true);
                setTimeout(() => setCopied(false), 1500);
              },
              () => null,
            );
          }}
        />
      }
    />
  );
}

/** Where a gateway serves a Worker: a link to it, and a button that copies it. */
export function WorkerLink({ at, tag }: { at: WorkerUrl; tag?: boolean }) {
  return (
    <span className="inline-flex max-w-full min-w-0 items-center gap-1.5">
      {tag && <Badge variant="outline">{at.gateway}</Badge>}
      <Link href={at.url} target="_blank" rel="noreferrer" onClick={(event) => event.stopPropagation()} className="min-w-0 truncate font-mono text-sm">
        {at.url}
      </Link>
      <CopyButton text={at.url} label={`Copy the URL at ${at.gateway}`} />
    </span>
  );
}

/** A named block of text as it came: headers, a body, console lines. */
export function Block({ title, empty, note, children }: { title: string; empty: string; note?: string; children: string }) {
  return (
    <section aria-label={title} className="flex flex-col gap-1.5">
      <h3 className="text-sm font-medium text-kumo-subtle">{title}</h3>
      {children ? <pre className="max-h-[240px] overflow-auto rounded-lg bg-kumo-recessed p-3 font-mono text-sm leading-normal break-words whitespace-pre-wrap ring ring-kumo-hairline">{children}</pre> : <p className="text-sm text-kumo-subtle">{empty}</p>}
      {note && <p className="text-sm text-kumo-warning">{note}</p>}
    </section>
  );
}

/** Classes for a table's cells. */
export const NUM = "text-right font-mono text-sm tabular-nums whitespace-nowrap";
export const MONO = "font-mono text-sm";
export const CLIP = "max-w-0 truncate";

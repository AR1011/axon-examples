import { LayerCard } from "@cloudflare/kumo";
import type { ReactNode } from "react";
import { Mark } from "./Mark";

/** The pages before the app: the sign-in and those a link from outside opens. One card under the mark. */
export function Frame({ title, text, children }: { title: string; text?: ReactNode; children?: ReactNode }) {
  return (
    <main className="flex min-h-dvh flex-col items-center justify-center gap-6 p-4 pb-[12vh]">
      <span className="flex items-center gap-2.5 text-lg font-semibold">
        <Mark size={28} />
        mdpad
      </span>
      <LayerCard className="flex w-full max-w-[380px] flex-col gap-5 p-7">
        <div className="flex flex-col gap-1">
          <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
          {text && <p className="text-base text-kumo-subtle">{text}</p>}
        </div>
        {children}
      </LayerCard>
    </main>
  );
}

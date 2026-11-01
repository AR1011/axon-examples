import { Loader } from "@cloudflare/kumo";
import { useEffect, useState } from "react";
import { useParams } from "react-router";
import { at } from "../../api";
import { Frame } from "../../shell/Frame";
import { titleOf } from "../editor/describe";
import { render } from "../editor/Preview";

/** A published document: its text rendered in a reading column, and nothing else. No socket, no sign-in. */
export function PublishedPage() {
  const { id = "" } = useParams();
  // The text; null when it is not published; false when the server did not answer.
  const [text, setText] = useState<string | null | false | undefined>(undefined);

  useEffect(() => {
    let current = true;
    const read = async (): Promise<string | null | false> => {
      try {
        const response = await fetch(at(`api/p/${encodeURIComponent(id)}`), { credentials: "omit" });
        return response.ok ? await response.text() : response.status === 404 ? null : false;
      } catch {
        return false;
      }
    };
    void read().then((answer) => current && setText(answer));
    return () => {
      current = false;
    };
  }, [id]);

  useEffect(() => {
    document.title = text ? `${titleOf(text) ?? "Untitled"} - mdpad` : "mdpad";
  }, [text]);

  if (text === undefined) {
    return (
      <main className="flex h-dvh items-center justify-center" aria-busy="true">
        <Loader />
      </main>
    );
  }
  if (text === null) return <Frame title="This page is not published" text="Its owner stopped it, or the address is wrong." />;
  if (text === false) return <Frame title="Cannot connect to the server" text="Try again later." />;
  return (
    <main className="min-h-dvh bg-kumo-base">
      <article className="md-preview mx-auto max-w-[760px] px-4 py-10 sm:px-6" dangerouslySetInnerHTML={{ __html: render(text) }} />
    </main>
  );
}

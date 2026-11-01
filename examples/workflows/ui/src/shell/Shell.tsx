import { cn } from "@cloudflare/kumo";
import { ChartLineIcon, FlowArrowIcon, ListChecksIcon, SidebarSimpleIcon, StackIcon, TreeStructureIcon, type Icon } from "@phosphor-icons/react";
import { Suspense, useState } from "react";
import { Link, Outlet, useLocation } from "react-router";
import { Loading } from "../components/ui";
import { Health } from "./Health";
import { Mark } from "./Mark";

const PINNED = "wf-sidebar";

function stored(): boolean {
  try {
    return localStorage.getItem(PINNED) === "open";
  } catch {
    return false;
  }
}

/** Text of the sidebar: it shows only while the sidebar is open. */
const label = "min-w-0 truncate opacity-0 transition-opacity duration-150 ease-out group-data-[open]/rail:opacity-100";
const row = "mx-3 flex h-8 shrink-0 items-center overflow-hidden rounded-md text-base text-kumo-default outline-none hover:bg-kumo-tint focus-visible:ring-2 focus-visible:ring-kumo-ring";

/** The name of a group of items. A closed sidebar shows a line in its place. */
function Heading({ children }: { children: string }) {
  return (
    <div className="relative mt-2 flex h-8 shrink-0 items-center px-5">
      <span aria-hidden="true" className="absolute inset-x-3 top-1/2 border-t border-kumo-line transition-opacity duration-150 ease-out group-data-[open]/rail:opacity-0" />
      <span className={cn(label, "text-xs font-medium text-kumo-subtle")}>{children}</span>
    </div>
  );
}

/** An item with an icon. A closed sidebar shows only the icon. */
function Item({ icon: IconOf, to, active, children }: { icon: Icon; to: string; active: boolean; children: string }) {
  return (
    <Link to={to} aria-label={children} aria-current={active ? "page" : undefined} className={cn(row, active && "bg-kumo-tint font-medium")}>
      <span className="flex size-8 shrink-0 items-center justify-center">
        <IconOf size={18} weight={active ? "fill" : "regular"} />
      </span>
      <span className={label}>{children}</span>
    </Link>
  );
}

/**
 * The frame of the console. The sidebar is a rail of icons until the pointer or the keyboard is in it: then it opens
 * above the page, which does not move. Its button, always at the same place, keeps it open beside the page.
 */
export function Shell() {
  const { pathname } = useLocation();
  const [pinned, setPinned] = useState(stored);
  const [hover, setHover] = useState(false);
  const [focus, setFocus] = useState(false);
  const open = pinned || hover || focus;
  const at = (path: string) => pathname === path || pathname.startsWith(`${path}/`);
  const pin = pinned ? "Close the sidebar" : "Keep the sidebar open";

  const toggle = () => {
    try {
      localStorage.setItem(PINNED, pinned ? "closed" : "open");
    } catch {
      /* no storage: the choice holds for this page only */
    }
    setPinned(!pinned);
  };

  return (
    <div className="isolate flex h-dvh">
      <div className={cn("relative z-20 shrink-0 transition-[width] duration-150 ease-out", pinned ? "w-60" : "w-14")}>
        <nav
          aria-label="Sidebar"
          data-open={open ? "" : undefined}
          data-state={pinned ? "pinned" : open ? "over" : "closed"}
          onMouseEnter={() => setHover(true)}
          onMouseLeave={() => setHover(false)}
          onFocus={(e) => setFocus(e.target.matches(":focus-visible"))}
          onBlur={() => setFocus(false)}
          className={cn("group/rail absolute inset-y-0 left-0 flex flex-col overflow-hidden border-r border-kumo-line bg-kumo-base transition-[width] duration-150 ease-out", open ? "w-60" : "w-14", open && !pinned && "shadow-xl")}
        >
          <Link to="/overview" aria-label="workflows" className="flex h-12 shrink-0 items-center gap-2.5 border-b border-kumo-line px-4 outline-none focus-visible:bg-kumo-tint">
            <Mark />
            <span className={cn(label, "text-base font-semibold")}>workflows</span>
          </Link>
          <div className="flex min-h-0 flex-1 flex-col overflow-x-hidden overflow-y-auto pb-2">
            <Heading>Runs</Heading>
            <Item icon={ChartLineIcon} to="/overview" active={at("/overview")}>
              Overview
            </Item>
            <Item icon={ListChecksIcon} to="/runs" active={at("/runs")}>
              Runs
            </Item>
            <Item icon={FlowArrowIcon} to="/workflows" active={at("/workflows")}>
              Workflows
            </Item>
            <Heading>Platform</Heading>
            <Item icon={StackIcon} to="/queues" active={at("/queues")}>
              Queues and workers
            </Item>
            <Item icon={TreeStructureIcon} to="/cluster" active={at("/cluster")}>
              Cluster
            </Item>
          </div>
          <div className="flex h-12 shrink-0 items-center border-t border-kumo-line px-3">
            <button
              type="button"
              aria-expanded={pinned}
              aria-label={pin}
              title={pin}
              onClick={toggle}
              className="flex size-8 shrink-0 cursor-pointer items-center justify-center rounded-md text-kumo-subtle outline-none hover:bg-kumo-tint hover:text-kumo-default focus-visible:ring-2 focus-visible:ring-kumo-ring"
            >
              <SidebarSimpleIcon size={18} weight={pinned ? "fill" : "regular"} />
            </button>
          </div>
        </nav>
      </div>
      <main id="content" className="flex min-w-0 flex-1 flex-col overflow-y-auto">
        <Health>
          <Suspense fallback={<Loading what="the page" />}>
            <Outlet />
          </Suspense>
        </Health>
      </main>
    </div>
  );
}

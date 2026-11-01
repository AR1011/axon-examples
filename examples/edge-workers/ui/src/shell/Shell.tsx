import { cn } from "@cloudflare/kumo";
import { CodeIcon, SidebarSimpleIcon, TreeStructureIcon, type Icon } from "@phosphor-icons/react";
import { Suspense, useState, type ReactNode } from "react";
import { Link, Outlet, useLocation } from "react-router";
import { Loading } from "../components/ui";
import { Mark } from "./Mark";

const PINNED = "edge-sidebar";

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

/** An item with an icon. A closed sidebar shows only the icon. An item with items below it is filled only then. */
function Item({ icon: IconOf, to, active, parent = false, children }: { icon: Icon; to: string; active: boolean; parent?: boolean; children: string }) {
  return (
    <Link to={to} aria-label={children} aria-current={active && !parent ? "page" : undefined} className={cn(row, active && "bg-kumo-tint font-medium", active && parent && "group-data-[open]/rail:bg-transparent group-data-[open]/rail:hover:bg-kumo-tint")}>
      <span className="flex size-8 shrink-0 items-center justify-center">
        <IconOf size={18} weight={active ? "fill" : "regular"} />
      </span>
      <span className={label}>{children}</span>
    </Link>
  );
}

/** The items below an item, beside a line that joins them. */
function SubItems({ children }: { children: ReactNode }) {
  return <div className="mt-0.5 mr-3 ml-7 flex shrink-0 flex-col gap-0.5 border-l border-kumo-line pl-3 opacity-0 transition-opacity duration-150 ease-out group-data-[open]/rail:opacity-100">{children}</div>;
}

function SubItem({ to, active, children }: { to: string; active: boolean; children: string }) {
  return (
    <Link to={to} aria-current={active ? "page" : undefined} className={cn(row, "mx-0 px-2.5", active && "bg-kumo-tint font-medium")}>
      <span className="truncate">{children}</span>
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
  const workers = pathname.startsWith("/workers");
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
          <Link to="/workers" aria-label="edge workers" className="flex h-12 shrink-0 items-center gap-2.5 border-b border-kumo-line px-4 outline-none focus-visible:bg-kumo-tint">
            <Mark />
            <span className={cn(label, "text-base font-semibold")}>edge workers</span>
          </Link>
          <div className="flex min-h-0 flex-1 flex-col overflow-x-hidden overflow-y-auto pb-2">
            <Heading>Build</Heading>
            <Item icon={CodeIcon} to="/workers" active={workers} parent>
              Compute
            </Item>
            <SubItems>
              <SubItem to="/workers" active={workers}>
                Workers
              </SubItem>
            </SubItems>
            <Heading>Platform</Heading>
            <Item icon={TreeStructureIcon} to="/cluster" active={pathname.startsWith("/cluster")}>
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
        <Suspense fallback={<Loading what="the page" />}>
          <Outlet />
        </Suspense>
      </main>
    </div>
  );
}

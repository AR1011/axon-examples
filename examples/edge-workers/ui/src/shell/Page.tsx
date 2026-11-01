import { Breadcrumbs, Button, cn, Tooltip } from "@cloudflare/kumo";
import { MoonIcon, SignOutIcon, SunIcon } from "@phosphor-icons/react";
import { Fragment, type ReactNode } from "react";
import { base, call } from "../api";
import { useTheme } from "../hooks";

async function signOut() {
  await call("POST", "logout").catch(() => null);
  location.assign(`${base}login`);
}

interface Props {
  /** The way to this page. The last one is the page. */
  crumbs: { label: string; href?: string }[];
  title: ReactNode;
  /** Beside the title: a state. */
  badge?: ReactNode;
  /** Beside the title, after the state. */
  detail?: ReactNode;
  actions?: ReactNode;
  /** Below the bar: the parts of the page. */
  tabs?: ReactNode;
  /** How much of the window the content takes: `fill` is all its width and, on a wide window, all its height. */
  width?: keyof typeof WIDTHS;
  children: ReactNode;
}

const WIDTHS = {
  narrow: "mx-auto max-w-[1280px] px-8 py-6 max-lg:px-4",
  wide: "mx-auto max-w-[1440px] px-8 py-6 max-lg:px-4",
  full: "p-6 max-lg:px-4",
  fill: "p-6 max-lg:px-4 xl:min-h-[42rem] xl:flex-1 xl:basis-0",
};

/** A page of the console: the bar with the way to it, its title, and its content. */
export function Page({ crumbs, title, badge, detail, actions, tabs, width = "wide", children }: Props) {
  const [theme, toggleTheme] = useTheme();
  const other = theme === "dark" ? "light" : "dark";
  return (
    <>
      <div className="sticky top-0 z-10 shrink-0 bg-kumo-base">
        <div className="flex h-12 items-center gap-2 border-b border-kumo-line px-4">
          <Breadcrumbs size="sm">
            {crumbs.map((crumb, i) => (
              <Fragment key={crumb.label}>
                {i > 0 && <Breadcrumbs.Separator />}
                {crumb.href ? <Breadcrumbs.Link href={crumb.href}>{crumb.label}</Breadcrumbs.Link> : <Breadcrumbs.Current>{crumb.label}</Breadcrumbs.Current>}
              </Fragment>
            ))}
          </Breadcrumbs>
          <div className="ml-auto flex items-center gap-1">
            <Tooltip content={`Use the ${other} theme`} render={<Button variant="ghost" shape="square" size="sm" aria-label={`Use the ${other} theme`} icon={theme === "dark" ? SunIcon : MoonIcon} onClick={toggleTheme} />} />
            <Button variant="ghost" size="sm" icon={SignOutIcon} onClick={() => void signOut()}>
              Sign out
            </Button>
          </div>
        </div>
        {tabs && <div className="flex h-12 items-center overflow-x-auto border-b border-kumo-line px-4">{tabs}</div>}
      </div>
      <div className={cn("flex w-full flex-col gap-4", WIDTHS[width])}>
        <header className="flex flex-wrap items-start gap-x-4 gap-y-3">
          <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-2">
            <h1 className="min-w-0 truncate text-2xl font-semibold tracking-tight">{title}</h1>
            {badge}
            {detail}
          </div>
          {actions && <div className="ml-auto flex items-center gap-2">{actions}</div>}
        </header>
        {children}
      </div>
    </>
  );
}

import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router";
import { signInOn401, whoAmI, type Me } from "../api";
import { Account } from "./Account";
import { Mark } from "./Mark";

/** Who is signed in; the login page when no one is. */
export const useMe = () => useQuery({ queryKey: ["me"], queryFn: signInOn401(whoAmI) });

/** The top bar of the documents and the admin page: the mark, and the account menu. */
export function TopBar({ me }: { me: Me | undefined }) {
  return (
    <header className="flex h-12 shrink-0 items-center gap-2 border-b border-kumo-hairline px-3">
      <Link to="/" className="flex items-center gap-2 rounded-md font-semibold">
        <Mark size={22} />
        mdpad
      </Link>
      <span className="flex-1" />
      <Account user={me?.user ?? null} operator={me?.operator ?? false} />
    </header>
  );
}

import { LinkProvider, Toasty, TooltipProvider, type LinkComponentProps } from "@cloudflare/kumo";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { forwardRef, StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Link } from "react-router";
import { base } from "./api";
import { App } from "./App";
import { applyTheme } from "./hooks";
import { POLL } from "./queries";
import "./app.css";

applyTheme();

const queries = new QueryClient({ defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false, staleTime: POLL } } });

/** The links of the component library go through the router. One to another site is a plain link. */
const AppLink = forwardRef<HTMLAnchorElement, LinkComponentProps>(function AppLink({ href = "", to, ...rest }, ref) {
  const target = to ?? href;
  return /^[a-z][a-z0-9+.-]*:/i.test(target) ? <a ref={ref} href={target} {...rest} /> : <Link ref={ref} to={target} {...rest} />;
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queries}>
      <BrowserRouter basename={base.replace(/\/$/, "") || "/"}>
        <LinkProvider component={AppLink}>
          <TooltipProvider>
            <Toasty>
              <App />
            </Toasty>
          </TooltipProvider>
        </LinkProvider>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);

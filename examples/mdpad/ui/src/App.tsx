import { Empty } from "@cloudflare/kumo";
import { lazy, Suspense } from "react";
import { Link, Route, Routes } from "react-router";
import { DocsPage } from "./features/docs/DocsPage";
import { GuestPage, InvitePage, LinkPage } from "./features/login/Entry";
import { LoginPage } from "./features/login/LoginPage";

const AdminPage = lazy(() => import("./features/admin/AdminPage").then((m) => ({ default: m.AdminPage })));
const PublishedPage = lazy(() => import("./features/published/PublishedPage").then((m) => ({ default: m.PublishedPage })));
const EditorPage = lazy(() => import("./features/editor/EditorPage").then((m) => ({ default: m.EditorPage })));

export function App() {
  return (
    <Routes>
      <Route path="login" element={<LoginPage />} />
      <Route path="i/:token" element={<InvitePage />} />
      <Route path="s/:token" element={<LinkPage />} />
      <Route path="g/:token" element={<GuestPage />} />
      <Route index element={<DocsPage />} />
      <Route
        path="admin"
        element={
          <Suspense fallback={<main className="h-dvh" aria-busy="true" />}>
            <AdminPage />
          </Suspense>
        }
      />
      <Route
        path="p/:id"
        element={
          <Suspense fallback={<main className="h-dvh" aria-busy="true" />}>
            <PublishedPage />
          </Suspense>
        }
      />
      <Route
        path="d/:id"
        element={
          <Suspense fallback={<main className="h-dvh" aria-busy="true" />}>
            <EditorPage />
          </Suspense>
        }
      />
      <Route path="*" element={<Empty className="m-8" title="Page not found" description="Go to the documents." contents={<Link to="/">Documents</Link>} />} />
    </Routes>
  );
}

import {
  A,
  Route,
  Router,
  useLocation,
  type RouteSectionProps,
} from "@solidjs/router";
import {
  For,
  Show,
  createEffect,
  createMemo,
  createResource,
  createSignal,
  onCleanup,
} from "solid-js";
import { managerClient, managerEndpoint } from "./grpcClient";
import { NodeMsg } from "./gen/legion_pb";
import type { Job, LedgerNode } from "./gen/legion_pb";
import Homepage from "./pages/Homepage";
import Jobs from "./pages/Jobs";
import KeyValue from "./pages/KeyValue";
import Manager from "./pages/Manager";
import Nodes from "./pages/Nodes";
import TestingUtils from "./pages/TestingUtils";
import { POLL_INTERVAL_MS } from "./utils/polling";

const navItems = [
  {
    key: "overview",
    label: "Overview",
    path: "/",
    title: "Overview",
    copy: "Jobs running on the cluster.",
  },
  {
    key: "nodes",
    label: "Nodes",
    path: "/nodes",
    title: "Nodes",
    copy: "Every node the manager knows about.",
  },
  {
    key: "jobs",
    label: "Jobs",
    path: "/jobs",
    title: "Jobs",
    copy: "Jobs reported by the manager in real time.",
  },
  {
    key: "kv",
    label: "Key/Value",
    path: "/kv",
    title: "Key/Value",
    copy: "Browse and edit key/value entries.",
  },
  {
    key: "manager",
    label: "Manager",
    path: "/manager",
    title: "Manager",
    copy: "Manager config and controls.",
  },
  {
    key: "testing",
    label: "Testing Utils",
    path: "/testing",
    title: "Testing Utilities",
    copy: "Sandboxes and utilities for experiments.",
  },
] as const;

const resolveInitialTheme = (): "light" | "dark" => {
  if (typeof window === "undefined") return "dark";
  const stored = localStorage.getItem("ui-theme");
  if (stored === "light" || stored === "dark") return stored;
  return window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
};

const baseContainer =
  "bg-neutral-100 text-neutral-900 dark:bg-gradient-to-br dark:from-neutral-950 dark:via-neutral-900 dark:to-black dark:text-neutral-100";

const navButton = (active: boolean) =>
  `group flex w-full items-center justify-between rounded-lg border px-3 py-2.5 text-left text-sm font-medium transition ` +
  (active
    ? "border-neutral-900 bg-neutral-900 text-white shadow-sm dark:border-neutral-300 dark:bg-neutral-100 dark:text-neutral-900"
    : "border-transparent text-neutral-600 hover:border-neutral-200 hover:bg-white hover:text-neutral-900 hover:shadow-sm dark:text-neutral-300 dark:hover:border-neutral-700 dark:hover:bg-neutral-900 dark:hover:text-white");

const isEqualData = <T,>(a: T | undefined, b: T | undefined) =>
  JSON.stringify(a) === JSON.stringify(b);

const normalizeNodes = (nodes: readonly NodeMsg[]): readonly NodeMsg[] => {
  const priority = (node: NodeMsg) =>
    node.status?.toLowerCase() === "active" ? 0 : 1;
  return [...nodes].sort((a, b) => {
    const diff = priority(a) - priority(b);
    if (diff !== 0) return diff;
    return (a.id || "").localeCompare(b.id || "");
  });
};

const areNodesEqual = (
  a: readonly NodeMsg[] | undefined,
  b: readonly NodeMsg[] | undefined,
) => {
  if (a === b) return true;
  if (!a || !b || a.length !== b.length) return false;
  for (let index = 0; index < a.length; index += 1) {
    if (!NodeMsg.equals(a[index], b[index])) return false;
  }
  return true;
};

const Layout = (props: RouteSectionProps) => {
  const location = useLocation();
  const [isNavOpen, setIsNavOpen] = createSignal(false);
  const [isSidebarCollapsed, setIsSidebarCollapsed] = createSignal(false);
  const [theme, setTheme] = createSignal<"light" | "dark">(
    resolveInitialTheme(),
  );

  createEffect(() => {
    const root = document.documentElement;
    const current = theme();
    root.classList.toggle("dark", current === "dark");
    localStorage.setItem("ui-theme", current);
  });

  createEffect(() => {
    location.pathname;
    setIsNavOpen(false);
  });

  const isActivePath = (path: string) => {
    const current = location.pathname.replace(/\/+$/, "") || "/";
    const target = path.replace(/\/+$/, "") || "/";
    return current === target || (target !== "/" && current.startsWith(target));
  };

  const activeNav = createMemo(
    () => navItems.find((item) => isActivePath(item.path)) ?? navItems[0],
  );

  const toggleTheme = () =>
    setTheme((prev) => (prev === "dark" ? "light" : "dark"));

  return (
    <div class={`min-h-screen ${baseContainer}`}>
      <div class="flex min-h-screen">
        <aside
          class={`fixed inset-y-0 left-0 z-40 flex w-72 transform flex-col overflow-hidden border-r border-neutral-200/80 bg-white/95 px-5 py-6 backdrop-blur-sm transition md:static ${
            isNavOpen() ? "translate-x-0" : "-translate-x-full"
          } ${
            isSidebarCollapsed() ? "md:hidden" : "md:flex md:translate-x-0"
          } dark:border-neutral-800 dark:bg-neutral-950/90`}
          aria-label="Sidebar"
        >
          <div class="mb-8 flex items-center justify-between gap-2">
            <A
              href="/"
              class="text-lg font-semibold uppercase tracking-[0.22em] text-neutral-900 dark:text-neutral-100"
              onClick={() => {
                setIsNavOpen(false);
                setIsSidebarCollapsed(false);
              }}
            >
              Legion
            </A>
            <button
              type="button"
              class="rounded-lg border border-neutral-200 p-2 text-neutral-700 shadow-sm transition hover:bg-neutral-100 md:hidden dark:border-neutral-700 dark:text-neutral-300 dark:hover:bg-neutral-900"
              onClick={() => setIsNavOpen(false)}
              aria-label="Close navigation"
            >
              ✕
            </button>
            <button
              type="button"
              class="hidden rounded-lg border border-neutral-200 p-2 text-neutral-700 shadow-sm transition hover:bg-neutral-100 md:inline-flex dark:border-neutral-700 dark:text-neutral-300 dark:hover:bg-neutral-900"
              onClick={() => {
                setIsSidebarCollapsed(true);
                setIsNavOpen(false);
              }}
              aria-label="Collapse sidebar"
            >
              ❯
            </button>
          </div>
          <nav class="flex flex-col gap-1.5">
            <For each={navItems}>
              {(item) => (
                <A
                  href={item.path}
                  class={navButton(isActivePath(item.path))}
                  aria-current={isActivePath(item.path) ? "page" : "false"}
                >
                  <span>{item.label}</span>
                </A>
              )}
            </For>
          </nav>
          <div class="mt-auto rounded-lg border border-neutral-200/80 bg-neutral-50/80 p-3 text-xs text-neutral-500 dark:border-neutral-800 dark:bg-neutral-900/60 dark:text-neutral-400">
            <p class="uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-500">
              Manager
            </p>
            <p class="break-words font-mono text-neutral-700 dark:text-neutral-300">
              {managerEndpoint}
            </p>
          </div>
        </aside>

        <Show when={isNavOpen()}>
          <button
            type="button"
            class="fixed inset-0 z-30 bg-black/30 backdrop-blur-sm md:hidden"
            onClick={() => setIsNavOpen(false)}
            aria-label="Close navigation overlay"
          />
        </Show>

        <div class="flex flex-1 flex-col">
          <header class="sticky top-0 z-20 flex flex-wrap items-center justify-between gap-3 border-b border-neutral-200/80 bg-neutral-100/95 px-4 pb-4 pt-5 backdrop-blur md:px-8 dark:border-neutral-800 dark:bg-neutral-950/85">
            <div class="flex items-center gap-3">
              <button
                type="button"
                class={`flex h-10 w-10 items-center justify-center rounded-lg border border-neutral-300 bg-white text-lg text-neutral-700 shadow-sm transition hover:bg-neutral-50 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-200 ${
                  isSidebarCollapsed() ? "md:flex" : "md:hidden"
                }`}
                onClick={() => {
                  setIsNavOpen(true);
                  setIsSidebarCollapsed(false);
                }}
                aria-label="Open navigation"
              >
                ☰
              </button>
              <div>
                <p class="text-xs uppercase tracking-[0.16em] text-neutral-500 dark:text-neutral-400">
                  {activeNav().title}
                </p>
                <h1 class="text-xl font-semibold text-neutral-900 dark:text-neutral-50 md:text-2xl">
                  {activeNav().copy}
                </h1>
              </div>
            </div>
            <div class="flex flex-wrap items-center gap-3">
              <button
                type="button"
                class="flex h-10 w-10 items-center justify-center rounded-lg border border-neutral-300 bg-white text-lg font-medium text-neutral-800 shadow-sm transition hover:bg-neutral-50 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
                onClick={toggleTheme}
                aria-label={
                  theme() === "dark"
                    ? "Switch to light mode"
                    : "Switch to dark mode"
                }
              >
                <span
                  aria-hidden="true"
                  class="inline-block text-lg leading-none"
                >
                  {theme() === "dark" ? "☾" : "☀"}
                </span>
              </button>
            </div>
          </header>

          <main class="flex-1 px-4 py-6 md:px-8 md:py-8"><div class="mx-auto w-full max-w-7xl">{props.children ?? null}</div></main>
        </div>
      </div>
    </div>
  );
};

type PollingControllerProps = {
  refreshLedger: () => Promise<void>;
  refreshNodes: () => Promise<void>;
  refreshJobs: () => Promise<void>;
};

const PollingController = (props: PollingControllerProps) => {
  const location = useLocation();

  createEffect(() => {
    const current = location.pathname.replace(/\/+$/, "") || "/";
    let refetch: (() => void) | null = null;

    if (current === "/") {
      refetch = () => void props.refreshLedger();
    } else if (current.startsWith("/nodes")) {
      refetch = () => {
        void props.refreshNodes();
        void props.refreshLedger();
      };
    } else if (current.startsWith("/jobs")) {
      refetch = () => void props.refreshJobs();
    }

    if (!refetch) return;

    const interval = window.setInterval(refetch, POLL_INTERVAL_MS);
    onCleanup(() => window.clearInterval(interval));
  });

  return null;
};

const AppRoutes = () => {
  const fetchLedger = async (): Promise<readonly LedgerNode[]> => {
    const response = await managerClient.getLedger({});
    return response.nodes;
  };

  const fetchNodes = async (): Promise<readonly NodeMsg[]> => {
    const response = await managerClient.getNodes({});
    return normalizeNodes(response.nodes);
  };

  const fetchJobs = async (): Promise<readonly Job[]> => {
    const response = await managerClient.listJobs({});
    return response.jobs;
  };

  const [ledger, { mutate: setLedger }] = createResource(fetchLedger);
  const [nodes, { mutate: setNodes }] = createResource(fetchNodes);
  const [jobs, { mutate: setJobs }] = createResource(fetchJobs);

  const refreshLedger = async () => {
    try {
      const next = await fetchLedger();
      setLedger((prev) => (isEqualData(prev, next) ? prev : next));
    } catch (error) {
      console.error("Failed to refresh ledger", error);
    }
  };

  const refreshNodes = async () => {
    try {
      const next = await fetchNodes();
      setNodes((prev) => (areNodesEqual(prev, next) ? prev : next));
    } catch (error) {
      console.error("Failed to refresh nodes", error);
    }
  };

  const refreshJobs = async () => {
    try {
      const next = await fetchJobs();
      setJobs((prev) => (isEqualData(prev, next) ? prev : next));
    } catch (error) {
      console.error("Failed to refresh jobs", error);
    }
  };

  return (
    <Route
      path="/"
      component={(props) => (
        <Layout {...props}>
          <PollingController
            refreshLedger={refreshLedger}
            refreshNodes={refreshNodes}
            refreshJobs={refreshJobs}
          />
          {props.children}
        </Layout>
      )}
    >
      <Route path="/" component={() => <Homepage ledger={ledger} />} />
      <Route path="/nodes" component={() => <Nodes nodes={nodes} ledger={ledger} />} />
      <Route path="/jobs" component={() => <Jobs jobs={jobs} />} />
      <Route path="/kv" component={KeyValue} />
      <Route path="/manager" component={Manager} />
      <Route path="/testing" component={TestingUtils} />
      <Route path="*" component={() => <Homepage ledger={ledger} />} />
    </Route>
  );
};

function App() {
  return (
    <Router>
      <AppRoutes />
    </Router>
  );
}

export default App;

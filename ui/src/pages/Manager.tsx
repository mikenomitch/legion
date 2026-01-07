import { For, Show, createResource } from "solid-js";
import { managerClient } from "../grpcClient";
import type { ManagerNodeConfig } from "../gen/legion_pb";

export default function Manager() {
  const [configs] = createResource(
    async (): Promise<readonly ManagerNodeConfig[]> => {
      const response = await managerClient.getManagerConfigs({});
      return response.configs;
    },
  );

  return (
    <div class="rounded border border-neutral-200 bg-white/90 p-6 text-neutral-700 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:text-neutral-300 dark:shadow-[4px_4px_0_#0b0d12]">
      <div class="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <p class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
            Manager nodes
          </p>
          <h2 class="text-xl font-semibold text-neutral-900 dark:text-neutral-100">
            Configuration snapshots
          </h2>
        </div>
      </div>

      <Show
        when={!configs.loading}
        fallback={<p class="text-neutral-500">Loading manager configs…</p>}
      >
        <Show
          when={!configs.error}
          fallback={<p class="text-red-500">Failed to load manager configs.</p>}
        >
          {configs() && configs()!.length > 0 ? (
            <div class="grid gap-4 md:grid-cols-2">
              <For each={configs()!}>
                {(config) => (
                  <article class="rounded border border-neutral-200 bg-neutral-50 p-4 shadow-[4px_4px_0_#d4d4d8] transition dark:border-neutral-800 dark:bg-neutral-950/60 dark:shadow-[4px_4px_0_#0b0d12]">
                    <div class="mb-3 flex items-start justify-between gap-3">
                      <div>
                        <p class="text-[11px] uppercase tracking-[0.14em] text-neutral-500 dark:text-neutral-400">
                          Node
                        </p>
                        <p class="text-lg font-semibold text-neutral-900 dark:text-neutral-100">
                          {config.nodeId}
                        </p>
                        <p class="text-xs text-neutral-500 dark:text-neutral-400">
                          Updated {config.updatedAt || "—"}
                        </p>
                      </div>
                      <span class="rounded border border-neutral-200 px-2 py-1 text-[11px] uppercase tracking-wide text-neutral-700 dark:border-neutral-700 dark:text-neutral-200">
                        Config
                      </span>
                    </div>
                    <pre class="max-h-64 overflow-auto rounded bg-white p-3 text-xs leading-relaxed text-neutral-800 shadow-inner dark:bg-neutral-900 dark:text-neutral-100">
                      {formatConfig(config.configJson)}
                    </pre>
                  </article>
                )}
              </For>
            </div>
          ) : (
            <p class="text-neutral-500">No manager configs reported yet.</p>
          )}
        </Show>
      </Show>
    </div>
  );
}

function formatConfig(configJson: string): string {
  try {
    return JSON.stringify(JSON.parse(configJson), null, 2);
  } catch (error) {
    console.error("Failed to parse manager config JSON", error);
    return configJson;
  }
}

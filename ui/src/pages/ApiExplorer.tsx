import { For, Show, createEffect, createMemo, createSignal, onCleanup, onMount } from "solid-js";
import JSONEditor from "jsoneditor";
import "jsoneditor/dist/jsoneditor.css";
import { ScalarType, type MessageType } from "@bufbuild/protobuf";
import { Manager } from "../gen/legion_connect";
import { managerClient } from "../grpcClient";

type ManagerMethod = {
  key: string;
  name: string;
  request: MessageType;
  response: MessageType;
};

type TypeField = {
  path: string;
  type: string;
  rule: string;
};

const scalarLabels: Record<number, string> = {
  [ScalarType.DOUBLE]: "double",
  [ScalarType.FLOAT]: "float",
  [ScalarType.INT64]: "int64",
  [ScalarType.UINT64]: "uint64",
  [ScalarType.INT32]: "int32",
  [ScalarType.FIXED64]: "fixed64",
  [ScalarType.FIXED32]: "fixed32",
  [ScalarType.BOOL]: "bool",
  [ScalarType.STRING]: "string",
  [ScalarType.BYTES]: "bytes",
  [ScalarType.UINT32]: "uint32",
  [ScalarType.SFIXED32]: "sfixed32",
  [ScalarType.SFIXED64]: "sfixed64",
  [ScalarType.SINT32]: "sint32",
  [ScalarType.SINT64]: "sint64",
};

const managerMethods: readonly ManagerMethod[] = Object.entries(Manager.methods)
  .map(([key, method]) => ({
    key,
    name: method.name,
    request: method.I,
    response: method.O,
  }))
  .sort((a, b) => a.name.localeCompare(b.name));

const scalarPlaceholder = (scalar: ScalarType) => {
  if (scalar === ScalarType.BOOL) return false;
  if (scalar === ScalarType.STRING || scalar === ScalarType.BYTES) return "";
  return 0;
};

const makeTemplate = (type: MessageType, depth = 0): Record<string, unknown> => {
  if (depth > 4) return {};

  const output: Record<string, unknown> = {};
  for (const field of type.fields.list()) {
    if (field.kind === "scalar") {
      output[field.localName] = field.repeated
        ? [scalarPlaceholder(field.T)]
        : scalarPlaceholder(field.T);
      continue;
    }

    if (field.kind === "enum") {
      output[field.localName] = field.repeated ? [0] : 0;
      continue;
    }

    if (field.kind === "map") {
      output[field.localName] = {};
      continue;
    }

    output[field.localName] = field.repeated
      ? [makeTemplate(field.T, depth + 1)]
      : makeTemplate(field.T, depth + 1);
  }

  return output;
};

const toTypeFields = (
  type: MessageType,
  pathPrefix = "",
  depth = 0,
): TypeField[] => {
  if (depth > 5) return [];

  const rows: TypeField[] = [];
  for (const field of type.fields.list()) {
    const path = pathPrefix ? `${pathPrefix}.${field.localName}` : field.localName;

    if (field.kind === "scalar") {
      rows.push({
        path,
        type: scalarLabels[field.T] ?? "scalar",
        rule: field.repeated ? "repeated" : field.opt ? "optional" : "required",
      });
      continue;
    }

    if (field.kind === "enum") {
      rows.push({
        path,
        type: `enum ${field.T.typeName}`,
        rule: field.repeated ? "repeated" : field.opt ? "optional" : "required",
      });
      continue;
    }

    if (field.kind === "map") {
      const valueType =
        field.V.kind === "scalar"
          ? scalarLabels[field.V.T] ?? "scalar"
          : field.V.kind === "enum"
            ? `enum ${field.V.T.typeName}`
            : `message ${field.V.T.typeName}`;
      rows.push({
        path,
        type: `map<string, ${valueType}>`,
        rule: "map",
      });
      if (field.V.kind === "message") {
        rows.push(...toTypeFields(field.V.T, path, depth + 1));
      }
      continue;
    }

    rows.push({
      path,
      type: `message ${field.T.typeName}`,
      rule: field.repeated ? "repeated" : field.opt ? "optional" : "required",
    });
    rows.push(...toTypeFields(field.T, path, depth + 1));
  }

  return rows;
};

export default function ApiExplorer() {
  const [selectedMethodKey, setSelectedMethodKey] = createSignal(managerMethods[0]?.key ?? "");
  const [responseBody, setResponseBody] = createSignal("");
  const [errorMessage, setErrorMessage] = createSignal<string | null>(null);
  const [isCalling, setIsCalling] = createSignal(false);
  const [requestBody, setRequestBody] = createSignal("{}");
  let editorHost: HTMLDivElement | undefined;
  let editor: JSONEditor | undefined;

  const selectedMethod = createMemo(
    () => managerMethods.find((method) => method.key === selectedMethodKey()) ?? managerMethods[0],
  );

  const requestFields = createMemo(() => toTypeFields(selectedMethod().request));
  const responseFields = createMemo(() => toTypeFields(selectedMethod().response));

  const updateEditorBody = (nextValue: string) => {
    setRequestBody(nextValue);
    if (!editor) return;
    try {
      editor.updateText(nextValue);
    } catch (error) {
      console.error("Failed to update request editor", error);
    }
  };

  onMount(() => {
    if (!editorHost) return;
    editor = new JSONEditor(editorHost, {
      mode: "code",
      mainMenuBar: false,
      statusBar: false,
      onChangeText: (text: string) => setRequestBody(text),
    });
    editor.setText(requestBody());
  });

  onCleanup(() => {
    editor?.destroy();
    editor = undefined;
  });

  createEffect(() => {
    const template = makeTemplate(selectedMethod().request);
    updateEditorBody(JSON.stringify(template, null, 2));
    setErrorMessage(null);
    setResponseBody("");
  });

  const invokeMethod = async () => {
    setIsCalling(true);
    setErrorMessage(null);
    setResponseBody("");

    try {
      const parsedRequest = JSON.parse(requestBody());
      const methodName = selectedMethod().key;
      const caller = (managerClient as Record<string, (request: unknown) => Promise<unknown>>)[methodName];

      if (!caller) {
        throw new Error(`No client method found for ${methodName}`);
      }

      const response = await caller(parsedRequest);
      if (response && typeof response === "object" && "toJson" in response) {
        const message = response as { toJson: () => unknown };
        setResponseBody(JSON.stringify(message.toJson(), null, 2));
      } else {
        setResponseBody(JSON.stringify(response, null, 2));
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Request failed";
      setErrorMessage(message);
    } finally {
      setIsCalling(false);
    }
  };

  return (
    <div class="space-y-6">
      <section class="rounded border border-neutral-200 bg-white/90 p-6 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]">
        <p class="text-xs uppercase tracking-[0.14em] text-neutral-500 dark:text-neutral-400">API explorer</p>
        <h2 class="mt-2 text-xl font-semibold text-neutral-900 dark:text-neutral-100">
          Interactive gRPC manager API
        </h2>
        <p class="mt-2 max-w-3xl text-sm text-neutral-600 dark:text-neutral-300">
          Built with ConnectRPC + Buf generated descriptors and JSONEditor so every Manager gRPC method is
          discoverable, executable, and paired with request/response type metadata.
        </p>
      </section>

      <section class="grid gap-6 lg:grid-cols-[360px_1fr]">
        <article class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]">
          <div>
            <label class="text-xs font-semibold uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400" for="grpc-method-select">
              Method
            </label>
            <select
              id="grpc-method-select"
              class="mt-2 w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-800 dark:border-neutral-700 dark:bg-neutral-950 dark:text-neutral-200"
              value={selectedMethodKey()}
              onInput={(event) => setSelectedMethodKey(event.currentTarget.value)}
            >
              <For each={managerMethods}>
                {(method) => (
                  <option value={method.key}>
                    {method.name}
                  </option>
                )}
              </For>
            </select>
          </div>

          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
              Request type
            </p>
            <p class="mt-1 break-all font-mono text-sm text-neutral-700 dark:text-neutral-200">
              {selectedMethod().request.typeName}
            </p>
          </div>

          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
              Response type
            </p>
            <p class="mt-1 break-all font-mono text-sm text-neutral-700 dark:text-neutral-200">
              {selectedMethod().response.typeName}
            </p>
          </div>

          <button
            type="button"
            class="w-full rounded border border-neutral-300 bg-neutral-900 px-3 py-2 text-sm font-semibold text-white shadow-[3px_3px_0_#d4d4d8] transition hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-60 dark:border-neutral-700 dark:bg-neutral-100 dark:text-neutral-900 dark:shadow-[3px_3px_0_#0b0d12]"
            onClick={() => void invokeMethod()}
            disabled={isCalling()}
          >
            {isCalling() ? "Calling…" : `Call ${selectedMethod().name}`}
          </button>

          <Show when={errorMessage()}>
            <p class="rounded border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-700 dark:border-red-500/70 dark:bg-red-900/30 dark:text-red-200">
              {errorMessage()}
            </p>
          </Show>
        </article>

        <article class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]">
          <div>
            <h3 class="text-sm font-semibold uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-300">
              Request payload
            </h3>
            <p class="mt-1 text-xs text-neutral-500 dark:text-neutral-400">
              JSONEditor provides inline syntax support for faster request editing.
            </p>
          </div>
          <div ref={(element) => (editorHost = element)} class="h-[320px] overflow-hidden rounded border border-neutral-200 dark:border-neutral-700" />

          <div>
            <h3 class="text-sm font-semibold uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-300">
              Response
            </h3>
            <pre class="mt-2 max-h-72 overflow-auto rounded border border-neutral-200 bg-neutral-50 p-3 text-xs leading-relaxed text-neutral-800 dark:border-neutral-700 dark:bg-neutral-950 dark:text-neutral-100">
              {responseBody() || "Run a call to see the response body."}
            </pre>
          </div>
        </article>
      </section>

      <section class="grid gap-6 lg:grid-cols-2">
        <TypeTable title="Request fields" fields={requestFields()} />
        <TypeTable title="Response fields" fields={responseFields()} />
      </section>
    </div>
  );
}

function TypeTable(props: { title: string; fields: readonly TypeField[] }) {
  return (
    <article class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]">
      <h3 class="text-sm font-semibold uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-300">
        {props.title}
      </h3>
      <div class="mt-3 max-h-[400px] overflow-auto">
        <table class="w-full table-auto border-collapse text-left text-xs">
          <thead>
            <tr class="border-b border-neutral-200 dark:border-neutral-700">
              <th class="px-2 py-2 font-semibold text-neutral-600 dark:text-neutral-300">Path</th>
              <th class="px-2 py-2 font-semibold text-neutral-600 dark:text-neutral-300">Type</th>
              <th class="px-2 py-2 font-semibold text-neutral-600 dark:text-neutral-300">Rule</th>
            </tr>
          </thead>
          <tbody>
            <Show when={props.fields.length > 0} fallback={<tr><td class="px-2 py-3 text-neutral-500" colspan={3}>No fields.</td></tr>}>
              <For each={props.fields}>
                {(field) => (
                  <tr class="border-b border-neutral-100 align-top dark:border-neutral-800">
                    <td class="px-2 py-2 font-mono text-[11px] text-neutral-700 dark:text-neutral-200">{field.path}</td>
                    <td class="px-2 py-2 font-mono text-[11px] text-neutral-700 dark:text-neutral-200">{field.type}</td>
                    <td class="px-2 py-2 text-[11px] text-neutral-500 dark:text-neutral-400">{field.rule}</td>
                  </tr>
                )}
              </For>
            </Show>
          </tbody>
        </table>
      </div>
    </article>
  );
}

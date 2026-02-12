import { For, Show, createMemo, createSignal } from "solid-js";
import HoverInfo from "../components/HoverInfo";
import { managerClient } from "../grpcClient";
import { Manager } from "../gen/legion_connect";
import {
  getFieldProtoComment,
  getMessageProtoComment,
  getMethodProtoComment,
} from "../utils/protoDocs";

type ManagerMethodName = keyof typeof Manager.methods;
type ManagerMethodDescriptor = (typeof Manager.methods)[ManagerMethodName];

type ProtoFieldInfo = {
  no: number;
  name: string;
  kind: "scalar" | "enum" | "message" | "map";
  repeated?: boolean;
  opt?: boolean;
  T?: unknown;
  K?: unknown;
  V?: { kind: "scalar" | "enum" | "message"; T?: unknown };
};

type ProtoMessageCtor = {
  typeName: string;
  fields: {
    list: () => readonly ProtoFieldInfo[];
  };
  fromJson: (json: unknown) => unknown;
};

const methodEntries = Object.entries(Manager.methods) as [
  ManagerMethodName,
  ManagerMethodDescriptor,
][];

const scalarTypeMap: Record<number, string> = {
  1: "double",
  2: "float",
  3: "int64",
  4: "uint64",
  5: "int32",
  6: "fixed64",
  7: "fixed32",
  8: "bool",
  9: "string",
  10: "bytes",
  11: "uint32",
  12: "sfixed32",
  13: "sfixed64",
  14: "sint32",
  15: "sint64",
};

const isProtoMessageCtor = (value: unknown): value is ProtoMessageCtor => {
  if (!value || typeof value !== "object") return false;
  const maybe = value as { fields?: { list?: unknown }; fromJson?: unknown };
  return (
    typeof maybe.fromJson === "function" &&
    typeof maybe.fields?.list === "function"
  );
};

const describeFieldType = (field: ProtoFieldInfo): string => {
  const optionalPrefix = field.opt ? "optional " : "";
  const repeatedPrefix = field.repeated ? "repeated " : "";

  if (field.kind === "scalar") {
    const scalarType = typeof field.T === "number" ? scalarTypeMap[field.T] : "scalar";
    return `${optionalPrefix}${repeatedPrefix}${scalarType}`;
  }

  if (field.kind === "enum") {
    const enumType =
      typeof field.T === "object" && field.T && "typeName" in field.T
        ? String((field.T as { typeName?: string }).typeName)
        : "enum";
    return `${optionalPrefix}${repeatedPrefix}${enumType}`;
  }

  if (field.kind === "message") {
    const messageType =
      typeof field.T === "object" && field.T && "typeName" in field.T
        ? String((field.T as { typeName?: string }).typeName)
        : "message";
    return `${optionalPrefix}${repeatedPrefix}${messageType}`;
  }

  if (field.kind === "map") {
    const keyType =
      typeof field.K === "number" ? scalarTypeMap[field.K] ?? "scalar" : "scalar";
    const valueDescriptor = field.V;
    if (!valueDescriptor) return `${optionalPrefix}map<${keyType}, unknown>`;
    if (valueDescriptor.kind === "scalar") {
      const valueType =
        typeof valueDescriptor.T === "number"
          ? scalarTypeMap[valueDescriptor.T] ?? "scalar"
          : "scalar";
      return `${optionalPrefix}map<${keyType}, ${valueType}>`;
    }
    if (valueDescriptor.kind === "enum" || valueDescriptor.kind === "message") {
      const valueType =
        typeof valueDescriptor.T === "object" && valueDescriptor.T && "typeName" in valueDescriptor.T
          ? String((valueDescriptor.T as { typeName?: string }).typeName)
          : valueDescriptor.kind;
      return `${optionalPrefix}map<${keyType}, ${valueType}>`;
    }
  }

  return `${optionalPrefix}${repeatedPrefix}${field.kind}`;
};

const buildJsonTemplate = (ctor: ProtoMessageCtor, depth = 0): Record<string, unknown> => {
  if (depth > 2) return {};

  const template: Record<string, unknown> = {};
  for (const field of ctor.fields.list()) {
    const key = toCamelCase(field.name);
    if (field.repeated) {
      template[key] = [];
      continue;
    }
    if (field.kind === "scalar") {
      template[key] = defaultScalarValue(field.T);
      continue;
    }
    if (field.kind === "enum") {
      template[key] = "";
      continue;
    }
    if (field.kind === "map") {
      template[key] = {};
      continue;
    }
    if (field.kind === "message") {
      template[key] = isProtoMessageCtor(field.T)
        ? buildJsonTemplate(field.T, depth + 1)
        : {};
    }
  }

  return template;
};

const defaultScalarValue = (scalar: unknown): unknown => {
  if (typeof scalar !== "number") return "";
  if (scalar === 8) return false;
  if (scalar === 10) return "";
  if ([1, 2, 3, 4, 5, 6, 7, 11, 12, 13, 14, 15].includes(scalar)) return 0;
  return "";
};

const toCamelCase = (value: string) => value.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());

export default function ApiExplorer() {
  const [selectedMethodName, setSelectedMethodName] =
    createSignal<ManagerMethodName>(methodEntries[0][0]);
  const [requestJson, setRequestJson] = createSignal("{}");
  const [responseJson, setResponseJson] = createSignal("{}");
  const [requestError, setRequestError] = createSignal<string | null>(null);
  const [callError, setCallError] = createSignal<string | null>(null);
  const [isCalling, setIsCalling] = createSignal(false);
  const [methodSearch, setMethodSearch] = createSignal("");

  const filteredMethods = createMemo(() => {
    const query = methodSearch().trim().toLowerCase();
    if (!query) return methodEntries;
    return methodEntries.filter(([name, method]) => {
      const methodName = method.name.toLowerCase();
      return methodName.includes(query) || name.toLowerCase().includes(query);
    });
  });

  const selectedMethod = createMemo(
    () => Manager.methods[selectedMethodName()] as ManagerMethodDescriptor,
  );
  const selectedMethodComment = createMemo(() =>
    getMethodProtoComment(selectedMethod().name),
  );

  const requestType = createMemo(() => selectedMethod().I as unknown as ProtoMessageCtor);
  const responseType = createMemo(() => selectedMethod().O as unknown as ProtoMessageCtor);

  const resetRequestTemplate = () => {
    const template = buildJsonTemplate(requestType());
    setRequestJson(JSON.stringify(template, null, 2));
    setRequestError(null);
    setCallError(null);
  };

  resetRequestTemplate();

  const runCall = async () => {
    setRequestError(null);
    setCallError(null);

    let requestMessage: unknown;
    try {
      const parsed = JSON.parse(requestJson());
      requestMessage = requestType().fromJson(parsed);
    } catch (error) {
      setRequestError(error instanceof Error ? error.message : "Invalid JSON payload");
      return;
    }

    setIsCalling(true);
    try {
      const methodName = selectedMethodName();
      const client = managerClient as unknown as Record<
        ManagerMethodName,
        (request: unknown) => Promise<unknown>
      >;
      const response = await client[methodName](requestMessage);
      const responseAsJson =
        typeof response === "object" && response && "toJson" in response
          ? (response as { toJson: () => unknown }).toJson()
          : response;
      setResponseJson(JSON.stringify(responseAsJson, null, 2));
    } catch (error) {
      setCallError(error instanceof Error ? error.message : "RPC failed");
    } finally {
      setIsCalling(false);
    }
  };

  return (
    <section class="space-y-4">
      <div class="grid gap-4 lg:grid-cols-[20rem,1fr]">
        <aside class="max-h-[70vh] overflow-auto rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]">
          <p class="mb-3 text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
            Methods
          </p>
          <input
            type="search"
            placeholder="Search RPC methods"
            class="mb-3 w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-900 placeholder:text-neutral-500 dark:border-neutral-700 dark:bg-neutral-950 dark:text-neutral-100 dark:placeholder:text-neutral-400"
            value={methodSearch()}
            onInput={(event) => setMethodSearch(event.currentTarget.value)}
          />
          <Show
            when={filteredMethods().length > 0}
            fallback={
              <p class="text-sm text-neutral-500 dark:text-neutral-400">
                No methods match your search.
              </p>
            }
          >
          <div class="space-y-2">
            <For each={filteredMethods()}>
              {([name, method]) => (
                <button
                  type="button"
                  class={`w-full rounded border px-3 py-2 text-left text-sm transition ${
                    selectedMethodName() === name
                      ? "border-neutral-900 bg-neutral-900 text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900"
                      : "border-neutral-300 text-neutral-700 hover:border-neutral-500 dark:border-neutral-700 dark:text-neutral-300"
                  }`}
                  onClick={() => {
                    setSelectedMethodName(name);
                    resetRequestTemplate();
                  }}
                >
                  <p class="font-semibold">{method.name}</p>
                  <p class="mt-1 text-xs opacity-80">Unary</p>
                </button>
              )}
            </For>
          </div>
          </Show>
        </aside>

        <div class="space-y-4">
          <div class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]">
            <p class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
              Type information
            </p>
            <h3 class="text-lg font-semibold text-neutral-900 dark:text-neutral-100">
              {selectedMethod().name}
            </h3>
            <Show when={selectedMethodComment()}>
              <p class="mt-1 whitespace-pre-wrap text-sm text-neutral-600 dark:text-neutral-300">
                {selectedMethodComment()}
              </p>
            </Show>
            <div class="mt-4 grid gap-4 md:grid-cols-2">
              <TypeTable
                title={`Request: ${requestType().typeName}`}
                typeName={requestType().typeName}
                fields={requestType().fields.list()}
              />
              <TypeTable
                title={`Response: ${responseType().typeName}`}
                typeName={responseType().typeName}
                fields={responseType().fields.list()}
              />
            </div>
          </div>

          <div class="grid gap-4 md:grid-cols-2">
            <div class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]">
              <div class="mb-2 flex items-center justify-between">
                <p class="text-sm font-semibold text-neutral-900 dark:text-neutral-100">
                  Request JSON
                </p>
                <button
                  type="button"
                  class="rounded border border-neutral-300 px-2 py-1 text-xs text-neutral-700 hover:border-neutral-500 dark:border-neutral-700 dark:text-neutral-200"
                  onClick={resetRequestTemplate}
                >
                  Reset template
                </button>
              </div>
              <textarea
                class="h-80 w-full rounded border border-neutral-300 bg-white p-3 font-mono text-xs text-neutral-900 dark:border-neutral-700 dark:bg-neutral-950 dark:text-neutral-100"
                value={requestJson()}
                onInput={(event) => setRequestJson(event.currentTarget.value)}
              />
              <Show when={requestError()}>
                <p class="mt-2 text-xs text-red-500">{requestError()}</p>
              </Show>
              <Show when={callError()}>
                <p class="mt-2 text-xs text-red-500">RPC error: {callError()}</p>
              </Show>
              <div class="mt-3 flex justify-end">
                <button
                  type="button"
                  class="rounded border border-neutral-900 bg-neutral-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50 dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900"
                  onClick={() => void runCall()}
                  disabled={isCalling()}
                >
                  {isCalling() ? "Calling…" : `Invoke ${selectedMethod().name}`}
                </button>
              </div>
            </div>

            <div class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]">
              <p class="mb-2 text-sm font-semibold text-neutral-900 dark:text-neutral-100">
                Response JSON
              </p>
              <pre class="h-80 overflow-auto rounded border border-neutral-300 bg-white p-3 font-mono text-xs text-neutral-900 dark:border-neutral-700 dark:bg-neutral-950 dark:text-neutral-100">
                {responseJson()}
              </pre>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

type TypeTableProps = {
  title: string;
  typeName: string;
  fields: readonly ProtoFieldInfo[];
};

function TypeTable(props: TypeTableProps) {
  const messageComment = createMemo(() => getMessageProtoComment(props.typeName));

  return (
    <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
      <HoverInfo content={messageComment()}>
        <p
          class={`text-sm font-semibold text-neutral-900 dark:text-neutral-100 ${
            messageComment()
              ? "cursor-help decoration-dotted underline-offset-4 hover:underline"
              : ""
          }`}
        >
          {props.title}
        </p>
      </HoverInfo>
      <Show
        when={props.fields.length > 0}
        fallback={<p class="mt-2 text-xs text-neutral-500">No fields.</p>}
      >
        <div class="mt-3 overflow-visible">
          <table class="min-w-full text-left text-xs">
            <thead>
              <tr class="text-neutral-500 dark:text-neutral-400">
                <th class="pb-2 pr-4">Field</th>
                <th class="pb-2 pr-4">#</th>
                <th class="pb-2">Type</th>
              </tr>
            </thead>
            <tbody>
              <For each={props.fields}>
                {(field) => {
                  const fieldComment = getFieldProtoComment(props.typeName, field.no);
                  return (
                    <tr class="border-t border-neutral-200 align-top dark:border-neutral-800">
                      <td class="py-2 pr-4 font-mono text-neutral-900 dark:text-neutral-100">
                        <HoverInfo content={fieldComment}>
                          <span
                            class={
                              fieldComment
                                ? "cursor-help decoration-dotted underline-offset-4 hover:underline"
                                : ""
                            }
                          >
                            {toCamelCase(field.name)}
                          </span>
                        </HoverInfo>
                      </td>
                      <td class="py-2 pr-4 text-neutral-600 dark:text-neutral-300">{field.no}</td>
                      <td class="py-2 text-neutral-700 dark:text-neutral-200">{describeFieldType(field)}</td>
                    </tr>
                  );
                }}
              </For>
            </tbody>
          </table>
        </div>
      </Show>
    </div>
  );
}

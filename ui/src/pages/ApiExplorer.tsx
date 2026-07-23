import { For, Show, createMemo, createSignal } from "solid-js";
import { MethodKind } from "@bufbuild/protobuf";
import HoverInfo from "../components/HoverInfo";
import { Badge, Button, Icon, Note, SectionCard, cx, inputClass, textareaClass } from "../components/ui";
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

// Cap on messages collected when invoking a server-streaming method so the
// explorer renders a snapshot instead of tailing forever.
const STREAM_PREVIEW_LIMIT = 20;
const STREAM_PREVIEW_TIMEOUT_MS = 5000;

const methodKindLabel = (kind: MethodKind): string =>
  kind === MethodKind.ServerStreaming ? "Server streaming" : "Unary";

const toPlainJson = (message: unknown): unknown =>
  typeof message === "object" && message && "toJson" in message
    ? (message as { toJson: () => unknown }).toJson()
    : message;

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
      if (selectedMethod().kind === MethodKind.ServerStreaming) {
        const client = managerClient as unknown as Record<
          ManagerMethodName,
          (request: unknown, options?: { signal?: AbortSignal }) => AsyncIterable<unknown>
        >;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), STREAM_PREVIEW_TIMEOUT_MS);
        const collected: unknown[] = [];
        try {
          for await (const message of client[methodName](requestMessage, { signal: controller.signal })) {
            collected.push(toPlainJson(message));
            if (collected.length >= STREAM_PREVIEW_LIMIT) break;
          }
        } catch (error) {
          // An abort just ends the preview window; surface anything else.
          if (!controller.signal.aborted) throw error;
        } finally {
          clearTimeout(timeout);
          controller.abort();
        }
        setResponseJson(JSON.stringify(collected, null, 2));
      } else {
        const client = managerClient as unknown as Record<
          ManagerMethodName,
          (request: unknown) => Promise<unknown>
        >;
        const response = await client[methodName](requestMessage);
        setResponseJson(JSON.stringify(toPlainJson(response), null, 2));
      }
    } catch (error) {
      setCallError(error instanceof Error ? error.message : "RPC failed");
    } finally {
      setIsCalling(false);
    }
  };

  return (
    <div class="grid items-start gap-4 lg:grid-cols-[19rem,1fr]">
      <SectionCard title="Methods" flush class="lg:sticky lg:top-16">
        <div class="border-b border-line p-2.5">
          <div class="relative">
            <Icon name="search" size={14} class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-faint" />
            <input
              type="search"
              placeholder="Search RPC methods…"
              class={cx(inputClass, "pl-8")}
              value={methodSearch()}
              onInput={(event) => setMethodSearch(event.currentTarget.value)}
            />
          </div>
        </div>
        <Show
          when={filteredMethods().length > 0}
          fallback={<p class="p-4 text-sm text-muted">No methods match your search.</p>}
        >
          <div class="max-h-[65vh] space-y-0.5 overflow-auto p-1.5">
            <For each={filteredMethods()}>
              {([name, method]) => (
                <button
                  type="button"
                  class={cx(
                    "flex w-full items-center justify-between gap-2 rounded-md px-2.5 py-1.5 text-left text-sm transition-colors",
                    selectedMethodName() === name
                      ? "bg-brass/10 font-medium text-ink"
                      : "text-muted hover:bg-surface2 hover:text-ink",
                  )}
                  onClick={() => {
                    setSelectedMethodName(name);
                    resetRequestTemplate();
                  }}
                >
                  <span class="truncate font-mono text-xs">{method.name}</span>
                  <Show when={method.kind === MethodKind.ServerStreaming}>
                    <Badge tone="info">stream</Badge>
                  </Show>
                </button>
              )}
            </For>
          </div>
        </Show>
      </SectionCard>

      <div class="min-w-0 space-y-4">
        <SectionCard
          title={<span class="font-mono normal-case tracking-normal">{selectedMethod().name}</span>}
          description={methodKindLabel(selectedMethod().kind)}
        >
          <Show when={selectedMethodComment()}>
            <p class="mb-4 whitespace-pre-wrap text-sm text-muted">{selectedMethodComment()}</p>
          </Show>
          <div class="grid gap-4 md:grid-cols-2">
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
        </SectionCard>

        <div class="grid gap-4 xl:grid-cols-2">
          <SectionCard
            title="Request JSON"
            actions={<Button size="xs" icon="refresh" onClick={resetRequestTemplate}>Reset template</Button>}
          >
            <textarea
              class={cx(textareaClass, "h-80")}
              value={requestJson()}
              onInput={(event) => setRequestJson(event.currentTarget.value)}
              spellcheck={false}
            />
            <Show when={requestError()}>
              <Note tone="danger" class="mt-2 text-xs">{requestError()}</Note>
            </Show>
            <Show when={callError()}>
              <Note tone="danger" class="mt-2 text-xs">RPC error: {callError()}</Note>
            </Show>
            <div class="mt-3 flex justify-end">
              <Button variant="primary" size="md" icon="zap" onClick={() => void runCall()} disabled={isCalling()}>
                {isCalling() ? "Calling…" : `Invoke ${selectedMethod().name}`}
              </Button>
            </div>
          </SectionCard>

          <SectionCard title="Response JSON">
            <pre class="h-80 overflow-auto rounded-md border border-line bg-surface2/70 p-3 font-mono text-xs leading-relaxed text-ink">
              {responseJson()}
            </pre>
          </SectionCard>
        </div>
      </div>
    </div>
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
    <div class="min-w-0 rounded-md border border-line bg-surface2/40 p-3">
      <HoverInfo content={messageComment()}>
        <p
          class={`font-mono text-xs font-semibold text-ink ${
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
        fallback={<p class="mt-2 text-xs text-faint">No fields.</p>}
      >
        <div class="mt-3 overflow-visible">
          <table class="min-w-full text-left text-xs">
            <thead>
              <tr class="text-2xs uppercase tracking-wider text-faint">
                <th class="pb-2 pr-4 font-semibold">Field</th>
                <th class="pb-2 pr-4 font-semibold">#</th>
                <th class="pb-2 font-semibold">Type</th>
              </tr>
            </thead>
            <tbody>
              <For each={props.fields}>
                {(field) => {
                  const fieldComment = getFieldProtoComment(props.typeName, field.no);
                  return (
                    <tr class="border-t border-line align-top">
                      <td class="py-1.5 pr-4 font-mono text-ink">
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
                      <td class="py-1.5 pr-4 tabular-nums text-faint">{field.no}</td>
                      <td class="py-1.5 font-mono text-muted">{describeFieldType(field)}</td>
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

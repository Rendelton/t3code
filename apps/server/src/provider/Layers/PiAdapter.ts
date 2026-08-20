/**
 * PiAdapter — provider adapter for the pi coding agent.
 *
 * One `pi --mode rpc` subprocess per thread session, spoken to over stdio
 * (see {@link ../piRuntime}). Translates pi's RPC events into canonical
 * `ProviderRuntimeEvent`s:
 *
 *   - T3 turn  = accepted `prompt` → `agent_settled` (retries, compaction and
 *     queued steers all land inside one T3 turn)
 *   - sendTurn while a turn is active = steer (the active turn id is reused)
 *   - `tool_execution_*` → command/file-change/… item lifecycle
 *   - `message_update` deltas → `content.delta` (assistant/reasoning text)
 *   - `extension_ui_request` dialogs → `request.opened` (confirm) or
 *     `user-input.requested` (select)
 *
 * @module provider/Layers/PiAdapter
 */
import {
  EventId,
  type ModelSelection,
  type PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ThreadTokenUsageSnapshot,
  ThreadId,
  TurnId,
  type CanonicalItemType,
  RuntimeItemId,
  RuntimeRequestId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import {
  makePiRpcConnection,
  type PiRpcCommand,
  type PiRpcConnection,
  type PiRpcEvent,
} from "../piRuntime.ts";
import { type PiAdapterShape } from "../Services/PiAdapter.ts";

const PROVIDER = ProviderDriverKind.make("pi");

/**
 * Version tag stamped into the pi resume cursor. Bump if the cursor shape
 * changes so stale-shaped cursors written by older builds are ignored rather
 * than misread (mirrors OPENCODE_RESUME_VERSION).
 */
const PI_RESUME_VERSION = 1 as const;

interface PiResumeCursor {
  readonly schemaVersion: typeof PI_RESUME_VERSION;
  readonly sessionFile: string;
}

function parsePiResumeCursor(raw: unknown): PiResumeCursor | undefined {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return undefined;
  }
  const record = raw as Record<string, unknown>;
  if (record.schemaVersion !== PI_RESUME_VERSION) {
    return undefined;
  }
  if (typeof record.sessionFile !== "string" || record.sessionFile.trim().length === 0) {
    return undefined;
  }
  return { schemaVersion: PI_RESUME_VERSION, sessionFile: record.sessionFile.trim() };
}

/** Split a T3 model slug (`provider/id`) into pi's `set_model` pair. */
export function parsePiModelSlug(
  slug: string | undefined,
): { readonly provider: string; readonly id: string } | undefined {
  if (!slug) return undefined;
  const separatorIndex = slug.indexOf("/");
  if (separatorIndex <= 0 || separatorIndex >= slug.length - 1) {
    return undefined;
  }
  return {
    provider: slug.slice(0, separatorIndex),
    id: slug.slice(separatorIndex + 1),
  };
}

function classifyPiToolItemType(toolName: string): CanonicalItemType {
  const normalized = toolName.toLowerCase();
  if (
    normalized === "bash" ||
    normalized.includes("command") ||
    normalized.includes("shell") ||
    normalized.includes("terminal")
  ) {
    return "command_execution";
  }
  if (
    normalized === "edit" ||
    normalized === "write" ||
    normalized.includes("patch") ||
    normalized.includes("file")
  ) {
    return "file_change";
  }
  if (normalized.startsWith("mcp") || normalized.includes("__")) {
    return "mcp_tool_call";
  }
  if (normalized.includes("task") || normalized.includes("agent")) {
    return "collab_agent_tool_call";
  }
  if (normalized.includes("image")) {
    return "image_view";
  }
  return "dynamic_tool_call";
}

interface PiDialogRequest {
  /** The `extension_ui_request` id to respond to. */
  readonly dialogId: string;
  readonly kind: "confirm" | "userInput";
}

interface PiSessionContext {
  session: ProviderSession;
  readonly sessionScope: Scope.Scope;
  readonly connection: PiRpcConnection;
  readonly cwd: string;
  activeTurnId: TurnId | undefined;
  /** Set when the active turn's completion event was already emitted. */
  turnEnded: boolean;
  abortRequested: boolean;
  lastRunError: string | undefined;
  currentModelSlug: string | undefined;
  thinkingLevel: string | undefined;
  pendingDialogs: Map<string, PiDialogRequest>;
  /** Counter for synthetic assistant-message item ids (pi messages are unnamed). */
  nextMessageItem: number;
  cumulativeTokens: {
    total: number;
    input: number;
    output: number;
    cacheRead: number;
    cacheWrite: number;
  };
  cumulativeCostUsd: number;
  readonly stopped: Ref.Ref<boolean>;
}

export interface PiAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId | undefined;
}

const nowIso: Effect.Effect<string> = Effect.map(DateTime.now, DateTime.formatIso);

function recordAsString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

/** Extract a printable summary from a pi tool result content array. */
function piToolResultText(result: unknown): string | undefined {
  const record = asRecord(result);
  const content = record?.content;
  if (!Array.isArray(content)) {
    return recordAsString(record ?? {}, "text");
  }
  const parts: Array<string> = [];
  for (const block of content) {
    const blockRecord = asRecord(block);
    const text = blockRecord ? recordAsString(blockRecord, "text") : undefined;
    if (text) parts.push(text);
  }
  const joined = parts.join("\n").trim();
  return joined.length > 0 ? joined.slice(0, 2_000) : undefined;
}

function piUsageNumbers(usage: unknown): {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  total: number;
  costUsd: number;
} {
  const record = asRecord(usage);
  const nonNegative = (value: unknown): number =>
    typeof value === "number" && Number.isFinite(value) && value > 0 ? value : 0;
  const cost = asRecord(record?.cost);
  return {
    input: nonNegative(record?.input),
    output: nonNegative(record?.output),
    cacheRead: nonNegative(record?.cacheRead),
    cacheWrite: nonNegative(record?.cacheWrite),
    total: nonNegative(record?.totalTokens) ||
      nonNegative(record?.input) + nonNegative(record?.output),
    costUsd: nonNegative(cost?.total),
  };
}

function updateProviderSession(
  context: PiSessionContext,
  patch: Partial<ProviderSession>,
  options?: {
    readonly clearActiveTurnId?: boolean;
    readonly clearLastError?: boolean;
  },
): Effect.Effect<ProviderSession> {
  return Effect.gen(function* () {
    const updatedAt = yield* nowIso;
    const nextSession = {
      ...context.session,
      ...patch,
      updatedAt,
    } as ProviderSession & Record<string, unknown>;
    const mutableSession = nextSession as Record<string, unknown>;
    if (options?.clearActiveTurnId) {
      delete mutableSession.activeTurnId;
    }
    if (options?.clearLastError) {
      delete mutableSession.lastError;
    }
    context.session = nextSession;
    return nextSession;
  });
}

/** pi extension that gates mutating tools behind a confirm dialog. */
const T3_APPROVAL_GATE_EXTENSION_SOURCE = `/**
 * t3-approval-gate — pi extension installed by T3 Code.
 *
 * When the T3 thread runs in approval-required mode, every bash/edit/write
 * call is routed through ctx.ui.confirm(). In RPC mode that surfaces as an
 * \`extension_ui_request\` dialog which the T3 adapter translates into a
 * \`request.opened\` approval — so T3's approval UI mediates the call.
 */
export default function (pi) {
  if (process.env.T3_APPROVAL_MODE !== "approval-required") return;
  const GATED_TOOLS = new Set(["bash", "edit", "write"]);
  pi.on("tool_call", async (event, ctx) => {
    if (!GATED_TOOLS.has(event.toolName)) return;
    const input = event.input ?? {};
    const summary =
      typeof input.command === "string"
        ? input.command
        : typeof input.path === "string"
          ? \`\${input.path}\`
          : JSON.stringify(input).slice(0, 300);
    const allowed = await ctx.ui.confirm(
      \`Allow \${event.toolName}?\`,
      summary.slice(0, 2000),
      { timeout: 5 * 60 * 1000 },
    );
    if (!allowed) {
      return { block: true, reason: "Denied in T3 Code." };
    }
  });
}
`;

export function makePiAdapter(piSettings: PiSettings, options?: PiAdapterLiveOptions) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
    const serverConfig = yield* ServerConfig;
    const fileSystem = yield* FileSystem.FileSystem;
    const crypto = yield* Crypto.Crypto;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();

    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate pi runtime identifier.",
            cause,
          }),
      ),
    );

    const binaryPath = piSettings.binaryPath || "pi";

    // Materialize the bundled approval-gate extension once per adapter. The
    // file must exist on disk for pi's `-e <path>` loading; embedding the
    // source keeps it immune to bundler asset-path differences.
    const approvalGatePath = `${serverConfig.stateDir}/pi/t3-approval-gate.ts`;
    yield* fileSystem
      .makeDirectory(`${serverConfig.stateDir}/pi`, { recursive: true })
      .pipe(
        Effect.andThen(
          fileSystem.writeFile(
            approvalGatePath,
            new TextEncoder().encode(T3_APPROVAL_GATE_EXTENSION_SOURCE),
          ),
        ),
        Effect.catchCause((cause) =>
          Effect.logError("Failed to write the pi approval-gate extension.", {
            path: approvalGatePath,
            cause,
          }),
        ),
      );

    interface EventBaseInput {
      readonly threadId: ThreadId;
      readonly turnId?: TurnId | undefined;
      readonly itemId?: string | undefined;
      readonly requestId?: string | undefined;
      readonly raw?: unknown;
    }

    const buildEventBase = (input: EventBaseInput) =>
      Effect.all({
        eventId: randomUUIDv4.pipe(Effect.map(EventId.make)),
      }).pipe(
        Effect.map(({ eventId }) => ({
          eventId,
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: input.threadId,
          createdAt: undefined as string | undefined,
          ...(input.turnId ? { turnId: input.turnId } : {}),
          ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
          ...(input.requestId ? { requestId: RuntimeRequestId.make(input.requestId) } : {}),
          ...(input.raw !== undefined
            ? {
                raw: {
                  source: "pi.rpc.event" as const,
                  payload: input.raw,
                },
              }
            : {}),
        })),
      );

    // createdAt resolved lazily per event (the Effect.all above keeps the
    // random id; iso timestamp joins here to avoid an extra tick).
    const stampEvent = <T extends { readonly createdAt: string | undefined; threadId: ThreadId }>(
      base: T,
    ) => nowIso.pipe(Effect.map((createdAt) => ({ ...base, createdAt })));

    const emit = (event: ProviderRuntimeEvent) =>
      Queue.offer(runtimeEvents, event).pipe(Effect.asVoid);

    const sessions = new Map<ThreadId, PiSessionContext>();

    const ensureSessionContext = (threadId: ThreadId) =>
      Effect.suspend(() => {
        const context = sessions.get(threadId);
        return context
          ? Effect.succeed(context)
          : new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId });
      });

    const toRequestError =
      (operation: string) =>
      (cause: unknown): ProviderAdapterRequestError =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: operation,
          detail:
            cause instanceof Error
              ? cause.message
              : typeof cause === "object" && cause !== null && "detail" in cause
                ? String((cause as { readonly detail?: unknown }).detail)
                : `pi ${operation} failed.`,
          cause,
        });

    const stopPiContext = Effect.fn("stopPiContext")(function* (context: PiSessionContext) {
      if (yield* Ref.getAndSet(context.stopped, true)) {
        return false;
      }
      yield* context.connection.close.pipe(Effect.ignore);
      yield* Scope.close(context.sessionScope, Exit.void).pipe(Effect.ignoreCause);
      return true;
    });

    /** Emit runtime.error + session.exited and drop the session (unexpected death). */
    const emitUnexpectedExit = Effect.fn("emitUnexpectedExit")(function* (
      context: PiSessionContext,
      message: string,
    ) {
      if (yield* Ref.getAndSet(context.stopped, true)) {
        return;
      }
      const turnId = context.activeTurnId;
      sessions.delete(context.session.threadId);
      yield* emit({
        ...(yield* stampEvent(yield* buildEventBase({ threadId: context.session.threadId, turnId }))),
        type: "runtime.error",
        payload: {
          message,
          class: "transport_error",
        },
      }).pipe(Effect.ignore);
      yield* emit({
        ...(yield* stampEvent(yield* buildEventBase({ threadId: context.session.threadId, turnId }))),
        type: "session.exited",
        payload: {
          reason: message,
          recoverable: false,
          exitKind: "error",
        },
      }).pipe(Effect.ignore);
      yield* Scope.close(context.sessionScope, Exit.void).pipe(Effect.ignoreCause);
    });

    const emitTokenUsage = Effect.fn("emitTokenUsage")(function* (context: PiSessionContext) {
      const tokens = context.cumulativeTokens;
      const usage: ThreadTokenUsageSnapshot = {
        usedTokens: tokens.input + tokens.cacheRead + tokens.cacheWrite + tokens.output,
        totalProcessedTokens: tokens.total,
        inputTokens: tokens.input,
        cachedInputTokens: tokens.cacheRead,
        outputTokens: tokens.output,
        lastInputTokens: tokens.input,
        lastCachedInputTokens: tokens.cacheRead,
        lastOutputTokens: tokens.output,
        compactsAutomatically: true,
      };
      yield* emit({
        ...(yield* stampEvent(
          yield* buildEventBase({ threadId: context.session.threadId, turnId: context.activeTurnId }),
        )),
        type: "thread.token-usage.updated",
        payload: { usage },
      });
    });

    const finishTurn = Effect.fn("finishTurn")(function* (
      context: PiSessionContext,
      completion: { readonly kind: "settled" } | { readonly kind: "aborted"; readonly reason: string },
    ) {
      const turnId = context.activeTurnId;
      if (turnId !== undefined && !context.turnEnded) {
        context.turnEnded = true;
        if (completion.kind === "aborted") {
          yield* emit({
            ...(yield* stampEvent(yield* buildEventBase({ threadId: context.session.threadId, turnId }))),
            type: "turn.aborted",
            payload: { reason: completion.reason },
          });
        } else {
          const failed = context.lastRunError !== undefined;
          yield* emit({
            ...(yield* stampEvent(
              yield* buildEventBase({ threadId: context.session.threadId, turnId }),
            )),
            type: "turn.completed",
            payload: {
              state: failed ? "failed" : "completed",
              ...(context.lastRunError !== undefined ? { errorMessage: context.lastRunError } : {}),
              ...(context.currentModelSlug ? { usage: { model: context.currentModelSlug } } : {}),
              ...(context.cumulativeCostUsd > 0
                ? { totalCostUsd: context.cumulativeCostUsd }
                : {}),
            },
          });
        }
      }
      context.activeTurnId = undefined;
      context.abortRequested = false;
      context.lastRunError = undefined;
      yield* updateProviderSession(context, { status: "ready" }, { clearActiveTurnId: true });
    });

    // ── Event translation ──────────────────────────────────────────────

    const handleDialogRequest = Effect.fn("handleDialogRequest")(function* (
      context: PiSessionContext,
      event: PiRpcEvent,
    ) {
      const dialogId = recordAsString(event, "id");
      const method = recordAsString(event, "method");
      if (!dialogId || !method) return;
      const threadId = context.session.threadId;

      if (method === "confirm") {
        const title = recordAsString(event, "title") ?? "pi is asking";
        const message = recordAsString(event, "message");
        context.pendingDialogs.set(dialogId, { dialogId, kind: "confirm" });
        yield* emit({
          ...(yield* stampEvent(
            yield* buildEventBase({ threadId, requestId: dialogId, raw: event }),
          )),
          type: "request.opened",
          payload: {
            requestType: "unknown",
            detail: title,
            args: { message: message ?? title },
          },
        });
        return;
      }

      if (method === "select") {
        const title = recordAsString(event, "title") ?? "pi is asking";
        const rawOptions = Array.isArray(event.options) ? event.options : [];
        const options = rawOptions
          .map((option) => (typeof option === "string" ? option.trim() : ""))
          .filter((option) => option.length > 0);
        const question: UserInputQuestion = {
          id: "answer",
          header: title,
          question: title,
          options: options.map((label) => ({ label, description: label })),
          multiSelect: false,
        };
        context.pendingDialogs.set(dialogId, { dialogId, kind: "userInput" });
        yield* emit({
          ...(yield* stampEvent(
            yield* buildEventBase({ threadId, requestId: dialogId, raw: event }),
          )),
          type: "user-input.requested",
          payload: { questions: [question] },
        });
        return;
      }

      if (method === "input" || method === "editor") {
        // Free-text dialogs have no T3 surface yet; surface the request and
        // cancel it on any response so the extension isn't left hanging.
        const title = recordAsString(event, "title") ?? "pi is asking";
        context.pendingDialogs.set(dialogId, { dialogId, kind: "confirm" });
        yield* emit({
          ...(yield* stampEvent(
            yield* buildEventBase({ threadId, requestId: dialogId, raw: event }),
          )),
          type: "request.opened",
          payload: {
            requestType: "tool_user_input",
            detail: `${title} (free-text answers are not supported yet — responding cancels)`,
            args: { title },
          },
        });
        return;
      }

      // Fire-and-forget methods (notify/setStatus/setWidget/setTitle/set_editor_text):
      // surface warnings and errors in the thread, drop the rest.
      if (method === "notify") {
        const notifyType = recordAsString(event, "notifyType") ?? "info";
        const message = recordAsString(event, "message");
        if (message && (notifyType === "warning" || notifyType === "error")) {
          yield* emit({
            ...(yield* stampEvent(yield* buildEventBase({ threadId, raw: event }))),
            type: "runtime.warning",
            payload: { message },
          });
        }
      }
    });

    const handlePiEvent = Effect.fn("handlePiEvent")(function* (
      context: PiSessionContext,
      event: PiRpcEvent,
    ) {
      const threadId = context.session.threadId;
      const type = recordAsString(event, "type");
      if (!type) return;
      const turnId = context.activeTurnId;

      switch (type) {
        case "agent_start": {
          context.turnEnded = false;
          context.lastRunError = undefined;
          break;
        }

        case "message_update": {
          const delta = asRecord(event.assistantMessageEvent);
          const deltaType = recordAsString(delta ?? {}, "type");
          if (deltaType === "text_delta" && typeof delta?.delta === "string") {
            yield* emit({
              ...(yield* stampEvent(yield* buildEventBase({ threadId, turnId, raw: event }))),
              type: "content.delta",
              payload: {
                streamKind: "assistant_text",
                delta: delta.delta,
                ...(typeof delta.contentIndex === "number"
                  ? { contentIndex: delta.contentIndex }
                  : {}),
              },
            });
          } else if (deltaType === "thinking_delta" && typeof delta?.delta === "string") {
            yield* emit({
              ...(yield* stampEvent(yield* buildEventBase({ threadId, turnId, raw: event }))),
              type: "content.delta",
              payload: {
                streamKind: "reasoning_text",
                delta: delta.delta,
                ...(typeof delta.contentIndex === "number"
                  ? { contentIndex: delta.contentIndex }
                  : {}),
              },
            });
          }
          break;
        }

        case "message_end": {
          const message = asRecord(event.message);
          if (message?.role !== "assistant") break;
          const usage = piUsageNumbers(message.usage);
          if (usage.total > 0 || usage.costUsd > 0) {
            context.cumulativeTokens.total += usage.total;
            context.cumulativeTokens.input = usage.input;
            context.cumulativeTokens.output = usage.output;
            context.cumulativeTokens.cacheRead = usage.cacheRead;
            context.cumulativeTokens.cacheWrite = usage.cacheWrite;
            context.cumulativeCostUsd += usage.costUsd;
            yield* emitTokenUsage(context);
          }
          if (message.stopReason === "error") {
            const content = Array.isArray(message.content) ? message.content : [];
            const errorText = content
              .map((block) => recordAsString(asRecord(block) ?? {}, "text"))
              .filter((text): text is string => text !== undefined)
              .join("\n");
            context.lastRunError = errorText.length > 0 ? errorText : "The model reported an error.";
          }
          // Assistant message item: synthesized id since pi messages are unnamed.
          const messageItemId = `pi-msg-${(context.nextMessageItem += 1)}`;
          const content = Array.isArray(message.content) ? message.content : [];
          const fullText = content
            .map((block) => {
              const blockRecord = asRecord(block);
              return blockRecord?.type === "text"
                ? recordAsString(blockRecord, "text")
                : undefined;
            })
            .filter((text): text is string => text !== undefined)
            .join("\n");
          yield* emit({
            ...(yield* stampEvent(
              yield* buildEventBase({ threadId, turnId, itemId: messageItemId, raw: event }),
            )),
            type: "item.completed",
            payload: {
              itemType: "assistant_message",
              status: "completed",
              title: "Assistant message",
              ...(fullText.length > 0 ? { detail: fullText.slice(0, 2_000) } : {}),
            },
          });
          break;
        }

        case "tool_execution_start": {
          const toolCallId = recordAsString(event, "toolCallId");
          const toolName = recordAsString(event, "toolName") ?? "tool";
          if (!toolCallId) break;
          yield* emit({
            ...(yield* stampEvent(
              yield* buildEventBase({ threadId, turnId, itemId: toolCallId, raw: event }),
            )),
            type: "item.started",
            payload: {
              itemType: classifyPiToolItemType(toolName),
              status: "inProgress",
              title: toolName,
              data: { args: event.args ?? {} },
            },
          });
          break;
        }

        case "tool_execution_update": {
          const toolCallId = recordAsString(event, "toolCallId");
          const toolName = recordAsString(event, "toolName");
          const summary = piToolResultText(event.partialResult);
          if (!toolCallId || summary === undefined) break;
          yield* emit({
            ...(yield* stampEvent(
              yield* buildEventBase({ threadId, turnId, itemId: toolCallId, raw: event }),
            )),
            type: "tool.progress",
            payload: {
              toolUseId: toolCallId,
              ...(toolName ? { toolName } : {}),
              summary: summary.slice(0, 500),
            },
          });
          break;
        }

        case "tool_execution_end": {
          const toolCallId = recordAsString(event, "toolCallId");
          const toolName = recordAsString(event, "toolName") ?? "tool";
          if (!toolCallId) break;
          const isError = event.isError === true;
          const summary = piToolResultText(event.result);
          yield* emit({
            ...(yield* stampEvent(
              yield* buildEventBase({ threadId, turnId, itemId: toolCallId, raw: event }),
            )),
            type: "item.completed",
            payload: {
              itemType: classifyPiToolItemType(toolName),
              status: isError ? "failed" : "completed",
              title: toolName,
              ...(summary ? { detail: summary.slice(0, 2_000) } : {}),
              data: { result: event.result ?? null },
            },
          });
          // Surface edit-tool patches in the turn diff view.
          const resultRecord = asRecord(event.result);
          const details = asRecord(resultRecord?.details);
          const patch = typeof details?.patch === "string" ? details.patch : undefined;
          if (patch && patch.length > 0) {
            yield* emit({
              ...(yield* stampEvent(
                yield* buildEventBase({ threadId, turnId, itemId: toolCallId, raw: event }),
              )),
              type: "turn.diff.updated",
              payload: { unifiedDiff: patch },
            });
          }
          break;
        }

        case "compaction_start": {
          const itemId = `pi-compaction-${Date.now()}`;
          yield* emit({
            ...(yield* stampEvent(
              yield* buildEventBase({ threadId, turnId, itemId, raw: event }),
            )),
            type: "item.started",
            payload: {
              itemType: "context_compaction",
              status: "inProgress",
              title: "Compacting conversation",
            },
          });
          break;
        }

        case "compaction_end": {
          const result = asRecord(event.result);
          const usage = piUsageNumbers(result?.usage);
          if (usage.total > 0 || usage.costUsd > 0) {
            context.cumulativeTokens.total += usage.total;
            context.cumulativeCostUsd += usage.costUsd;
            yield* emitTokenUsage(context);
          }
          yield* emit({
            ...(yield* stampEvent(
              yield* buildEventBase({ threadId, turnId, raw: event }),
            )),
            type: "item.completed",
            payload: {
              itemType: "context_compaction",
              status: event.aborted === true ? "declined" : "completed",
              title: "Compacting conversation",
            },
          });
          break;
        }

        case "auto_retry_start": {
          const attempt = typeof event.attempt === "number" ? event.attempt : "?";
          const errorMessage = recordAsString(event, "errorMessage");
          yield* emit({
            ...(yield* stampEvent(yield* buildEventBase({ threadId, turnId, raw: event }))),
            type: "runtime.warning",
            payload: {
              message: `pi is retrying after a transient error (attempt ${attempt}).`,
              ...(errorMessage ? { detail: errorMessage } : {}),
            },
          });
          break;
        }

        case "auto_retry_end": {
          if (event.success !== true) {
            context.lastRunError =
              recordAsString(event, "finalError") ?? "The model kept failing after retries.";
          }
          break;
        }

        case "extension_ui_request": {
          yield* handleDialogRequest(context, event);
          break;
        }

        case "agent_settled": {
          if (context.abortRequested) {
            yield* finishTurn(context, { kind: "aborted", reason: "Interrupted by user." });
          } else {
            yield* finishTurn(context, { kind: "settled" });
          }
          break;
        }

        default:
          break;
      }
    });

    /** Drain the connection's event queue until the process exits. */
    const startEventPump = (context: PiSessionContext) =>
      Stream.fromQueue(context.connection.events).pipe(
        Stream.mapEffect((event: PiRpcEvent) =>
          handlePiEvent(context, event).pipe(
            Effect.catchCause((cause) =>
              Effect.logError("pi event handling failed.", {
                threadId: context.session.threadId,
                cause,
              }),
            ),
          ),
        ),
        Stream.runDrain,
        Effect.flatMap(() =>
          emitUnexpectedExit(
            context,
            "The pi process exited unexpectedly.",
          ),
        ),
        Effect.catchCause((cause) =>
          emitUnexpectedExit(
            context,
            `The pi event stream failed: ${causeToString(cause)}`,
          ),
        ),
        Effect.forkIn(context.sessionScope),
      );

    // ── Adapter methods ────────────────────────────────────────────────

    const applyModelSelection = Effect.fn("applyModelSelection")(function* (
      context: PiSessionContext,
      modelSelection: ModelSelection | undefined,
    ) {
      if (!modelSelection) return;
      const parsed = parsePiModelSlug(modelSelection.model);
      if (!parsed) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `pi model selection must use the 'provider/id' format (got '${modelSelection.model}').`,
        });
      }
      const slug = `${parsed.provider}/${parsed.id}`;
      if (slug !== context.currentModelSlug) {
        yield* context.connection
          .send({ type: "set_model", provider: parsed.provider, modelId: parsed.id })
          .pipe(Effect.mapError(toRequestError("set_model")));
        context.currentModelSlug = slug;
        context.session = { ...context.session, model: slug };
      }
      const thinkingLevel = getModelSelectionStringOptionValue(modelSelection, "thinkingLevel");
      if (thinkingLevel && thinkingLevel !== context.thinkingLevel) {
        yield* context.connection
          .send({ type: "set_thinking_level", level: thinkingLevel })
          .pipe(Effect.mapError(toRequestError("set_thinking_level")));
        context.thinkingLevel = thinkingLevel;
      }
    });

    const startSession: PiAdapterShape["startSession"] = Effect.fn("startSession")(
      function* (input) {
        const existing = sessions.get(input.threadId);
        if (existing) {
          return existing.session;
        }
        const cwd = input.cwd ?? serverConfig.cwd;
        const resume = parsePiResumeCursor(input.resumeCursor);
        const sessionFile =
          resume !== undefined &&
          (yield* fileSystem.exists(resume.sessionFile).pipe(Effect.orElseSucceed(() => false)))
            ? resume.sessionFile
            : undefined;
        if (resume !== undefined && sessionFile === undefined) {
          yield* Effect.logWarning(
            `pi session file '${resume.sessionFile}' no longer exists; starting a fresh session.`,
          );
        }

        const modelPattern = input.modelSelection
          ? piModelPattern(input.modelSelection)
          : undefined;

        const sessionScope = yield* Scope.make();
        const connection = yield* makePiRpcConnection({
          binaryPath,
          cwd,
          environment: process.env,
          ...(piSettings.agentDir ? { agentDir: piSettings.agentDir } : {}),
          ...(sessionFile ? { sessionFile } : {}),
          ...(input.title ? { sessionName: input.title } : {}),
          ...(modelPattern ? { modelPattern } : {}),
        }).pipe(
          Effect.provideService(Scope.Scope, sessionScope),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.mapError(
            (cause) =>
              new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: `Failed to start the pi RPC process for thread '${input.threadId}'.`,
                cause,
              }),
          ),
          Effect.tapError((error) =>
            Scope.close(sessionScope, Exit.void).pipe(
              Effect.ignoreCause,
              Effect.asVoid,
              Effect.as(error),
            ),
          ),
        );

        // Handshake: confirm the RPC process is responsive and capture its
        // session identity (file + id + model).
        const state = yield* connection
          .send({ type: "get_state" }, 20_000)
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: `pi RPC handshake failed for thread '${input.threadId}'.`,
                  cause,
                }),
            ),
            Effect.tapError(() => connection.close.pipe(Effect.ignore)),
          );
        const stateRecord = asRecord(state) ?? {};
        const sessionId =
          typeof stateRecord.sessionId === "string" && stateRecord.sessionId.length > 0
            ? stateRecord.sessionId
            : undefined;
        const stateModel = asRecord(stateRecord.model);
        const initialModelSlug =
          (stateModel ? `${recordAsString(stateModel, "provider")}/${recordAsString(stateModel, "id")}` : undefined) ??
          input.modelSelection?.model;

        const createdAt = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd,
          ...(initialModelSlug ? { model: initialModelSlug } : {}),
          threadId: input.threadId,
          resumeCursor:
            typeof stateRecord.sessionFile === "string" && stateRecord.sessionFile.length > 0
              ? {
                  schemaVersion: PI_RESUME_VERSION,
                  sessionFile: stateRecord.sessionFile,
                }
              : undefined,
          createdAt,
          updatedAt: createdAt,
        };

        const context: PiSessionContext = {
          session,
          sessionScope,
          connection,
          cwd,
          activeTurnId: undefined,
          turnEnded: false,
          abortRequested: false,
          lastRunError: undefined,
          currentModelSlug: initialModelSlug,
          thinkingLevel:
            typeof stateRecord.thinkingLevel === "string"
              ? stateRecord.thinkingLevel
              : undefined,
          pendingDialogs: new Map(),
          nextMessageItem: 0,
          cumulativeTokens: { total: 0, input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          cumulativeCostUsd: 0,
          stopped: yield* Ref.make(false),
        };
        sessions.set(input.threadId, context);
        yield* startEventPump(context);

        if (input.title) {
          yield* connection
            .send({ type: "set_session_name", name: input.title })
            .pipe(Effect.ignore);
        }

        yield* emit({
          ...(yield* stampEvent(yield* buildEventBase({ threadId: input.threadId }))),
          type: "session.started",
          payload: {
            message: "pi session started",
            ...(sessionFile ? { resume: sessionFile } : {}),
          },
        });
        yield* emit({
          ...(yield* stampEvent(yield* buildEventBase({ threadId: input.threadId }))),
          type: "thread.started",
          payload: sessionId ? { providerThreadId: sessionId } : {},
        });

        return session;
      },
    );

    const sendTurn: PiAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
      const context = yield* ensureSessionContext(input.threadId);
      const steeringTurnId = context.activeTurnId;
      const turnId = steeringTurnId ?? TurnId.make(`pi-turn-${yield* randomUUIDv4}`);
      const threadId = input.threadId;

      const text = input.input?.trim();
      const images = yield* readAttachments(fileSystem, serverConfig.attachmentsDir, input);
      if ((!text || text.length === 0) && images.length === 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "pi turns require text input or at least one attachment.",
        });
      }

      if (steeringTurnId === undefined) {
        yield* applyModelSelection(context, input.modelSelection ?? defaultModelSelection(context));
      }

      const state = yield* context.connection
        .send({ type: "get_state" })
        .pipe(Effect.mapError(toRequestError("sendTurn.get_state")));
      const isStreaming = asRecord(state)?.isStreaming === true;

      context.activeTurnId = turnId;
      context.turnEnded = false;
      context.abortRequested = false;
      yield* updateProviderSession(
        context,
        {
          status: "running",
          activeTurnId: turnId,
          ...(context.currentModelSlug ? { model: context.currentModelSlug } : {}),
        },
        { clearLastError: true },
      );

      if (steeringTurnId === undefined) {
        yield* emit({
          ...(yield* stampEvent(yield* buildEventBase({ threadId, turnId }))),
          type: "turn.started",
          payload: {
            ...(context.currentModelSlug ? { model: context.currentModelSlug } : {}),
            ...(context.thinkingLevel ? { effort: context.thinkingLevel } : {}),
          },
        });
      }

      yield* context.connection
        .send({
          type: "prompt",
          ...(text ? { message: text } : { message: "" }),
          ...(images.length > 0 ? { images } : {}),
          ...(isStreaming ? { streamingBehavior: "steer" as const } : {}),
        })
        .pipe(
          Effect.mapError(toRequestError("sendTurn.prompt")),
          Effect.tapError((requestError) =>
            steeringTurnId !== undefined
              ? Effect.void
              : Effect.gen(function* () {
                  context.activeTurnId = undefined;
                  yield* updateProviderSession(
                    context,
                    { status: "ready", lastError: requestError.detail },
                    { clearActiveTurnId: true },
                  );
                  yield* emit({
                    ...(yield* stampEvent(yield* buildEventBase({ threadId, turnId }))),
                    type: "turn.aborted",
                    payload: { reason: requestError.detail },
                  });
                }),
          ),
        );

      return {
        threadId,
        turnId,
        ...(context.session.resumeCursor !== undefined
          ? { resumeCursor: context.session.resumeCursor }
          : {}),
      };
    });

    const interruptTurn: PiAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
      function* (threadId, turnId) {
        const context = yield* ensureSessionContext(threadId);
        context.abortRequested = true;
        yield* context.connection
          .send({ type: "abort" })
          .pipe(Effect.mapError(toRequestError("interruptTurn")));
        const activeTurn = turnId ?? context.activeTurnId;
        if (activeTurn !== undefined && !context.turnEnded) {
          context.turnEnded = true;
          yield* emit({
            ...(yield* stampEvent(yield* buildEventBase({ threadId, turnId: activeTurn }))),
            type: "turn.aborted",
            payload: { reason: "Interrupted by user." },
          });
          context.activeTurnId = undefined;
          yield* updateProviderSession(context, { status: "ready" }, { clearActiveTurnId: true });
        }
      },
    );

    const respondToRequest: PiAdapterShape["respondToRequest"] = Effect.fn(
      "respondToRequest",
    )(function* (threadId, requestId, decision) {
      const context = yield* ensureSessionContext(threadId);
      const dialog = context.pendingDialogs.get(requestId);
      if (!dialog) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToRequest",
          detail: `Unknown pending pi dialog request: ${requestId}`,
        });
      }
      context.pendingDialogs.delete(requestId);
      const confirmed = decision === "accept" || decision === "acceptForSession";
      yield* context.connection.write({
        type: "extension_ui_response",
        id: dialog.dialogId,
        confirmed,
      }).pipe(Effect.mapError(toRequestError("respondToRequest.write")));
      yield* emit({
        ...(yield* stampEvent(
          yield* buildEventBase({ threadId, requestId, turnId: context.activeTurnId }),
        )),
        type: "request.resolved",
        payload: {
          requestType: "unknown",
          decision: confirmed ? "accept" : "decline",
        },
      });
    });

    const respondToUserInput: PiAdapterShape["respondToUserInput"] = Effect.fn(
      "respondToUserInput",
    )(function* (threadId, requestId, answers) {
      const context = yield* ensureSessionContext(threadId);
      const dialog = context.pendingDialogs.get(requestId);
      if (!dialog) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToUserInput",
          detail: `Unknown pending pi dialog request: ${requestId}`,
        });
      }
      context.pendingDialogs.delete(requestId);
      const answer =
        typeof answers.answer === "string"
          ? answers.answer
          : Object.values(answers).find((value) => typeof value === "string");
      const response: PiRpcCommand =
        typeof answer === "string" ? { value: answer } : { cancelled: true };
      yield* context.connection.write({
        type: "extension_ui_response",
        id: dialog.dialogId,
        ...response,
      }).pipe(Effect.mapError(toRequestError("respondToUserInput.write")));
      yield* emit({
        ...(yield* stampEvent(
          yield* buildEventBase({ threadId, requestId, turnId: context.activeTurnId }),
        )),
        type: "user-input.resolved",
        payload: { answers: answers as Record<string, unknown> },
      });
    });

    const stopSession: PiAdapterShape["stopSession"] = Effect.fn("stopSession")(
      function* (threadId) {
        const context = sessions.get(threadId);
        if (!context) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          });
        }
        const stopped = yield* stopPiContext(context);
        sessions.delete(threadId);
        if (!stopped) {
          return;
        }
        yield* emit({
          ...(yield* stampEvent(yield* buildEventBase({ threadId }))),
          type: "session.exited",
          payload: {
            reason: "Session stopped.",
            recoverable: false,
            exitKind: "graceful",
          },
        });
      },
    );

    const listSessions: PiAdapterShape["listSessions"] = () =>
      Effect.sync(() => [...sessions.values()].map((context) => context.session));

    const hasSession: PiAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => sessions.has(threadId));

    const readThread: PiAdapterShape["readThread"] = Effect.fn("readThread")(
      function* (threadId) {
        const context = yield* ensureSessionContext(threadId);
        const data = yield* context.connection
          .send({ type: "get_entries" })
          .pipe(Effect.mapError(toRequestError("readThread")));
        return {
          threadId,
          turns: piEntriesToTurns(asRecord(data)?.entries),
        };
      },
    );

    const rollbackThread: PiAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
      function* (threadId, numTurns) {
        const context = yield* ensureSessionContext(threadId);
        const data = yield* context.connection
          .send({ type: "get_entries" })
          .pipe(Effect.mapError(toRequestError("rollbackThread")));
        const userEntryIds = userMessageEntryIds(asRecord(data)?.entries);
        if (userEntryIds.length === 0) {
          return yield* readThread(threadId);
        }
        // Removing the last `numTurns` turns = branch at the user entry that
        // starts the first removed turn. Forking at that entry makes it the
        // new leaf, ready to be re-prompted.
        const targetIndex = Math.max(0, userEntryIds.length - numTurns);
        yield* context.connection
          .send({ type: "fork", entryId: userEntryIds[targetIndex] })
          .pipe(Effect.mapError(toRequestError("rollbackThread.fork")));
        // The fork replaced the session; adopt the new file as resume cursor.
        const state = yield* context.connection
          .send({ type: "get_state" })
          .pipe(Effect.mapError(toRequestError("rollbackThread.get_state")));
        const sessionFile = asRecord(state)?.sessionFile;
        if (typeof sessionFile === "string" && sessionFile.length > 0) {
          context.session = {
            ...context.session,
            resumeCursor: { schemaVersion: PI_RESUME_VERSION, sessionFile },
          };
        }
        return yield* readThread(threadId);
      },
    );

    const stopAll: PiAdapterShape["stopAll"] = () =>
      Effect.gen(function* () {
        const contexts = [...sessions.values()];
        sessions.clear();
        yield* Effect.forEach(contexts, (context) => stopPiContext(context).pipe(Effect.ignore), {
          concurrency: "unbounded",
          discard: true,
        });
      });

    // Layer-level finalizer: stop every session when the adapter's scope
    // closes so no pi subprocess outlives its instance.
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const contexts = [...sessions.values()];
        sessions.clear();
        yield* Effect.forEach(contexts, (context) => stopPiContext(context).pipe(Effect.ignore), {
          concurrency: "unbounded",
          discard: true,
        });
      }).pipe(Effect.ensuring(Queue.shutdown(runtimeEvents))),
    );

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "in-session",
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      get streamEvents() {
        return Stream.fromQueue(runtimeEvents);
      },
    } satisfies PiAdapterShape;
  });
}

// ── Helpers ───────────────────────────────────────────────────────────

function causeToString(cause: unknown): string {
  if (cause instanceof Error) return cause.message;
  return String(cause);
}

function defaultModelSelection(context: PiSessionContext): ModelSelection | undefined {
  return context.currentModelSlug
    ? {
        instanceId: context.session.providerInstanceId ?? ProviderInstanceId.make("pi"),
        model: context.currentModelSlug,
        options: [],
      }
    : undefined;
}

/** Build the `--model` pattern (`provider/id:thinking`) for session spawn.
 *
 * The thinking level is always explicit: inheriting pi's global
 * `defaultThinkingLevel` would make thread behavior depend on the user's
 * CLI settings, and some self-hosted endpoints return empty completions for
 * levels they don't actually support. T3's model picker exposes the levels
 * per model; `off` is the safe floor when nothing is selected. */
function piModelPattern(modelSelection: ModelSelection): string | undefined {
  const thinking = getModelSelectionStringOptionValue(modelSelection, "thinkingLevel") ?? "off";
  return `${modelSelection.model}:${thinking}`;
}

/** Convert pi session entries into turn snapshots (one turn per user message). */
function piEntriesToTurns(entries: unknown): ReadonlyArray<{ id: TurnId; items: Array<unknown> }> {
  if (!Array.isArray(entries)) return [];
  const turns: Array<{ id: TurnId; items: Array<unknown> }> = [];
  let current: { id: TurnId; items: Array<unknown> } | undefined;
  for (const entry of entries) {
    const record = asRecord(entry);
    const message = asRecord(record?.message);
    if (!record || !message) continue;
    if (message.role === "user") {
      current = { id: TurnId.make(`pi-entry-${String(record.id)}`), items: [record] };
      turns.push(current);
      continue;
    }
    if (current) {
      current.items.push(record);
    }
  }
  return turns;
}

function userMessageEntryIds(entries: unknown): ReadonlyArray<string> {
  if (!Array.isArray(entries)) return [];
  const ids: Array<string> = [];
  for (const entry of entries) {
    const record = asRecord(entry);
    const message = asRecord(record?.message);
    if (record && message?.role === "user" && typeof record.id === "string") {
      ids.push(record.id);
    }
  }
  return ids;
}

/** Read image attachments into pi's base64 image format. */
function readAttachments(
  fileSystem: FileSystem.FileSystem,
  attachmentsDir: string,
  input: ProviderSendTurnInput,
): Effect.Effect<
  ReadonlyArray<{ readonly type: "image"; readonly data: string; readonly mimeType: string }>,
  ProviderAdapterValidationError
> {
  const attachments = input.attachments ?? [];
  if (attachments.length === 0) {
    return Effect.succeed([]);
  }
  return Effect.forEach(attachments, (attachment) =>
    Effect.gen(function* () {
      if (attachment.type !== "image") {
        return undefined;
      }
      const path = resolveAttachmentPath({ attachmentsDir, attachment });
      if (!path) {
        return undefined;
      }
      const bytes = yield* fileSystem.readFile(path).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: `Failed to read attachment '${attachment.name}'.`,
              cause,
            }),
        ),
      );
      return {
        type: "image" as const,
        data: Buffer.from(bytes).toString("base64"),
        mimeType: attachment.mimeType,
      };
    }),
  ).pipe(
    Effect.map((images) =>
      images.filter((image): image is NonNullable<typeof image> => image !== undefined),
    ),
  );
}

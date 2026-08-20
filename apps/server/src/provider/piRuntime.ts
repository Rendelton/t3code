/**
 * piRuntime — subprocess plumbing for the pi coding agent RPC protocol.
 *
 * pi speaks a JSONL RPC protocol over stdio (`pi --mode rpc`): commands go
 * to stdin one per line, responses and agent events come back on stdout
 * (see pi's rpc.md). This module owns the wire details so the adapter and
 * provider layers stay protocol-free:
 *
 *   - strict LF framing — Node `readline` is NOT protocol-compliant because
 *     it also splits on U+2028/U+2029, which are valid inside JSON strings.
 *     Lines are split at the byte level (0x0A never appears inside a UTF-8
 *     multi-byte sequence), so chunk boundaries mid-character are safe.
 *   - request/response correlation by `id`
 *   - a decoded event stream for everything pi emits between responses
 *   - `PI_CODING_AGENT_DIR` wiring for the optional agentDir setting
 *
 * @module provider/piRuntime
 */
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

export class PiRuntimeError extends Data.TaggedError("PiRuntimeError")<{
  readonly operation: string;
  readonly detail: string;
  readonly cause?: unknown;
}> {}

/** A command object written to pi's stdin (JSON, one per line). */
export type PiRpcCommand = Record<string, unknown>;

/** A decoded non-response message from pi's stdout (agent events, extension UI requests, …). */
export type PiRpcEvent = Record<string, unknown>;

export interface PiRpcResponseMessage {
  readonly type: "response";
  readonly id?: string | undefined;
  readonly command: string;
  readonly success: boolean;
  readonly data?: unknown;
  readonly error?: string | undefined;
}

export const isPiRpcResponseMessage = (value: unknown): value is PiRpcResponseMessage =>
  typeof value === "object" &&
  value !== null &&
  (value as { readonly type?: unknown }).type === "response";

// ── JSONL framing ─────────────────────────────────────────────────────

const utf8LineDecoder = new TextDecoder();

/**
 * Split a stdout byte stream into pi RPC records. Splits on LF (0x0A) only,
 * stripping one trailing CR per record; multi-byte UTF-8 sequences never
 * contain 0x0A so byte-level splitting survives chunk boundaries that land
 * mid-character. A trailing record without LF (crashed writer mid-line) is
 * flushed on halt. Empty records are dropped.
 */
export const piRpcRecords = <A extends Uint8Array, E, R>(
  stdout: Stream.Stream<A, E, R>,
): Stream.Stream<string, E, R> =>
  stdout.pipe(
    Stream.mapAccum(
      () => new Uint8Array(0),
      (buffer: Uint8Array, chunk: A) => {
        const combined = new Uint8Array(buffer.byteLength + chunk.byteLength);
        combined.set(buffer);
        combined.set(chunk, buffer.byteLength);
        const records: Array<string> = [];
        let start = 0;
        for (let index = 0; index < combined.byteLength; index += 1) {
          if (combined[index] !== 0x0a) {
            continue;
          }
          let end = index;
          if (end > start && combined[end - 1] === 0x0d) {
            end -= 1;
          }
          if (end > start) {
            records.push(utf8LineDecoder.decode(combined.subarray(start, end)));
          }
          start = index + 1;
        }
        return [combined.subarray(start), records] as const;
      },
      {
        onHalt: (buffer: Uint8Array) =>
          buffer.byteLength > 0 ? [utf8LineDecoder.decode(buffer)] : [],
      },
    ),
    Stream.filter((record) => record.length > 0),
  );

/** Parse a record, returning `None` for malformed JSON (logged and dropped upstream). */
export const parsePiRpcRecord = (
  record: string,
): { readonly ok: true; readonly value: unknown } | { readonly ok: false } => {
  try {
    return { ok: true, value: JSON.parse(record) };
  } catch {
    return { ok: false };
  }
};

// ── Connection ────────────────────────────────────────────────────────

export interface PiRpcSpawnOptions {
  readonly binaryPath: string;
  readonly cwd: string;
  readonly environment: NodeJS.ProcessEnv;
  /** Override for pi's config directory (`~/.pi/agent` by default). Empty/undefined = default. */
  readonly agentDir?: string | undefined;
  /** Resume a specific pi session file. */
  readonly sessionFile?: string | undefined;
  /** Display name for the pi session (`--name`). */
  readonly sessionName?: string | undefined;
  /** Model pattern (`provider/id` or `provider/id:thinking`). */
  readonly modelPattern?: string | undefined;
  /** Probe mode: `--no-session`, no persistence. */
  readonly noSession?: boolean;
}

export function buildPiRpcArgs(options: PiRpcSpawnOptions): ReadonlyArray<string> {
  return [
    "--mode",
    "rpc",
    // Trust project-local extensions/skills/context files: T3 threads own the
    // cwd, so pi's interactive project-approval prompt has nowhere to go.
    "--approve",
    ...(options.noSession ? ["--no-session"] : []),
    ...(options.sessionFile ? ["--session", options.sessionFile] : []),
    ...(options.sessionName ? ["--name", options.sessionName] : []),
    ...(options.modelPattern ? ["--model", options.modelPattern] : []),
  ];
}

export function buildPiEnvironment(
  environment: NodeJS.ProcessEnv,
  agentDir: string | undefined,
): NodeJS.ProcessEnv {
  const trimmed = agentDir?.trim();
  return trimmed && trimmed.length > 0
    ? { ...environment, PI_CODING_AGENT_DIR: trimmed }
    : { ...environment };
}

export interface PiRpcConnection {
  /** Send a command and await its correlated response. Fails on process exit or timeout. */
  readonly send: (
    command: PiRpcCommand,
    timeoutMs?: number,
  ) => Effect.Effect<unknown, PiRuntimeError>;
  /** Write a payload to stdin without awaiting a response (e.g. `extension_ui_response`). */
  readonly write: (payload: PiRpcCommand) => Effect.Effect<void, PiRuntimeError>;
  /** Decoded non-response messages from stdout. Shut down when the process exits. */
  readonly events: Queue.Queue<PiRpcEvent>;
  /** Resolves with the exit code (null when killed by a signal) once the process exits. */
  readonly exited: Deferred.Deferred<number | null, PiRuntimeError>;
  /** Best-effort tail of the process's stderr, for diagnostics. */
  readonly recentStderr: Effect.Effect<string>;
  /** Terminate the process (whole process group) and release resources. Idempotent. */
  readonly close: Effect.Effect<void>;
}

const DEFAULT_SEND_TIMEOUT_MS = 30_000;
const STDERR_TAIL_BYTES = 8_192;

export const makePiRpcConnection = (
  options: PiRpcSpawnOptions,
): Effect.Effect<
  PiRpcConnection,
  PiRuntimeError,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> =>
  Effect.gen(function* () {
    const scope = yield* Scope.Scope;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const hostPlatform = yield* HostProcessPlatform;
    const spawnCommand = yield* resolveSpawnCommand(
      options.binaryPath,
      buildPiRpcArgs(options),
      { env: options.environment },
    ).pipe(
      Effect.mapError(
        (cause) =>
          new PiRuntimeError({
            operation: "spawn",
            detail: `Failed to resolve the pi command '${options.binaryPath}'.`,
            cause,
          }),
      ),
    );

    const child = yield* spawner
      .spawn(
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          cwd: options.cwd,
          env: buildPiEnvironment(options.environment, options.agentDir),
          shell: spawnCommand.shell,
          // A dedicated process group on POSIX lets close() take down pi's
          // own subprocesses (bash tool commands) along with pi itself.
          detached: hostPlatform !== "win32",
        }),
      )
      .pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.mapError(
          (cause) =>
            new PiRuntimeError({
              operation: "spawn",
              detail: `Failed to spawn the pi process '${options.binaryPath}'.`,
              cause,
            }),
        ),
      );

    const events = yield* Queue.unbounded<PiRpcEvent>();
    const exited = yield* Deferred.make<number | null, PiRuntimeError>();
    const stderrRef = yield* Ref.make("");
    const stdinLines = yield* Queue.unbounded<string>();
    const pending = new Map<string, Deferred.Deferred<unknown, PiRuntimeError>>();
    let nextRequestId = 0;

    const killProcessGroup = (signal: NodeJS.Signals): Effect.Effect<void> =>
      hostPlatform === "win32"
        ? child
            .kill({ killSignal: signal, forceKillAfter: "3 seconds" })
            .pipe(Effect.asVoid, Effect.ignore)
        : Effect.sync(() => {
            try {
              process.kill(-Number(child.pid), signal);
            } catch {
              // Already gone — nothing to signal.
            }
          });

    const onProcessExit = (code: number | null) =>
      Effect.gen(function* () {
        yield* Deferred.done(
          exited,
          Exit.succeed(code),
        ).pipe(Effect.ignore);
        const failure = new PiRuntimeError({
          operation: "stdin",
          detail: `The pi process exited (code ${code ?? "null"}).`,
        });
        for (const deferred of pending.values()) {
          yield* Deferred.fail(deferred, failure).pipe(Effect.ignore);
        }
        pending.clear();
        yield* Queue.shutdown(events).pipe(Effect.ignore);
        yield* Queue.shutdown(stdinLines).pipe(Effect.ignore);
      });

    // Route stdout records: responses go to their awaiting Deferred, everything
    // else is offered to the event queue for the session event pump.
    yield* Stream.runForEach(piRpcRecords(child.stdout), (record) =>
      Effect.gen(function* () {
        const parsed = parsePiRpcRecord(record);
        if (!parsed.ok) {
          yield* Effect.logWarning("pi RPC: dropping unparseable stdout record.", {
            recordPreview: record.slice(0, 200),
          });
          return;
        }
        if (isPiRpcResponseMessage(parsed.value) && typeof parsed.value.id === "string") {
          const deferred = pending.get(parsed.value.id);
          if (deferred !== undefined) {
            pending.delete(parsed.value.id);
            yield* Deferred.done(
              deferred,
              parsed.value.success
                ? Exit.succeed(parsed.value.data)
                : Exit.fail(
                    new PiRuntimeError({
                      operation: parsed.value.command,
                      detail: parsed.value.error ?? `pi command '${parsed.value.command}' failed.`,
                    }),
                  ),
            ).pipe(Effect.ignore);
            return;
          }
        }
        yield* Queue.offer(events, parsed.value as PiRpcEvent);
      }),
    ).pipe(Effect.ignore, Effect.forkIn(scope));

    yield* Stream.runForEach(child.stderr, (chunk) =>
      Ref.update(stderrRef, (current) => {
        const next = `${current}${utf8LineDecoder.decode(chunk)}`;
        return next.length > STDERR_TAIL_BYTES ? next.slice(next.length - STDERR_TAIL_BYTES) : next;
      }),
    ).pipe(Effect.ignore, Effect.forkIn(scope));

    yield* child.exitCode.pipe(
      Effect.flatMap((code) => onProcessExit(Number(code))),
      Effect.catchCause((cause) =>
        Deferred.fail(
          exited,
          new PiRuntimeError({
            operation: "exit",
            detail: "The pi process was interrupted.",
            cause,
          }),
        ).pipe(Effect.ignore),
      ),
      Effect.forkIn(scope),
    );

    // Persistent stdin writer: a single long-lived stream run so individual
    // writes never complete the sink (closing stdin would EOF the child).
    yield* Stream.fromQueue(stdinLines)
      .pipe(Stream.encodeText, Stream.run(child.stdin), Effect.ignore, Effect.forkIn(scope));

    const write: PiRpcConnection["write"] = (payload) =>
      Queue.offer(stdinLines, `${JSON.stringify(payload)}\n`).pipe(Effect.asVoid);

    const send: PiRpcConnection["send"] = (command, timeoutMs = DEFAULT_SEND_TIMEOUT_MS) =>
      Effect.gen(function* () {
        const id = `t3-${Date.now().toString(36)}-${(nextRequestId += 1)}`;
        const deferred = yield* Deferred.make<unknown, PiRuntimeError>();
        pending.set(id, deferred);
        const awaitResponse = Deferred.await(deferred).pipe(
          Effect.ensuring(Effect.sync(() => pending.delete(id))),
        );
        yield* write({ ...command, id });
        return yield* awaitResponse.pipe(
          Effect.timeoutOption(timeoutMs),
          Effect.flatMap((result) =>
            result._tag === "None"
              ? new PiRuntimeError({
                  operation: String(command.type ?? "command"),
                  detail: `pi command '${String(command.type)}' timed out after ${timeoutMs}ms.`,
                })
              : Effect.succeed(result.value),
          ),
        );
      });

    return {
      send,
      write,
      events,
      exited,
      recentStderr: Ref.get(stderrRef),
      close: Effect.gen(function* () {
        yield* killProcessGroup("SIGTERM");
        yield* Deferred.await(exited).pipe(
          Effect.timeoutOption("3 seconds"),
          Effect.flatMap((result) =>
            result._tag === "None" ? killProcessGroup("SIGKILL") : Effect.void,
          ),
          Effect.ignore,
        );
        yield* Scope.close(scope, Exit.void).pipe(Effect.ignoreCause);
      }),
    };
  });

// ── Probes ────────────────────────────────────────────────────────────

export const PI_VERSION_PROBE_TIMEOUT_MS = 4_000;
export const PI_INVENTORY_PROBE_TIMEOUT_MS = 20_000;

export interface PiModelInfo {
  readonly provider: string;
  readonly id: string;
  readonly name: string;
  readonly reasoning: boolean;
  /** Thinking levels the model supports, when known (from `thinkingLevelMap`). */
  readonly thinkingLevels: ReadonlyArray<string> | undefined;
  readonly imageInput: boolean;
}

export interface PiCommandInfo {
  readonly name: string;
  readonly description: string | undefined;
  /** `extension` | `prompt` | `skill` */
  readonly source: string;
  readonly path: string | undefined;
  readonly scope: string | undefined;
}

export interface PiInventory {
  readonly models: ReadonlyArray<PiModelInfo>;
  readonly commands: ReadonlyArray<PiCommandInfo>;
}

const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;

const asTrimmedString = (value: unknown): string | undefined => {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
};

const PI_THINKING_LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;

function thinkingLevelsFromModel(model: Record<string, unknown>): ReadonlyArray<string> | undefined {
  const map = asRecord(model.thinkingLevelMap);
  if (map === undefined) {
    return model.reasoning === true ? ["off", "minimal", "low", "medium", "high"] : undefined;
  }
  const supported = PI_THINKING_LEVELS.filter((level) => map[level] !== null && map[level] !== undefined);
  return supported.length > 1 ? supported : undefined;
}

export function piModelsFromResponse(data: unknown): ReadonlyArray<PiModelInfo> {
  const models = asRecord(data)?.models;
  if (!Array.isArray(models)) return [];
  const out: Array<PiModelInfo> = [];
  for (const entry of models) {
    const model = asRecord(entry);
    const provider = asTrimmedString(model?.provider);
    const id = asTrimmedString(model?.id);
    if (!model || !provider || !id) continue;
    const input = Array.isArray(model.input) ? model.input : [];
    out.push({
      provider,
      id,
      name: asTrimmedString(model.name) ?? id,
      reasoning: model.reasoning === true,
      thinkingLevels: thinkingLevelsFromModel(model),
      imageInput: input.includes("image"),
    });
  }
  return out;
}

export function piCommandsFromResponse(data: unknown): ReadonlyArray<PiCommandInfo> {
  const commands = asRecord(data)?.commands;
  if (!Array.isArray(commands)) return [];
  const out: Array<PiCommandInfo> = [];
  for (const entry of commands) {
    const command = asRecord(entry);
    const name = asTrimmedString(command?.name);
    if (!command || !name) continue;
    const sourceInfo = asRecord(command.sourceInfo);
    out.push({
      name,
      description: asTrimmedString(command.description),
      source: asTrimmedString(command.source) ?? "extension",
      path: asTrimmedString(sourceInfo?.path),
      scope: asTrimmedString(sourceInfo?.scope),
    });
  }
  return out;
}

/**
 * One-shot inventory probe: spawn a session-less pi RPC process, ask for
 * available models (only models with valid auth are listed) and commands,
 * then terminate it. Used by the provider snapshot refresh.
 */
export const probePiInventory = (
  options: PiRpcSpawnOptions,
): Effect.Effect<
  PiInventory,
  PiRuntimeError,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Effect.gen(function* () {
    const connection = yield* makePiRpcConnection({ ...options, noSession: true });
    const modelsData = yield* connection.send(
      { type: "get_available_models" },
      PI_INVENTORY_PROBE_TIMEOUT_MS,
    );
    const commandsData = yield* connection.send({ type: "get_commands" });
    yield* connection.close;
    return {
      models: piModelsFromResponse(modelsData),
      commands: piCommandsFromResponse(commandsData),
    };
  }).pipe(Effect.scoped);

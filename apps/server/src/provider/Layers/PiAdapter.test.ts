/**
 * PiAdapterLive tests against a scripted pi RPC peer (scripts/pi-mock-rpc.mjs).
 * Covers the canonical event mapping, steering, abort, dialogs, model
 * switching, rollback, and unexpected process exit.
 */
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  PiSettings,
  ProviderInstanceId,
  ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";


import { ServerConfig } from "../../config.ts";
import { makePiAdapter } from "./PiAdapter.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const __dirname = NodePath.dirname(import.meta.url.replace("file://", ""));
const mockRpcPath = NodePath.join(__dirname, "../../../scripts/pi-mock-rpc.mjs");

interface FakePiHarness {
  readonly binaryPath: string;
  readonly logPath: string;
  readonly readLoggedCommands: () => Promise<Array<Record<string, unknown>>>;
}

async function makeFakePi(scenario: string): Promise<FakePiHarness> {
  const dir = await NodeFS.promises.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-adapter-test-"));
  const logPath = NodePath.join(dir, "commands.log");
  const wrapperPath = NodePath.join(dir, "fake-pi.sh");
  const script = `#!/bin/sh
export FAKE_PI_SCENARIO=${JSON.stringify(scenario)}
export FAKE_PI_LOG=${JSON.stringify(logPath)}
exec ${JSON.stringify(process.execPath)} ${JSON.stringify(mockRpcPath)} "$@"
`;
  await NodeFS.promises.writeFile(wrapperPath, script, "utf8");
  await NodeFS.promises.chmod(wrapperPath, 0o755);
  return {
    binaryPath: wrapperPath,
    logPath,
    readLoggedCommands: async () => {
      const raw = await NodeFS.promises.readFile(logPath, "utf8").catch(() => "");
      return raw
        .split("\n")
        .map((line) => line.trim())
        .filter((line) => line.length > 0)
        .map((line) => JSON.parse(line) as Record<string, unknown>);
    },
  };
}

const piAdapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-pi-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeTestAdapter = (binaryPath: string) => makePiAdapter(decodePiSettings({ binaryPath })).pipe(Effect.orDie);

/**
 * Pump the adapter's runtime events into a queue and expose blocking
 * `waitFor` — deterministic, no polling: events are consumed in order.
 */
function makeEventTap(adapter: { readonly streamEvents: Stream.Stream<ProviderRuntimeEvent> }) {
  return Effect.gen(function* () {
    const queue = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const seen: ProviderRuntimeEvent[] = [];
    const fiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
      Effect.sync(() => {
        seen.push(event);
      }).pipe(Effect.andThen(Queue.offer(queue, event))),
    ).pipe(Effect.forkChild);
    const waitFor = (predicate: (event: ProviderRuntimeEvent) => boolean) =>
      Effect.gen(function* () {
        while (true) {
          const event = yield* Queue.take(queue);
          if (predicate(event)) {
            return event;
          }
        }
      });
    return { fiber, waitFor, seen };
  });
}

it.layer(piAdapterTestLayer)("PiAdapterLive", (it) => {
  it.effect("maps a prompt run to canonical runtime events", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => makeFakePi("basic"));
      const adapter = yield* makeTestAdapter(harness.binaryPath);
      const threadId = ThreadId.make("pi-mock-basic");
      const tap = yield* makeEventTap(adapter);

      const session = yield* adapter.startSession({
        threadId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      assert.equal(session.provider, "pi");
      assert.equal(session.model, "omlx/Qwen3.8-27B-4bit");
      const resume = session.resumeCursor as { sessionFile?: string } | undefined;
      assert.isDefined(resume?.sessionFile);

      const turn = yield* adapter.sendTurn({ threadId, input: "hello pi" });
      assert.isDefined(turn.turnId);

      const completed = yield* tap.waitFor((event) => event.type === "turn.completed");
      if (completed.type !== "turn.completed") {
        throw new Error("unreachable");
      }
      assert.equal(completed.payload.state, "completed");
      assert.equal(completed.turnId, turn.turnId);

      const types = tap.seen.map((event) => event.type);
      assert.includeMembers(types, [
        "session.started",
        "thread.started",
        "turn.started",
        "content.delta",
        "item.started",
        "item.completed",
        "thread.token-usage.updated",
        "turn.completed",
      ] as const);

      // Deltas concatenated form the assistant text.
      const deltaText = tap.seen
        .filter((event) => event.type === "content.delta")
        .map((event) => (event.type === "content.delta" ? event.payload.delta : ""))
        .join("");
      assert.equal(deltaText, "Hello world");

      // The bash tool call maps to a command execution item lifecycle.
      const itemStarted = tap.seen.find(
        (event) => event.type === "item.started" && event.itemId?.toString() === "call-1",
      );
      assert.isDefined(itemStarted);
      if (itemStarted?.type === "item.started") {
        assert.equal(itemStarted.payload.itemType, "command_execution");
      }

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(tap.fiber);
    }),
  );

  it.effect("reuses the active turn id when steering a running prompt", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => makeFakePi("steer"));
      const adapter = yield* makeTestAdapter(harness.binaryPath);
      const threadId = ThreadId.make("pi-mock-steer");
      const tap = yield* makeEventTap(adapter);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      const first = yield* adapter.sendTurn({ threadId, input: "start" });

      // Second sendTurn while the run is open: same T3 turn, steer command.
      const second = yield* adapter.sendTurn({ threadId, input: "redirect" });
      assert.equal(second.turnId, first.turnId);

      yield* tap.waitFor((event) => event.type === "turn.completed");
      const commands = yield* Effect.promise(harness.readLoggedCommands);
      const promptCommands = commands.filter((command) => command.type === "prompt");
      assert.equal(promptCommands.length, 2);
      assert.equal(promptCommands[1]?.streamingBehavior, "steer");
      // Only one turn.started for the whole steered run.
      assert.equal(
        tap.seen.filter((event) => event.type === "turn.started").length,
        1,
      );

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(tap.fiber);
    }),
  );

  it.effect("emits turn.aborted when interrupted and does not double-complete", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => makeFakePi("abort"));
      const adapter = yield* makeTestAdapter(harness.binaryPath);
      const threadId = ThreadId.make("pi-mock-abort");
      const tap = yield* makeEventTap(adapter);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      yield* adapter.sendTurn({ threadId, input: "long task" });
      yield* adapter.interruptTurn(threadId);

      yield* tap.waitFor((event) => event.type === "turn.aborted");
      // agent_settled arrives after abort; it must not complete the turn.
      // Advance the virtual clock past the stabilization window.
      yield* TestClock.adjust("200 millis");
      assert.equal(tap.seen.filter((event) => event.type === "turn.completed").length, 0);
      const session = (yield* adapter.listSessions()).find((s) => s.threadId === threadId);
      assert.isDefined(session);
      assert.isUndefined(session?.activeTurnId);

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(tap.fiber);
    }),
  );

  it.effect("surfaces confirm dialogs as approvals and forwards decisions", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => makeFakePi("confirm-dialog"));
      const adapter = yield* makeTestAdapter(harness.binaryPath);
      const threadId = ThreadId.make("pi-mock-confirm");
      const tap = yield* makeEventTap(adapter);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      yield* adapter.sendTurn({ threadId, input: "run it" });

      const opened = yield* tap.waitFor((event) => event.type === "request.opened");
      if (opened.type !== "request.opened") {
        throw new Error("unreachable");
      }
      assert.equal(opened.payload.detail, "Allow bash?");
      assert.isDefined(opened.requestId);

      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(String(opened.requestId)),
        "accept",
      );
      yield* tap.waitFor((event) => event.type === "turn.completed");

      const commands = yield* Effect.promise(harness.readLoggedCommands);
      const reply = commands.find((command) => command.type === "extension_ui_response");
      assert.deepEqual(
        { id: reply?.id, confirmed: reply?.confirmed },
        { id: "dialog-1", confirmed: true },
      );
      const resolved = tap.seen.find((event) => event.type === "request.resolved");
      assert.isDefined(resolved);

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(tap.fiber);
    }),
  );

  it.effect("surfaces select dialogs as user input and forwards the answer", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => makeFakePi("select-dialog"));
      const adapter = yield* makeTestAdapter(harness.binaryPath);
      const threadId = ThreadId.make("pi-mock-select");
      const tap = yield* makeEventTap(adapter);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      yield* adapter.sendTurn({ threadId, input: "pick" });

      const requested = yield* tap.waitFor((event) => event.type === "user-input.requested");
      if (requested.type !== "user-input.requested") {
        throw new Error("unreachable");
      }
      assert.equal(requested.payload.questions[0]?.options.length, 2);
      assert.isDefined(requested.requestId);

      yield* adapter.respondToUserInput(
        threadId,
        ApprovalRequestId.make(String(requested.requestId)),
        { answer: "blue" },
      );
      yield* tap.waitFor((event) => event.type === "turn.completed");

      const commands = yield* Effect.promise(harness.readLoggedCommands);
      const reply = commands.find((command) => command.type === "extension_ui_response");
      assert.deepEqual(
        { id: reply?.id, value: reply?.value },
        { id: "dialog-2", value: "blue" },
      );

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(tap.fiber);
    }),
  );

  it.effect("switches models in-session via set_model and set_thinking_level", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => makeFakePi("basic"));
      const adapter = yield* makeTestAdapter(harness.binaryPath);
      const threadId = ThreadId.make("pi-mock-model");
      const tap = yield* makeEventTap(adapter);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      yield* adapter.sendTurn({
        threadId,
        input: "hello",
        modelSelection: {
          instanceId: ProviderInstanceId.make("pi"),
          model: "omlx/Qwen3.8-27B-8bit",
          options: [{ id: "thinkingLevel", value: "high" }],
        },
      });
      yield* tap.waitFor((event) => event.type === "turn.completed");

      const commands = yield* Effect.promise(harness.readLoggedCommands);
      const setModel = commands.find((command) => command.type === "set_model");
      assert.equal(setModel?.provider, "omlx");
      assert.equal(setModel?.modelId, "Qwen3.8-27B-8bit");
      const setThinking = commands.find((command) => command.type === "set_thinking_level");
      assert.equal(setThinking?.level, "high");

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(tap.fiber);
    }),
  );

  it.effect("rolls back N turns by forking at the target user entry", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => makeFakePi("basic"));
      const adapter = yield* makeTestAdapter(harness.binaryPath);
      const threadId = ThreadId.make("pi-mock-rollback");

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });

      const snapshot = yield* adapter.readThread(threadId);
      assert.equal(snapshot.turns.length, 2);

      const rolled = yield* adapter.rollbackThread(threadId, 1);
      assert.equal(rolled.turns.length, 2);

      const commands = yield* Effect.promise(harness.readLoggedCommands);
      const fork = commands.find((command) => command.type === "fork");
      assert.equal(fork?.entryId, "entry-3");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("emits runtime.error and session.exited when the process dies mid-run", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => makeFakePi("exit-mid-run"));
      const adapter = yield* makeTestAdapter(harness.binaryPath);
      const threadId = ThreadId.make("pi-mock-exit");
      const tap = yield* makeEventTap(adapter);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      yield* adapter.sendTurn({ threadId, input: "doom" });

      yield* tap.waitFor((event) => event.type === "session.exited");
      const error = tap.seen.find((event) => event.type === "runtime.error");
      assert.isDefined(error);
      assert.equal(yield* adapter.hasSession(threadId), false);
      yield* Fiber.interrupt(tap.fiber);
    }),
  );
});

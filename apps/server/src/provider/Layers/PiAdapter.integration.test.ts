/**
 * Real-pi integration test. Only runs when T3_PI_INTEGRATION=1 and the pi
 * binary resolves; exercises the true RPC protocol end to end against the
 * environment's configured models (a session file lands in pi's own session
 * storage for the temp project cwd, exactly like a CLI session would).
 */
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { describe } from "@effect/vitest";
import {
  PiSettings,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ServerConfig } from "../../config.ts";
import { makePiAdapter, parsePiModelSlug } from "./PiAdapter.ts";
import { probePiInventory } from "../piRuntime.ts";

const RUN_INTEGRATION = process.env.T3_PI_INTEGRATION === "1";

const decodePiSettings = Schema.decodeSync(PiSettings);

const integrationLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-pi-integration-",
}).pipe(Layer.provideMerge(NodeServices.layer));

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
          if (predicate(event)) return event;
        }
      });
    return { fiber, waitFor, seen };
  });
}

describe.skipIf(!RUN_INTEGRATION)("PiAdapterLive (real pi)", () => {
  it.layer(integrationLayer)("runs a full turn", (it) => {
  it.effect("runs a full turn against the real pi RPC process", () =>
    Effect.gen(function* () {
      // Pick the first available model with auth so the test works on any
      // configured environment (prefers the pinned default when present).
      const inventory = yield* probePiInventory({
        binaryPath: "pi",
        cwd: process.cwd(),
        environment: process.env as NodeJS.ProcessEnv,
      });

      assert.isTrue(inventory.models.length > 0, "no authenticated pi models available");
      const pinned = inventory.models.find(
        (model) => `${model.provider}/${model.id}` === "omlx/Qwen3.8-27B-4bit",
      );
      const chosen = pinned ?? inventory.models[0]!;
      const modelSlug = `${chosen.provider}/${chosen.id}`;
      assert.isDefined(parsePiModelSlug(modelSlug));

      const adapter = yield* makePiAdapter(
        decodePiSettings({ binaryPath: "pi" }),
      ).pipe(Effect.orDie);
      const threadId = ThreadId.make("pi-integration-turn");
      const tap = yield* makeEventTap(adapter);

      const session = yield* adapter.startSession({
        threadId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("pi"), model: modelSlug },
      });
      assert.equal(session.model, modelSlug);

      yield* adapter.sendTurn({
        threadId,
        input: "Reply with exactly the word PONG and nothing else. Do not use any tools.",
        modelSelection: { instanceId: ProviderInstanceId.make("pi"), model: modelSlug },
      });

      const completed = yield* tap.waitFor(
        (event) => event.type === "turn.completed" || event.type === "turn.aborted",
      );
      const deltaText = tap.seen
        .filter((event) => event.type === "content.delta")
        .map((event) => (event.type === "content.delta" ? event.payload.delta : ""))
        .join("");
      assert.equal(completed.type, "turn.completed");
      assert.include(deltaText.toUpperCase(), "PONG");

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(tap.fiber);
    }),
  );
  });
});

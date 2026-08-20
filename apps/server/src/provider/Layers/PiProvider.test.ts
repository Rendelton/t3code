/**
 * PiProvider snapshot tests against the scripted pi RPC peer
 * (scripts/pi-mock-rpc.mjs): verifies the version + inventory probes and
 * their mapping into ServerProvider drafts.
 */
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { PiSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { checkPiProviderStatus, makePendingPiProvider } from "./PiProvider.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);
const __dirname = NodePath.dirname(import.meta.url.replace("file://", ""));
const mockRpcPath = NodePath.join(__dirname, "../../../scripts/pi-mock-rpc.mjs");
async function makeFakePi() {
  const dir = await NodeFS.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-provider-test-"));
  const wrapperPath = NodePath.join(dir, "fake-pi.sh");
  const script = `#!/bin/sh\nexec ${JSON.stringify(process.execPath)} ${JSON.stringify(mockRpcPath)} "$@"\n`;
  await NodeFS.writeFile(wrapperPath, script, "utf8");
  await NodeFS.chmod(wrapperPath, 0o755);
  return wrapperPath;
}

const piProviderTestLayer = NodeServices.layer;

it.layer(NodeServices.layer)("PiProvider", (it) => {
  it.effect("pending snapshot is warning + custom models only", () =>
    Effect.gen(function* () {
      const draft = yield* makePendingPiProvider(
        decodePiSettings({ customModels: ["omlx/Custom-Model"] }),
      );
      assert.equal(draft.displayName, "pi");
      assert.equal(draft.enabled, true);
      assert.equal(draft.status, "warning");
      assert.equal(draft.models.length, 1);
      assert.equal(draft.models[0]?.slug, "omlx/Custom-Model");
      assert.isTrue(draft.models[0]?.isCustom);
    }),
  );

  it.effect("disabled snapshot short-circuits", () =>
    Effect.gen(function* () {
      const draft = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: false }),
        process.cwd(),
      );
      assert.equal(draft.status, "disabled");
      assert.equal(draft.enabled, false);
    }),
  );

  it.effect("missing binary surfaces as not-installed warning", () =>
    Effect.gen(function* () {
      const draft = yield* checkPiProviderStatus(
        decodePiSettings({ binaryPath: "/nonexistent/definitely-not-pi" }),
        process.cwd(),
      );
      assert.equal(draft.status, "error");
      assert.isFalse(draft.installed);
      assert.include(draft.message ?? "", "not installed");
    }),
  );

  it.effect("ready snapshot maps models, thinking levels, and commands", () =>
    Effect.gen(function* () {
      const wrapperPath = yield* Effect.promise(makeFakePi);
      const draft = yield* checkPiProviderStatus(
        decodePiSettings({ binaryPath: wrapperPath, customModels: ["omlx/Extra"] }),
        process.cwd(),
      );
      assert.equal(draft.status, "ready");
      assert.isTrue(draft.installed);
      assert.equal(draft.auth.status, "authenticated");
      assert.equal(draft.auth.type, "pi");
      // Built-in (from probe) + custom (from settings).
      assert.deepEqual(
        draft.models.map((model) => model.slug),
        ["omlx/Qwen3.8-27B-4bit", "omlx/Extra"],
      );
      const builtin = draft.models[0];
      assert.equal(builtin?.subProvider, "omlx");
      assert.isFalse(builtin?.isCustom);
      const descriptors = builtin?.capabilities?.optionDescriptors ?? [];
      assert.equal(descriptors[0]?.id, "thinkingLevel");
      // Custom models keep empty capabilities.
      assert.isTrue((draft.models[1]?.capabilities?.optionDescriptors ?? []).length === 0);
    }),
  );
});

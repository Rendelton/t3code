/**
 * PiProvider — server provider snapshot for the pi coding agent.
 *
 * Builds `ServerProvider` drafts from two probes:
 *   - `pi --version` (cheap, catches a missing binary)
 *   - a short-lived `pi --mode rpc --no-session` process asked for
 *     `get_available_models` + `get_commands`. pi only lists models with
 *     valid authentication, so a non-empty model list doubles as the auth
 *     signal.
 *
 * @module provider/Layers/PiProvider
 */
import {
  DEFAULT_MODEL_BY_PROVIDER,
  type ModelCapabilities,
  type PiSettings,
  ProviderDriverKind,
  type ServerProviderModel,
  type ServerProviderSkill,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  PI_INVENTORY_PROBE_TIMEOUT_MS,
  PI_VERSION_PROBE_TIMEOUT_MS,
  probePiInventory,
  type PiInventory,
} from "../piRuntime.ts";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

const PI_PRESENTATION = {
  displayName: "pi",
  showInteractionModeToggle: false,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const PI_DRIVER_KIND = ProviderDriverKind.make("pi");
const PI_DEFAULT_MODEL = DEFAULT_MODEL_BY_PROVIDER[PI_DRIVER_KIND] ?? null;

function titleCaseSlug(value: string): string {
  const segments: Array<string> = [];
  for (const segment of value.split(/[-_/]+/)) {
    if (segment.length > 0) {
      segments.push(segment.charAt(0).toUpperCase() + segment.slice(1));
    }
  }
  return segments.join(" ");
}

function thinkingLevelCapabilities(
  levels: ReadonlyArray<string>,
): ModelCapabilities | undefined {
  if (levels.length === 0) {
    return undefined;
  }
  const defaultValue = levels.includes("medium") ? "medium" : levels[0];
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "thinkingLevel",
        label: "Thinking",
        type: "select" as const,
        description: "Reasoning effort for this model.",
        options: levels.map((level) =>
          level === defaultValue
            ? { id: level, label: titleCaseSlug(level), isDefault: true as const }
            : { id: level, label: titleCaseSlug(level) },
        ),
        currentValue: defaultValue,
      },
    ],
  });
}

function piModelsToServerModels(
  inventory: PiInventory,
): ReadonlyArray<ServerProviderModel> {
  const models: Array<ServerProviderModel> = [];
  let pinnedDefaultSeen = false;
  for (const model of inventory.models) {
    const slug = `${model.provider}/${model.id}`;
    const isPinnedDefault = PI_DEFAULT_MODEL !== null && slug === PI_DEFAULT_MODEL;
    pinnedDefaultSeen = pinnedDefaultSeen || isPinnedDefault;
    const entry: ServerProviderModel = {
      slug,
      name: model.name,
      subProvider: model.provider,
      isCustom: false,
      ...(isPinnedDefault ? { isDefault: true } : {}),
      capabilities: model.thinkingLevels
        ? (thinkingLevelCapabilities(model.thinkingLevels) ?? null)
        : null,
    };
    models.push(entry);
  }
  return models;
}

function piCommandsToSlashCommands(
  inventory: PiInventory,
): ReadonlyArray<ServerProviderSlashCommand> {
  return inventory.commands
    .filter((command) => command.name.length > 0)
    .map((command) => ({
      name: command.name,
      ...(command.description ? { description: command.description } : {}),
    }));
}

function piCommandsToSkills(inventory: PiInventory): ReadonlyArray<ServerProviderSkill> {
  return inventory.commands
    .filter((command) => command.source === "skill" && command.path !== undefined)
    .map((command) => ({
      name: command.name.replace(/^skill:/, ""),
      ...(command.description ? { description: command.description } : {}),
      path: command.path as string,
      ...(command.scope ? { scope: command.scope } : {}),
      enabled: true,
    }));
}

function piModelsFromSettings(settings: PiSettings): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings([], settings.customModels, EMPTY_CAPABILITIES);
}

export const makePendingPiProvider = (piSettings: PiSettings): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = piModelsFromSettings(piSettings);
    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "pi is disabled in T3 Code settings.",
        },
      });
    }
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking pi availability...",
      },
    });
  });

const runPiVersionCommand = (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
): Effect.Effect<
  { readonly stdout: string; readonly stderr: string; readonly code: number },
  unknown,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Effect.gen(function* () {
    const command = piSettings.binaryPath || "pi";
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  cwd: string,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const customModels = piModelsFromSettings(piSettings);

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: customModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "pi is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runPiVersionCommand(piSettings, environment).pipe(
    Effect.timeoutOption(PI_VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("pi CLI health check failed.", {
      errorTag: typeof error === "object" && error !== null && "_tag" in error ? error._tag : "unknown",
    });
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: customModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "pi CLI (`pi`) is not installed or not on PATH. Install it with `npm install -g @earendil-works/pi-coding-agent`."
          : "Failed to execute the pi CLI health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: customModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "pi CLI is installed but timed out while running `pi --version`.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: customModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "pi CLI is installed but failed to run.",
      },
    });
  }

  const inventoryExit = yield* probePiInventory({
    binaryPath: piSettings.binaryPath || "pi",
    cwd,
    environment,
    ...(piSettings.agentDir ? { agentDir: piSettings.agentDir } : {}),
  }).pipe(
    Effect.timeoutOption(PI_INVENTORY_PROBE_TIMEOUT_MS),
    Effect.exit,
  );

  if (Exit.isFailure(inventoryExit)) {
    yield* Effect.logWarning("pi inventory probe failed.", {
      errorTag:
        typeof inventoryExit.cause === "object" && inventoryExit.cause !== null && "_tag" in inventoryExit.cause
          ? String(inventoryExit.cause._tag)
          : "unknown",
    });
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: customModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "pi CLI is installed but its RPC mode failed to start. Check `pi --version` output for install issues.",
      },
    });
  }

  if (Option.isNone(inventoryExit.value)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: customModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: `pi inventory probe timed out after ${PI_INVENTORY_PROBE_TIMEOUT_MS}ms.`,
      },
    });
  }

  const inventory = inventoryExit.value.value;
  const models = providerModelsFromSettings(
    piModelsToServerModels(inventory),
    piSettings.customModels,
    EMPTY_CAPABILITIES,
  );
  const modelCount = inventory.models.length;
  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    slashCommands: piCommandsToSlashCommands(inventory),
    skills: piCommandsToSkills(inventory),
    probe: {
      installed: true,
      version,
      status: modelCount > 0 ? "ready" : "warning",
      auth: {
        status: modelCount > 0 ? "authenticated" : "unknown",
        type: "pi",
      },
      message:
        modelCount > 0
          ? `${modelCount} model${modelCount === 1 ? "" : "s"} available through pi.`
          : "pi is available, but no models have valid authentication. Run `pi auth` or configure a provider API key.",
    },
  });
});

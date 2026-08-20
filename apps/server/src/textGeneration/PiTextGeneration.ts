/**
 * PiTextGeneration — one-shot text generation through the pi CLI.
 *
 * Spawns `pi -p --no-session` per request (thinking off) and parses the
 * JSON object the shared prompts request. Sessions are deliberately
 * ephemeral: generation must never touch a user's pi session history.
 *
 * @module textGeneration/PiTextGeneration
 */
import { type ModelSelection, type PiSettings, TextGenerationError } from "@t3tools/contracts";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import { ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";
import { parsePiModelSlug } from "../provider/Layers/PiAdapter.ts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";

const PI_TEXT_GENERATION_TIMEOUT_MS = 120_000;

/** Fold a child process byte stream into text. */
const decodeTextFold = <E>(stream: Stream.Stream<Uint8Array, E>): Effect.Effect<string, E> =>
  stream.pipe(
    Stream.decodeText(),
    Stream.runFold(() => "", (acc: string, chunk: string) => acc + chunk),
  );

type TextGenerationOperation =
  | "generateCommitMessage"
  | "generatePrContent"
  | "generateBranchName"
  | "generateThreadTitle";

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const runPiJson = Effect.fn("runPiJson")(function* <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchema,
    modelSelection,
  }: {
    operation: TextGenerationOperation;
    cwd: string;
    prompt: string;
    outputSchema: S;
    modelSelection: ModelSelection;
  }): Effect.fn.Return<S["Type"], TextGenerationError, S["DecodingServices"]> {
    const parsedModel = parsePiModelSlug(modelSelection.model);
    if (!parsedModel) {
      return yield* new TextGenerationError({
        operation,
        detail: `pi text generation requires a 'provider/id' model selection (got '${modelSelection.model}').`,
      });
    }
    const thinking = getModelSelectionStringOptionValue(modelSelection, "thinkingLevel");
    const spawnCommand = yield* resolveSpawnCommand(
      piSettings.binaryPath || "pi",
      [
        "-p",
        "--no-session",
        "--approve",
        "--no-extensions",
        "--no-prompt-templates",
        "--no-skills",
        "--no-themes",
        "--no-context-files",
        "--tools",
        "",
        "--model",
        `${parsedModel.provider}/${parsedModel.id}${thinking ? `:${thinking}` : ":off"}`,
        prompt,
      ],
      { env: environment },
    );
    const exit = yield* commandSpawner
      .spawn(
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          cwd,
          env: {
            ...environment,
            ...(piSettings.agentDir ? { PI_CODING_AGENT_DIR: piSettings.agentDir } : {}),
          },
          shell: spawnCommand.shell,
        }),
      )
      .pipe(
        Effect.scoped,
        Effect.flatMap((child) =>
          Effect.all(
            [
              child.stdout.pipe(decodeTextFold),
              child.stderr.pipe(decodeTextFold),
              child.exitCode,
            ],
            { concurrency: "unbounded" },
          ),
        ),
        Effect.timeoutOption(PI_TEXT_GENERATION_TIMEOUT_MS),
        Effect.mapError((cause) =>
          new TextGenerationError({
            operation,
            detail: `Failed to run the pi CLI for ${operation}.`,
            cause,
          }),
        ),
      );
    if (exit._tag === "None") {
      return yield* new TextGenerationError({
        operation,
        detail: `pi ${operation} timed out after ${PI_TEXT_GENERATION_TIMEOUT_MS}ms.`,
      });
    }
    const [stdout, stderr, exitCode] = exit.value;
    if (Number(exitCode) !== 0) {
      return yield* new TextGenerationError({
        operation,
        detail: `pi ${operation} exited with code ${Number(exitCode)}: ${stderr.trim().slice(0, 500)}`,
      });
    }
    const trimmed = stdout.trim();
    if (!trimmed) {
      return yield* new TextGenerationError({
        operation,
        detail: `pi ${operation} returned empty output.`,
      });
    }
    const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchema));
    return yield* decodeOutput(extractJsonObject(trimmed)).pipe(
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: `pi ${operation} returned invalid structured output: ${trimmed.slice(0, 500)}`,
            cause,
          }),
      ),
    );
  });

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("PiTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });
      const generated = yield* runPiJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("PiTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        changeRequestTemplate: input.changeRequestTemplate,
        policy: input.policy,
      });
      const generated = yield* runPiJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("PiTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
      });
      const generated = yield* runPiJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("PiTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
      });
      const generated = yield* runPiJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizeThreadTitle(generated.title),
      };
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});

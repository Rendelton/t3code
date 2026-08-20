import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import {
  buildPiEnvironment,
  parsePiRpcRecord,
  piCommandsFromResponse,
  piModelsFromResponse,
  piRpcRecords,
} from "./piRuntime.ts";

const collectRecords = (chunks: ReadonlyArray<Uint8Array>): Promise<Array<string>> =>
  Stream.make(...chunks)
    .pipe(piRpcRecords, Stream.runCollect)
    .pipe(Effect.runPromise)
    .then((chunk) => Array.from(chunk));

const encoder = new TextEncoder();
const bytes = (text: string): Uint8Array => encoder.encode(text);

describe("piRpcRecords", () => {
  it("splits records on LF only", async () => {
    const records = await collectRecords([
      bytes('{"type":"a"}\n{"type":"b"}\n'),
      bytes('{"type":"c"}\n'),
    ]);
    expect(records).toEqual(['{"type":"a"}', '{"type":"b"}', '{"type":"c"}']);
  });

  it("does not split on U+2028/U+2029 inside JSON strings", async () => {
    // readline would treat these as line separators; the pi protocol forbids that.
    const payload = `{"delta":"line1\u2028line2\u2029more"}\n`;
    const records = await collectRecords([bytes(payload), bytes('{"next":true}\n')]);
    expect(records).toEqual([`{"delta":"line1\u2028line2\u2029more"}`, '{"next":true}']);
  });

  it("reassembles records split across chunk boundaries, including mid-character", async () => {
    // "héllo" — é is a 2-byte UTF-8 sequence; split it across chunks.
    const full = `{"delta":"héllo"}\n`;
    const raw = encoder.encode(full);
    const records = await collectRecords([raw.subarray(0, 12), raw.subarray(12)]);
    expect(records).toEqual(['{"delta":"héllo"}']);
  });

  it("accepts CRLF-terminated records", async () => {
    const records = await collectRecords([bytes('{"type":"a"}\r\n')]);
    expect(records).toEqual(['{"type":"a"}']);
  });

  it("drops empty records", async () => {
    const records = await collectRecords([bytes("\n\n\n")]);
    expect(records).toEqual([]);
  });

  it("flushes a trailing record without a newline", async () => {
    const records = await collectRecords([bytes('{"partial":true}')]);
    expect(records).toEqual(['{"partial":true}']);
  });
});

describe("parsePiRpcRecord", () => {
  it("parses valid JSON", () => {
    const result = parsePiRpcRecord('{"type":"response"}');
    expect(result).toEqual({ ok: true, value: { type: "response" } });
  });

  it("rejects invalid JSON without throwing", () => {
    expect(parsePiRpcRecord("not json")).toEqual({ ok: false });
  });
});

describe("buildPiEnvironment", () => {
  it("sets PI_CODING_AGENT_DIR when an agentDir is given", () => {
    const env = buildPiEnvironment({ PATH: "/bin" }, "~/.pi-custom");
    expect(env.PI_CODING_AGENT_DIR).toBe("~/.pi-custom");
    expect(env.PATH).toBe("/bin");
  });

  it("leaves the environment untouched without an agentDir", () => {
    const env = buildPiEnvironment({ PATH: "/bin" }, undefined);
    expect(env).toEqual({ PATH: "/bin" });
  });
});

describe("piModelsFromResponse", () => {
  it("maps pi models with thinking levels", () => {
    const models = piModelsFromResponse({
      models: [
        {
          id: "Qwen3.8-27B-4bit",
          name: "Qwen3.8-27B-4bit",
          provider: "omlx",
          reasoning: true,
          input: ["text", "image"],
          thinkingLevelMap: { off: null, low: "low", high: "high", max: "max" },
        },
        { id: "plain", provider: "p", reasoning: false },
      ],
    });
    expect(models).toHaveLength(2);
    expect(models[0]).toMatchObject({
      provider: "omlx",
      id: "Qwen3.8-27B-4bit",
      imageInput: true,
      thinkingLevels: ["low", "high", "max"],
    });
    expect(models[1]).toMatchObject({ id: "plain", imageInput: false });
  });

  it("returns [] for malformed payloads", () => {
    expect(piModelsFromResponse(undefined)).toEqual([]);
    expect(piModelsFromResponse({ models: "nope" })).toEqual([]);
    expect(piModelsFromResponse({ models: [{ id: "" }] })).toEqual([]);
  });
});

describe("piCommandsFromResponse", () => {
  it("maps commands from all sources", () => {
    const commands = piCommandsFromResponse({
      commands: [
        {
          name: "commit",
          description: "Commit staged work",
          source: "extension",
          sourceInfo: { path: "/home/u/.pi/agent/extensions/git-commit.ts", scope: "user" },
        },
        { name: "skill:hf-cli", source: "skill", sourceInfo: { path: "/skills/hf/SKILL.md" } },
      ],
    });
    expect(commands).toEqual([
      {
        name: "commit",
        description: "Commit staged work",
        source: "extension",
        path: "/home/u/.pi/agent/extensions/git-commit.ts",
        scope: "user",
      },
      {
        name: "skill:hf-cli",
        description: undefined,
        source: "skill",
        path: "/skills/hf/SKILL.md",
        scope: undefined,
      },
    ]);
  });
});

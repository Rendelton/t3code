import { describe, expect, it } from "@effect/vitest";

import { mightCarryUsage, parsePiLine, totalTokens } from "./usageTranscripts.ts";

const usage = {
  input: 1867,
  output: 344,
  cacheRead: 4096,
  cacheWrite: 10,
  reasoning: 100,
  cost: { total: 0.00194264 },
};
const entry = {
  type: "message",
  id: "1234abcd",
  timestamp: "2026-08-01T10:00:00Z",
  message: { role: "assistant", model: "Qwen3.8-27B-4bit", usage },
};

describe("Pi usage", () => {
  it("preserves custom model costs and disjoint token counts", () => {
    const line = JSON.stringify(entry);
    expect(mightCarryUsage(line, "pi")).toBe(true);
    const record = parsePiLine(line, "session.jsonl")!;
    expect(record).toMatchObject({
      provider: "pi",
      model: entry.message.model,
      sessionId: "session.jsonl",
      reportedCostUsd: usage.cost.total,
      totals: {
        uncachedInputTokens: 1867,
        outputTokens: 344,
        cachedInputTokens: 4096,
        cacheCreationTokens: 10,
        reasoningTokens: 100,
      },
    });
    expect(totalTokens(record.totals)).toBe(6317);
    expect(parsePiLine(line, "fork.jsonl")?.dedupeKey).toBe(record.dedupeKey);
  });

  it.each([0, undefined, -1, "bad"])("distinguishes free from absent/invalid cost (%s)", (cost) => {
    expect(
      parsePiLine(
        JSON.stringify({
          ...entry,
          message: { ...entry.message, usage: { ...usage, cost: { total: cost } } },
        }),
      )?.reportedCostUsd,
    ).toBe(cost === 0 ? 0 : null);
  });

  it.each(["compaction", "branch_summary", "toolResult"])(
    "counts %s without recounting retained messages",
    (type) => {
      const record = parsePiLine(
        JSON.stringify(
          type === "toolResult"
            ? { ...entry, message: { role: type, usage } }
            : { ...entry, type, usage, retainedTail: [entry.message] },
        ),
      );
      expect(record).toMatchObject({ model: "Tools/summaries", reportedCostUsd: usage.cost.total });
      expect(totalTokens(record!.totals)).toBe(6317);
    },
  );

  it("ignores malformed and unrelated entries", () => {
    for (const line of [
      "{",
      "null",
      JSON.stringify({ ...entry, timestamp: "bad" }),
      JSON.stringify({ ...entry, message: { ...entry.message, role: "user" } }),
      JSON.stringify({ ...entry, type: "custom" }),
    ]) {
      expect(parsePiLine(line)).toBeNull();
    }
  });
});

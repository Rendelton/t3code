#!/usr/bin/env node
/**
 * pi-mock-rpc — scripted pi RPC peer for PiAdapter tests.
 *
 * Speaks the pi `--mode rpc` JSONL protocol over stdin/stdout. The scenario
 * is selected with FAKE_PI_SCENARIO; every command received on stdin is also
 * appended (one JSON per line) to $FAKE_PI_LOG so tests can assert on what
 * the adapter wrote.
 */
import { appendFileSync } from "node:fs";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const scenario = process.env.FAKE_PI_SCENARIO ?? "basic";
const logPath = process.env.FAKE_PI_LOG;
const sessionDir = mkdtempSync(join(tmpdir(), "fake-pi-session-"));
let sessionFile = join(sessionDir, "session.jsonl");
let model = { id: "Qwen3.8-27B-4bit", name: "Qwen3.8-27B-4bit", provider: "omlx" };
let thinkingLevel = "medium";
let streaming = false;
let settled = false;

const write = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);
const respond = (id, command, data, error) =>
  write({
    type: "response",
    ...(id ? { id } : {}),
    command,
    success: error === undefined,
    ...(data !== undefined ? { data } : {}),
    ...(error !== undefined ? { error } : {}),
  });
const log = (line) => {
  if (logPath) {
    try {
      appendFileSync(logPath, `${JSON.stringify(line)}\n`);
    } catch {}
  }
};

const USAGE = {
  input: 120,
  output: 34,
  cacheRead: 10,
  cacheWrite: 0,
  totalTokens: 154,
  cost: { input: 0.01, output: 0.02, cacheRead: 0, cacheWrite: 0, total: 0.03 },
};

function basicAssistantRun() {
  streaming = true;
  settled = false;
  write({ type: "agent_start" });
  write({ type: "turn_start" });
  write({ type: "message_start" });
  write({
    type: "message_update",
    usage: USAGE,
    assistantMessageEvent: { type: "text_start", contentIndex: 0 },
  });
  write({
    type: "message_update",
    usage: USAGE,
    assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Hello " },
  });
  write({
    type: "message_update",
    usage: USAGE,
    assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "world" },
  });
  write({
    type: "message_update",
    usage: USAGE,
    assistantMessageEvent: { type: "text_end", contentIndex: 0, content: "Hello world" },
  });
  write({
    type: "tool_execution_start",
    toolCallId: "call-1",
    toolName: "bash",
    args: { command: "echo hi" },
  });
  write({
    type: "tool_execution_update",
    toolCallId: "call-1",
    toolName: "bash",
    args: { command: "echo hi" },
    partialResult: { content: [{ type: "text", text: "hi" }] },
  });
  write({
    type: "tool_execution_end",
    toolCallId: "call-1",
    toolName: "bash",
    result: { content: [{ type: "text", text: "hi" }], details: {} },
    isError: false,
  });
  write({
    type: "message_end",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "Hello world" }],
      provider: model.provider,
      model: model.id,
      usage: USAGE,
      stopReason: "stop",
    },
  });
  write({ type: "turn_end" });
  write({ type: "agent_end", messages: [], willRetry: false });
  write({ type: "agent_settled" });
  streaming = false;
  settled = true;
}

const ENTRIES = {
  entries: [
    {
      type: "message",
      id: "entry-1",
      parentId: null,
      message: { role: "user", content: "first prompt" },
    },
    {
      type: "message",
      id: "entry-2",
      parentId: "entry-1",
      message: { role: "assistant", content: [{ type: "text", text: "first answer" }] },
    },
    {
      type: "message",
      id: "entry-3",
      parentId: "entry-2",
      message: { role: "user", content: "second prompt" },
    },
    {
      type: "message",
      id: "entry-4",
      parentId: "entry-3",
      message: { role: "assistant", content: [{ type: "text", text: "second answer" }] },
    },
  ],
  leafId: "entry-4",
};

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  while (true) {
    const newline = buffer.indexOf("\n");
    if (newline === -1) break;
    let line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (line.endsWith("\r")) line = line.slice(0, -1);
    if (!line.trim()) continue;
    let command;
    try {
      command = JSON.parse(line);
    } catch {
      continue;
    }
    log(command);
    handleCommand(command);
  }
});

function handleCommand(command) {
  switch (command.type) {
    case "get_state":
      respond(command.id, "get_state", {
        model,
        thinkingLevel,
        isStreaming: streaming,
        sessionFile,
        sessionId: "fake-session-1",
        sessionName: null,
        autoCompactionEnabled: true,
        messageCount: 0,
        pendingMessageCount: 0,
      });
      return;

    case "get_available_models":
      respond(command.id, "get_available_models", {
        models: [
          { ...model, reasoning: true, input: ["text"], contextWindow: 262144, maxTokens: 32768 },
        ],
      });
      return;

    case "get_commands":
      respond(command.id, "get_commands", { commands: [] });
      return;

    case "set_model":
      model = { ...model, provider: command.provider, id: command.modelId };
      respond(command.id, "set_model", { ...model });
      return;

    case "set_thinking_level":
      thinkingLevel = command.level;
      respond(command.id, "set_thinking_level");
      return;

    case "set_session_name":
      respond(command.id, "set_session_name");
      return;

    case "prompt":
      respond(command.id, "prompt");
      if (scenario === "exit-mid-run") {
        write({ type: "agent_start" });
        write({ type: "message_start" });
        write({
          type: "message_update",
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "partial" },
        });
        setTimeout(() => process.exit(1), 20);
        return;
      }
      if (scenario === "confirm-dialog") {
        streaming = true;
        write({ type: "agent_start" });
        write({
          type: "extension_ui_request",
          id: "dialog-1",
          method: "confirm",
          title: "Allow bash?",
          message: "echo hi",
        });
        return;
      }
      if (scenario === "select-dialog") {
        streaming = true;
        write({ type: "agent_start" });
        write({
          type: "extension_ui_request",
          id: "dialog-2",
          method: "select",
          title: "Pick one",
          options: ["red", "blue"],
        });
        return;
      }
      if (scenario === "steer") {
        // First prompt starts streaming and stays open; a second prompt
        // (steering) settles the run.
        if (!streaming) {
          streaming = true;
          write({ type: "agent_start" });
          write({
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "started" },
          });
          return;
        }
        write({
          type: "message_update",
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: " steered" },
        });
        write({ type: "agent_end", messages: [], willRetry: false });
        write({ type: "agent_settled" });
        streaming = false;
        return;
      }
      if (scenario === "abort") {
        streaming = true;
        write({ type: "agent_start" });
        write({
          type: "message_update",
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "working" },
        });
        return;
      }
      basicAssistantRun();
      return;

    case "steer":
      respond(command.id, "steer");
      return;

    case "abort":
      respond(command.id, "abort");
      if (scenario === "abort") {
        write({ type: "agent_end", messages: [], willRetry: false });
        write({ type: "agent_settled" });
        streaming = false;
      }
      return;

    case "get_entries":
      respond(command.id, "get_entries", ENTRIES);
      return;

    case "fork":
      sessionFile = join(sessionDir, "session-fork.jsonl");
      respond(command.id, "fork", { text: "first prompt", cancelled: false });
      return;

    case "get_session_stats":
      respond(command.id, "get_session_stats", {
        sessionFile,
        sessionId: "fake-session-1",
        userMessages: 0,
        assistantMessages: 0,
        toolCalls: 0,
        toolResults: 0,
        totalMessages: 0,
        tokens: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        cost: 0,
      });
      return;

    case "extension_ui_response":
      // Test-controlled replies; the scenario's settled continuation runs
      // once the dialog is answered.
      if (scenario === "confirm-dialog" || scenario === "select-dialog") {
        write({
          type: "tool_execution_end",
          toolCallId: "call-after-dialog",
          toolName: "bash",
          result: { content: [{ type: "text", text: "ran" }], details: {} },
          isError: false,
        });
        write({ type: "agent_end", messages: [], willRetry: false });
        write({ type: "agent_settled" });
        streaming = false;
      }
      return;

    default:
      respond(command.id, String(command.type ?? "unknown"));
  }
}

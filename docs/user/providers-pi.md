# pi

[pi](https://github.com/earendil-works/pi) is an open-source coding agent from Earendil Works. The
pi provider in T3 Code runs pi as a subprocess and talks to it over its RPC protocol, so your pi
setup — providers, models, extensions, skills, and sessions — is exactly what T3 Code drives.

## Setup

Install pi and sign in to at least one model provider:

```bash
npm install -g @earendil-works/pi-coding-agent
pi auth check --provider <provider>
```

pi supports API keys and OAuth for Anthropic, OpenAI, Google, Z.ai, custom OpenAI-compatible
endpoints (via `~/.pi/agent/models.json`), and more. Run `pi --list-models` to confirm which models
are available with your credentials — T3 Code shows that same list.

No T3 Code configuration is required. The default pi provider uses the `pi` binary on your `PATH`
and pi's own config directory (`~/.pi/agent`).

## Settings

| Setting | What it does |
| --- | --- |
| Binary path | Path to the pi binary. Defaults to `pi` on your `PATH`. |
| Agent directory | pi's config directory. Leave blank to use `~/.pi/agent`. |

Model and thinking-level pickers come from your pi installation and refresh automatically.

## Using pi threads

- **Threads, models, and thinking.** Start a thread with any available `provider/model` pair.
  Switching the model or thinking level mid-thread applies immediately — no new thread needed.
- **Your pi sessions are shared.** Every T3 Code pi thread is a real pi session file. Resume the
  same conversation in a terminal with `pi --resume`, and a thread restarted in T3 Code picks its
  session back up.
- **Approvals.** With a thread's access mode set to *Approval required*, pi's `bash`, `edit`, and
  `write` tools pause for your decision in T3 Code's approval UI. Other access modes run them
  without asking, exactly like pi's own full-access default.
- **Steering.** Sending a message while pi is working steers the run, like pi's own steering queue.
- **Slash commands and skills.** Your pi extensions, prompt templates, and skills appear in the
  composer's slash-command list and run inside pi when invoked.

## Differences from the CLI

- Free-text pi dialogs (extension `input`/`editor` prompts) are surfaced as requests, but answering
  is not supported yet — responding cancels the dialog.
- pi compacts long conversations automatically; compaction shows in the thread timeline like other
  providers' compaction.
- T3 Code always starts pi with an explicit thinking level (`off` unless you pick one), so your
  global `defaultThinkingLevel` setting in `~/.pi/agent/settings.json` does not apply to T3 Code
  threads. Pick the level per model in the model picker instead.

## Updating

pi is npm-managed. T3 Code checks the npm registry for new versions and shows an update notice;
run the in-app update (which runs `pi update`) or update manually:

```bash
pi update
```

## Troubleshooting

- **"pi CLI is not installed or not on PATH"** — install pi, or set Binary path to its full path.
- **"no models have valid authentication"** — run `pi auth check --provider <provider>` or add a
  provider API key, then refresh providers in Settings.
- **A self-hosted model returns empty responses** — some OpenAI-compatible servers fail on
  thinking levels they don't support. Set the model's thinking level to `off` in the model picker.

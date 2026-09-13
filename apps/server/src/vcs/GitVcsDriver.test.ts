import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/unstable/process";
import { assert, it } from "@effect/vitest";

import { CheckpointRef, GitCommandError } from "@t3tools/contracts";
import * as ServerConfig from "../config.ts";
import * as GitVcsDriver from "./GitVcsDriver.ts";
import * as VcsProcess from "./VcsProcess.ts";
import * as VcsDriverRegistry from "./VcsDriverRegistry.ts";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import { runVcsDriverContractSuite } from "./testing/VcsDriverContractHarness.ts";

const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-git-vcs-contract-",
});
const GitContractLayer = Layer.mergeAll(GitVcsDriver.vcsLayer, GitVcsDriver.layer).pipe(
  Layer.provide(ServerConfigLayer),
  Layer.provideMerge(VcsProcess.layer),
  Layer.provideMerge(NodeServices.layer),
);

const runGit = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const driver = yield* GitVcsDriver.GitVcsDriver;
    yield* driver.execute({
      operation: "GitVcsDriver.contract.git",
      cwd,
      args,
      timeoutMs: 10_000,
    });
  });

type GitContractError = GitCommandError | PlatformError.PlatformError;

runVcsDriverContractSuite<GitVcsDriver.GitVcsDriver, GitContractError>({
  name: "Git",
  kind: "git",
  layer: GitContractLayer,
  fixture: {
    createRepo: (cwd) =>
      Effect.gen(function* () {
        yield* runGit(cwd, ["init"]);
        yield* runGit(cwd, ["config", "user.email", "test@test.com"]);
        yield* runGit(cwd, ["config", "user.name", "Test"]);
      }),
    writeFile: (cwd, relativePath, contents) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const absolutePath = path.join(cwd, relativePath);
        yield* fileSystem.makeDirectory(path.dirname(absolutePath), { recursive: true });
        yield* fileSystem.writeFileString(absolutePath, contents);
      }),
    trackFile: (cwd, relativePath) => runGit(cwd, ["add", relativePath]),
    commit: (cwd, message) => runGit(cwd, ["commit", "-m", message]),
    ignorePath: (cwd, pattern) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        yield* fileSystem.writeFileString(path.join(cwd, ".gitignore"), `${pattern}\n`);
      }),
  },
});

it.effect("restores empty checkpoints without changing paths outside the workspace", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    for (const nested of [false, true]) {
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-empty-checkpoint-" });
      yield* runGit(root, ["init"]);
      yield* runGit(root, ["config", "user.email", "test@test.com"]);
      yield* runGit(root, ["config", "user.name", "Test"]);
      if (nested) {
        yield* fileSystem.writeFileString(path.join(root, "outside.txt"), "original\n");
        yield* runGit(root, ["add", "."]);
      }
      yield* runGit(root, ["commit", "--allow-empty", "-m", "initial"]);
      const cwd = nested ? path.join(root, "nested") : root;
      yield* fileSystem.makeDirectory(cwd, { recursive: true });
      const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/empty");
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      if (nested) {
        yield* fileSystem.writeFileString(path.join(root, "outside.txt"), "changed\n");
        yield* runGit(root, ["add", "outside.txt"]);
      }
      for (const staged of [false, true]) {
        const addedPath = path.join(cwd, "added.txt");
        yield* fileSystem.writeFileString(addedPath, "new\n");
        if (staged) yield* runGit(cwd, ["add", "added.txt"]);
        assert.isTrue(
          yield* driver.checkpoints.restoreCheckpoint({
            cwd,
            checkpointRef,
            fallbackToHead: false,
          }),
        );
        assert.isFalse(yield* fileSystem.exists(addedPath));
      }
      yield* fileSystem.writeFileString(
        path.join(root, ".git", "info", "exclude"),
        "ignored.txt\n",
      );
      yield* fileSystem.writeFileString(path.join(cwd, "ignored.txt"), "keep\n");
      yield* fileSystem.makeDirectory(path.join(cwd, "untracked"));
      yield* fileSystem.writeFileString(path.join(cwd, "untracked", "file.txt"), "remove\n");
      assert.isTrue(
        yield* driver.checkpoints.restoreCheckpoint({ cwd, checkpointRef, fallbackToHead: false }),
      );
      assert.strictEqual(yield* fileSystem.readFileString(path.join(cwd, "ignored.txt")), "keep\n");
      assert.isFalse(yield* fileSystem.exists(path.join(cwd, "untracked")));
      if (nested) {
        assert.strictEqual(
          yield* fileSystem.readFileString(path.join(root, "outside.txt")),
          "changed\n",
        );
        const staged = yield* driver.execute({
          operation: "test",
          cwd: root,
          args: ["diff", "--cached", "--name-only"],
        });
        assert.strictEqual(staged.stdout.trim(), "outside.txt");
      }
    }
  }).pipe(Effect.scoped, Effect.provide(GitContractLayer)),
);

it.effect("GitVcsDriver forwards execute env to the VCS process", () => {
  let observedEnv: NodeJS.ProcessEnv | undefined;
  let observedAppendTruncationMarker: boolean | undefined;
  let observedOutputMode: VcsProcess.VcsProcessInput["outputMode"];

  return Effect.gen(function* () {
    const driver = yield* GitVcsDriver.makeVcsDriverShape();

    yield* driver.execute({
      operation: "GitVcsDriver.test.env",
      cwd: "/repo",
      args: ["status"],
      env: {
        GIT_INDEX_FILE: "/tmp/t3-index",
      },
      appendTruncationMarker: true,
      outputMode: "error",
    });

    assert.deepStrictEqual(observedEnv, {
      GIT_INDEX_FILE: "/tmp/t3-index",
    });
    assert.strictEqual(observedAppendTruncationMarker, true);
    assert.strictEqual(observedOutputMode, "error");
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(VcsProcess.VcsProcess)({
          run: (input) =>
            Effect.sync(() => {
              observedEnv = input.env;
              observedAppendTruncationMarker = input.appendTruncationMarker;
              observedOutputMode = input.outputMode;
              return {
                exitCode: ChildProcessSpawner.ExitCode(0),
                stdout: "",
                stderr: "",
                stdoutTruncated: false,
                stderrTruncated: false,
              };
            }),
        }),
      ),
    ),
  );
});

for (const usePointer of [true, false]) {
  it.effect(
    `supports a bare repository ${usePointer ? "through a .git pointer" : "directly"}`,
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-bare-workspace-" });
        const barePath = path.join(root, ".bare");
        yield* runGit(root, ["init", "--bare", "--initial-branch=main", barePath]);
        if (usePointer) {
          yield* fs.writeFileString(path.join(root, ".git"), "gitdir: ./.bare\n");
        }
        const cwd = usePointer ? root : barePath;
        yield* runGit(cwd, ["config", "user.email", "test@test.com"]);
        yield* runGit(cwd, ["config", "user.name", "Test"]);
        // A local remote exercises metadata discovery without network access.
        yield* runGit(cwd, ["remote", "add", "origin", barePath]);
        const registry = yield* VcsDriverRegistry.VcsDriverRegistry;
        const handle = yield* registry.resolve({ cwd });
        assert.equal(handle.kind, "git");
        assert.equal(handle.repository.rootPath, cwd);
        assert.equal(handle.repository.metadataPath, barePath);
        assert.equal(yield* handle.driver.isInsideWorkTree(cwd), false);
        assert.equal((yield* handle.driver.listRemotes(cwd)).remotes[0]?.name, "origin");

        const checkpoints = yield* CheckpointStore.CheckpointStore;
        assert.equal(yield* checkpoints.isGitRepository(cwd), false);
        const driver = yield* GitVcsDriver.GitVcsDriver;
        const status = yield* driver.statusDetailsLocal(cwd);
        assert.equal(status.isRepo, true);
        assert.equal(status.hasOriginRemote, true);
        assert.equal(status.branch, null);
        assert.equal(status.hasWorkingTreeChanges, false);

        const mainPath = path.join(root, "main");
        yield* runGit(root, ["init", "--initial-branch=main", mainPath]);
        yield* runGit(mainPath, ["config", "user.email", "test@test.com"]);
        yield* runGit(mainPath, ["config", "user.name", "Test"]);
        yield* fs.writeFileString(path.join(mainPath, "README.md"), "hello\n");
        yield* runGit(mainPath, ["add", "README.md"]);
        yield* runGit(mainPath, ["commit", "-m", "initial"]);
        yield* runGit(cwd, ["fetch", mainPath, "main:main"]);
        yield* fs.remove(mainPath, { recursive: true });
        yield* runGit(cwd, ["worktree", "add", mainPath, "main"]);
        const refs = yield* driver.listRefs({ cwd });
        assert.equal(refs.isRepo, true);
        assert.equal(refs.refs.find((ref) => ref.name === "main")?.worktreePath, mainPath);
        assert.equal(yield* checkpoints.isGitRepository(mainPath), true);
        assert.equal((yield* registry.resolve({ cwd: mainPath })).repository.rootPath, mainPath);

        const featurePath = path.join(root, "feature");
        yield* driver.createWorktree({
          cwd,
          path: featurePath,
          refName: "main",
          newRefName: "feature",
        });
        assert.equal(yield* fs.readFileString(path.join(featurePath, "README.md")), "hello\n");
        yield* driver.removeWorktree({ cwd, path: featurePath });
        assert.equal(yield* fs.exists(featurePath), false);
      }).pipe(
        Effect.provide(
          CheckpointStore.layer.pipe(
            Layer.provideMerge(VcsDriverRegistry.layer),
            Layer.provideMerge(GitContractLayer),
          ),
        ),
      ),
  );
}

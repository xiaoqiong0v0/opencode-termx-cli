// 把被跟踪的插件源码构建到 opencode 自动发现目录（.opencode 由 opencode 管理，通常不纳入版本库）。
// 用法：node scripts/build-opencode-plugin.mjs
//
// 插件已是单文件 bundle（esbuild），不再需要 .opencode/lib 下的 rpc helper。

import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, rmdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const pluginDir = join(root, "opencode-plugin", "termx");
const outPluginDir = join(root, ".opencode", "plugins");
const outLibDir = join(root, ".opencode", "lib");

mkdirSync(outPluginDir, { recursive: true });

// 1) 调用插件目录自己的 esbuild 构建（与 `npm run build:opencode` 同一脚本）。
// 直接用 node 执行，避免 Windows 上 spawnSync("npm.cmd") 的 EINVAL。
const built = spawnSync(process.execPath, [join(pluginDir, "scripts", "build-opencode.mjs")], {
  cwd: pluginDir,
  stdio: "inherit",
});
if (built.error) {
  console.error(`构建失败：${built.error.message}`);
  process.exit(1);
}
if (built.status !== 0) {
  process.exit(built.status ?? 1);
}

// 2) 清理旧产物，避免 opencode 同时加载 .ts 与 .js 造成工具重复。
rmSync(join(outPluginDir, "termx.ts"), { force: true });
// 旧版 rpc helper 已打进 bundle；目录为空时顺带移除，避免误导。
rmSync(join(outLibDir, "termx-rpc.mjs"), { force: true });
try {
  rmdirSync(outLibDir);
} catch {
  // 目录不存在、非空或占用时忽略。
}

// 3) opencode 会在 .opencode/ 下自动安装依赖；同步运行时依赖与 peer 声明。
const pkg = JSON.parse(readFileSync(join(pluginDir, "package.json"), "utf8"));
// opencode 的插件运行目录需要 @opencode-ai/plugin 可解析，因此同时并入 dependencies。
const runtimeDependencies = { ...pkg.dependencies, ...pkg.peerDependencies };
writeFileSync(
  join(root, ".opencode", "package.json"),
  JSON.stringify(
    {
      type: pkg.type,
      dependencies: runtimeDependencies,
      peerDependencies: pkg.peerDependencies,
    },
    null,
    2,
  ),
  "utf8",
);

console.log("generated: .opencode/plugins/termx.js, .opencode/package.json");

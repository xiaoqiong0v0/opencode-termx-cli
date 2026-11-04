// 构建 opencode 自动发现目录使用的单文件插件（bundle 到仓库根 .opencode/plugins）。
// 运行时依赖由 .opencode/package.json 安装，因此保持 external。
import { build } from "esbuild";
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const outfile = join(root, "..", "..", ".opencode", "plugins", "termx.js");

mkdirSync(dirname(outfile), { recursive: true });

await build({
  entryPoints: [join(root, "src", "termx.ts")],
  outfile,
  bundle: true,
  platform: "node",
  format: "esm",
  target: "node20",
  sourcemap: false,
  logLevel: "info",
  external: ["@opencode-ai/plugin", "@xiaoqiong0v0/opencode-plugin-logger"],
});

console.log(`built: ${outfile}`);

// build 后把非 TS 运行时文件复制到 dist（入口 dist/termx.js 会 import ./termx-rpc.mjs）。
import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const src = join(root, "src");
const dist = join(root, "dist");

mkdirSync(dist, { recursive: true });
for (const file of ["termx-rpc.mjs", "termx-rpc.d.mts"]) {
  copyFileSync(join(src, file), join(dist, file));
}
console.log("copied runtime files to dist/");

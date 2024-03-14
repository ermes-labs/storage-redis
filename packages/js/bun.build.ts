import * as Bun from "bun";

await Bun.build({
  entrypoints: ["./src/index.ts"],
  format: "esm",
  outdir: "./dist",
  sourcemap: "external",
});

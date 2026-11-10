import { existsSync, copyFileSync } from "node:fs";
import { resolve } from "node:path";

const root = process.cwd();

const example = resolve(root, ".env.example");
const env = resolve(root, ".env");

if (existsSync(env)) {
  console.log(".env already exists; leaving it unchanged.");
} else {
  copyFileSync(example, env);
  console.log("Created .env from .env.example");
}

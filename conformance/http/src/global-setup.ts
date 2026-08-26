// vitest globalSetup: runs once per `vitest run`, before any suite file.
// Waits for the host and provisions the fixture tenant (the Worker path
// goes through `PUT /admin/tenants/conf`; the Rust host has it on disk).

import { provisionTenant } from "./seed.ts";
import { env } from "./client.ts";

export default async function setup(): Promise<void> {
  const e = env();
  console.log(`[conformance] host=${e.hostKind} base=${e.baseUrl} tenant=${e.tenant}`);
  await provisionTenant();
}

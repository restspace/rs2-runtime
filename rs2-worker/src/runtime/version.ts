// The Worker's version string. Set from the same git tag as `rs2-core`'s
// `CARGO_PKG_VERSION` by the release workflow (cloudflare.md §G), so
// `user-agent: rs2/<version>` and OpenAPI `info.version` match on both hosts.
export const RS2_VERSION = "0.1.0";

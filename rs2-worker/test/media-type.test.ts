// Port of the `#[cfg(test)]` module in `rs2-core/src/message/media_type.rs`:
// the extension map is a cross-host contract, so both hosts must name the same
// media type for the same file and probe the same extension-less candidates.
import { describe, expect, it } from "vitest";
import { MediaType } from "../src/runtime/media-type";

describe("extension map", () => {
  it("maps media and font extensions", () => {
    const cases: Array<[string, string]> = [
      ["clips/intro.mp4", "video/mp4"],
      ["clips/intro.webm", "video/webm"],
      ["clips/intro.MOV", "video/quicktime"],
      ["audio/theme.mp3", "audio/mpeg"],
      ["audio/theme.ogg", "audio/ogg"],
      ["fonts/inter.woff2", "font/woff2"],
      ["favicon.ico", "image/vnd.microsoft.icon"],
    ];
    for (const [path, essence] of cases) {
      expect(MediaType.forPath(path).essence(), path).toBe(essence);
    }
  });

  it("never sniffs an unknown path", () => {
    expect(MediaType.forPath("a/b/mystery").essence()).toBe("application/octet-stream");
  });

  // Bulk media maps by name but must not join the extension-less probe list —
  // each candidate there costs a store probe on a miss.
  it("keeps bulk media out of negotiation", () => {
    const negotiable = MediaType.knownExtensions().map(([ext]) => ext);
    expect(negotiable).toContain("html");
    expect(negotiable).toContain("png");
    for (const ext of ["mp4", "webm", "mp3", "woff2", "ico"]) {
      expect(negotiable, ext).not.toContain(ext);
      expect(MediaType.fromExtension(ext), ext).toBeDefined();
    }
  });
});

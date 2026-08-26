// Vite transform surfaces the tests use (`?raw` imports, `import.meta.glob`)
// without pulling in `vite/client`'s DOM-flavored globals.

declare module "*?raw" {
  const content: string;
  export default content;
}

interface ImportMeta {
  glob(
    pattern: string,
    options?: { eager?: boolean; query?: string; import?: string },
  ): Record<string, unknown>;
}

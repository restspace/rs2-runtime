// Canned upstream responses per mock host, shaped like the real APIs — a
// port of `rs2-core/tests/sdk_corpus.rs::mock_response`, run as a static
// auxiliary worker and used as the corpus guests' `globalOutbound`
// (see vitest.config.ts and test/engines.test.ts).

const CANNED = {
  "api.stripe.test": { id: "cus_1", object: "customer", email: "ada@example.com" },
  "api.openai.test": {
    id: "chatcmpl-1",
    object: "chat.completion",
    created: 1700000000,
    model: "gpt-4o",
    choices: [{ index: 0, finish_reason: "stop", message: { role: "assistant", content: "hi" } }],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  },
  "api.anthropic.test": {
    id: "msg_1",
    type: "message",
    role: "assistant",
    model: "claude-sonnet-4-6",
    content: [{ type: "text", text: "hi" }],
    stop_reason: "end_turn",
    stop_sequence: null,
    usage: { input_tokens: 1, output_tokens: 1 },
  },
  "api.github.test": { id: 1, full_name: "octo/hello", private: false },
  "proj.supabase.test": [{ id: 1, name: "first" }],
  "api.resend.test": { id: "email_1" },
  "generativelanguage.google.test": {
    candidates: [{ index: 0, finishReason: "STOP", content: { role: "model", parts: [{ text: "hi" }] } }],
    usageMetadata: { promptTokenCount: 1, candidatesTokenCount: 1, totalTokenCount: 2 },
  },
  "api.mistral.test": {
    id: "cmpl-1",
    object: "chat.completion",
    created: 1700000000,
    model: "mistral-small-latest",
    choices: [{ index: 0, finish_reason: "stop", message: { role: "assistant", content: "hi" } }],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  },
  "api.groq.test": {
    id: "chatcmpl-1",
    object: "chat.completion",
    created: 1700000000,
    model: "llama-3.3-70b-versatile",
    choices: [{ index: 0, finish_reason: "stop", message: { role: "assistant", content: "hi" } }],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  },
};

export default {
  async fetch(request) {
    const host = new URL(request.url).hostname;
    const body = CANNED[host];
    const headers = {
      "content-type": "application/json",
      "request-id": "req_mock",
      "x-request-id": "req_mock",
      "content-range": "0-0/1", // supabase pagination metadata
    };
    if (body === undefined) return new Response("{}", { status: 404, headers });
    return new Response(JSON.stringify(body), { status: 200, headers });
  },
};

// G5 corpus: @anthropic-ai/sdk — create a message through the official SDK.
import Anthropic from "@anthropic-ai/sdk";

export default async (msg, ctx) => {
  const client = new Anthropic({
    apiKey: "sk-ant-test",
    baseURL: "https://api.anthropic.test",
    dangerouslyAllowBrowser: true,
    maxRetries: 0,
  });
  const message = await client.messages.create({
    model: "claude-sonnet-4-6",
    max_tokens: 64,
    messages: [{ role: "user", content: "hello" }],
  });
  return {
    status: 200,
    body: { sdk: "anthropic", text: message.content[0].text, stop: message.stop_reason },
  };
};

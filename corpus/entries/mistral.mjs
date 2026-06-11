// G5 corpus: @mistralai/mistralai — chat completion through the official SDK.
import { Mistral } from "@mistralai/mistralai";

export default async (msg, ctx) => {
  const client = new Mistral({ apiKey: "test-key", serverURL: "https://api.mistral.test" });
  const result = await client.chat.complete({
    model: "mistral-small-latest",
    messages: [{ role: "user", content: "hello" }],
  });
  return { status: 200, body: { sdk: "mistral", text: result.choices[0].message.content } };
};

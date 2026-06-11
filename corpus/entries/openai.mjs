// G5 corpus: openai — chat completion through the official SDK.
import OpenAI from "openai";

export default async (msg, ctx) => {
  const client = new OpenAI({
    apiKey: "sk-test",
    baseURL: "https://api.openai.test/v1",
    dangerouslyAllowBrowser: true,
    maxRetries: 0,
  });
  const completion = await client.chat.completions.create({
    model: "gpt-4o",
    messages: [{ role: "user", content: "hello" }],
  });
  return {
    status: 200,
    body: { sdk: "openai", text: completion.choices[0].message.content, model: completion.model },
  };
};

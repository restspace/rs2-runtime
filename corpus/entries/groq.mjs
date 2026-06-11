// G5 corpus: groq-sdk — chat completion through the official SDK.
import Groq from "groq-sdk";

export default async (msg, ctx) => {
  const client = new Groq({
    apiKey: "gsk_test",
    baseURL: "https://api.groq.test",
    dangerouslyAllowBrowser: true,
    maxRetries: 0,
  });
  const completion = await client.chat.completions.create({
    model: "llama-3.3-70b-versatile",
    messages: [{ role: "user", content: "hello" }],
  });
  return { status: 200, body: { sdk: "groq", text: completion.choices[0].message.content } };
};

// G5 corpus: @google/generative-ai — generate content through the official SDK.
import { GoogleGenerativeAI } from "@google/generative-ai";

export default async (msg, ctx) => {
  const genAI = new GoogleGenerativeAI("test-key");
  const model = genAI.getGenerativeModel(
    { model: "gemini-2.0-flash" },
    { baseUrl: "https://generativelanguage.google.test" },
  );
  const result = await model.generateContent("hello");
  return { status: 200, body: { sdk: "gemini", text: result.response.text() } };
};

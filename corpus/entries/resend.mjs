// G5 corpus: resend — send an email through the official SDK.
import "./lib/resend-env.mjs";
import { Resend } from "resend";

export default async (msg, ctx) => {
  const resend = new Resend("re_test_123");
  const { data, error } = await resend.emails.send({
    from: "rs2 <noreply@rs2.test>",
    to: ["ada@example.com"],
    subject: "hello",
    html: "<b>hi</b>",
  });
  if (error) throw new Error(`resend error: ${JSON.stringify(error)}`);
  return { status: 200, body: { sdk: "resend", id: data.id } };
};

// G5 corpus: @supabase/supabase-js — a table select through the official SDK.
import { createClient } from "@supabase/supabase-js";

export default async (msg, ctx) => {
  const supabase = createClient("https://proj.supabase.test", "anon-key");
  const { data, error } = await supabase.from("items").select("*");
  if (error) throw new Error(`supabase error: ${JSON.stringify(error)}`);
  return { status: 200, body: { sdk: "supabase", rows: data.length, first: data[0].name } };
};

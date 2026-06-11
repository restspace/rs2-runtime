// G5 corpus: @slack/web-api — list conversations through the official SDK.
// Node-heavy (axios transport); expected to need shims or fail — tracked as
// corpus data either way.
import { WebClient } from "@slack/web-api";

export default async (msg, ctx) => {
  const web = new WebClient("xoxb-test", { slackApiUrl: "https://slack.test/api/" });
  const result = await web.conversations.list();
  return { status: 200, body: { sdk: "slack", ok: result.ok, count: result.channels.length } };
};

// G5 corpus: @octokit/core — repo lookup through the official SDK.
import { Octokit } from "@octokit/core";

export default async (msg, ctx) => {
  const octokit = new Octokit({ auth: "ghp_test", baseUrl: "https://api.github.test" });
  const { data, status } = await octokit.request("GET /repos/{owner}/{repo}", {
    owner: "octo",
    repo: "hello",
  });
  return {
    status: 200,
    body: { sdk: "octokit", fullName: data.full_name, upstreamStatus: status },
  };
};

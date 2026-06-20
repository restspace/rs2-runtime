// Equivalent Node HTTP server: same trivial work + same JSON shape as
// bench/handler.js, so the comparison is server overhead, not handler logic.
// Single-threaded by default (Node's model); pass --cluster to fork per-core.
const http = require("http");
const os = require("os");

function handle(req, res) {
  let acc = 0;
  for (let i = 0; i < 50; i++) acc += i;
  const body = JSON.stringify({ ok: true, engine: "node", acc });
  res.writeHead(200, { "content-type": "application/json", "x-engine": "node" });
  res.end(body);
}

const PORT = Number(process.env.PORT || 3200);

if (process.argv.includes("--cluster")) {
  const cluster = require("cluster");
  if (cluster.isPrimary) {
    const n = os.cpus().length;
    console.log(`node cluster: forking ${n} workers on :${PORT}`);
    for (let i = 0; i < n; i++) cluster.fork();
  } else {
    http.createServer(handle).listen(PORT);
  }
} else {
  http.createServer(handle).listen(PORT, () =>
    console.log(`node single-thread on :${PORT}`)
  );
}

// Dependency-free closed-loop HTTP load generator. The SAME client drives every
// target so the comparison is fair. Keep-alive connections, fixed concurrency,
// fixed wall-clock duration; reports throughput + latency percentiles.
//
//   node load.js <url> [concurrency] [durationSec] [warmupSec]
const http = require("http");

const url = process.argv[2] || "http://127.0.0.1:3200/";
const CONC = Number(process.argv[3] || 50);
const DUR = Number(process.argv[4] || 10);
const WARMUP = Number(process.argv[5] || 2);

const target = new URL(url);
const agent = new http.Agent({ keepAlive: true, maxSockets: CONC });

let measuring = false;
let done = 0;
let errors = 0;
let stopAt = 0;
const lat = [];

function once(resolve) {
  const start = process.hrtime.bigint();
  const req = http.request(
    {
      host: target.hostname,
      port: target.port,
      path: target.pathname + target.search,
      method: "GET",
      agent,
    },
    (res) => {
      res.on("data", () => {});
      res.on("end", () => {
        if (measuring) {
          const us = Number(process.hrtime.bigint() - start) / 1000;
          lat.push(us);
          done++;
        }
        if (Date.now() < stopAt) once(resolve);
        else resolve();
      });
    }
  );
  req.on("error", () => {
    if (measuring) errors++;
    if (Date.now() < stopAt) once(resolve);
    else resolve();
  });
  req.end();
}

function pct(sorted, p) {
  if (!sorted.length) return 0;
  const i = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[i];
}

async function main() {
  console.log(`load: ${url}  conc=${CONC}  warmup=${WARMUP}s  measure=${DUR}s`);
  // warmup
  stopAt = Date.now() + WARMUP * 1000;
  await Promise.all(Array.from({ length: CONC }, () => new Promise(once)));
  // measure
  measuring = true;
  const t0 = Date.now();
  stopAt = Date.now() + DUR * 1000;
  await Promise.all(Array.from({ length: CONC }, () => new Promise(once)));
  const secs = (Date.now() - t0) / 1000;

  lat.sort((a, b) => a - b);
  const rps = done / secs;
  const mean = lat.reduce((s, x) => s + x, 0) / (lat.length || 1);
  console.log(
    `  req/s ${rps.toFixed(0).padStart(8)}   ` +
      `mean ${mean.toFixed(0)}us  p50 ${pct(lat, 50).toFixed(0)}us  ` +
      `p90 ${pct(lat, 90).toFixed(0)}us  p99 ${pct(lat, 99).toFixed(0)}us  ` +
      `max ${pct(lat, 100).toFixed(0)}us  errors ${errors}`
  );
}
main();

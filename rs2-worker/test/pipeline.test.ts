// Pipeline modules: ports of the `#[cfg(test)]` modules in
// `rs2-core/src/pipeline/{spec,dsl,condition,segments,executor}.rs`.

import { describe, expect, it } from "vitest";

import { evaluateCondition, parseCondition } from "../src/pipeline/condition";
import { convert } from "../src/pipeline/dsl";
import { deriveKey } from "../src/pipeline/executor";
import { plan } from "../src/pipeline/segments";
import { callEffectClass, callStep, emptySpec, emptyStep, specFromJson, specToJson, validateSpec } from "../src/pipeline/spec";
import type { PipelineSpec } from "../src/pipeline/spec";
import { Message } from "../src/runtime/message";

function msgWithStatus(status: number): Message {
  const req = Message.request("GET", "/x", "t");
  return req.response(status, undefined);
}

describe("condition", () => {
  it("parses and evaluates core forms", () => {
    const vars = { order: { status: "open", total: 5 } };
    const msg = msgWithStatus(200);
    const cases: Array<[string, boolean]> = [
      ["ok", true],
      ["status == 200", true],
      ["status >= 400", false],
      ["$order.status == 'open'", true],
      ["$order.total > 4 && ok", true],
      ["!ok || $order.status == 'open'", true],
      ["$missing", false],
      ["method == 'GET'", true],
    ];
    for (const [expr, expected] of cases) {
      expect(evaluateCondition(parseCondition(expr), msg, vars), expr).toBe(expected);
    }
  });

  it("header accessor and grouping", () => {
    const msg = msgWithStatus(200);
    msg.setHeader("x-mode", "manage");
    expect(evaluateCondition(parseCondition('(header("x-mode") == \'manage\') && ok'), msg, {})).toBe(true);
  });

  it("rejects bad grammar at config time", () => {
    for (const bad of ["status ==", "unknownIdent == 1", "status == 200 extra", "'unterminated"]) {
      expect(() => parseCondition(bad), bad).toThrow();
    }
  });
});

describe("spec", () => {
  it("parses the PRD example", () => {
    const spec = specFromJson({
      mode: "serial",
      onFail: "stop",
      concurrency: 12,
      steps: [
        { call: { method: "GET", url: "/data/orders/${id}" }, as: "$order" },
        { if: "$order.status == 'open'", call: { method: "POST", url: "/payments/charge", effect: "keyed" }, try: true },
        { transform: { total: "$sum($order.lines.price)", charged: "$_ok" } },
        {
          pipeline: {
            mode: "parallel",
            steps: [
              { call: { method: "GET", url: "/data/customers/${order.customerId}" }, as: "$customer" },
              { call: { method: "GET", url: "/stock/check" }, as: "$stock" },
            ],
            join: "jsonObject",
          },
        },
      ],
    });
    expect(spec.steps.length).toBe(4);
    expect(spec.onFail).toBe("stop");
    expect(callEffectClass(spec.steps[1]!.call!)).toBe("keyed");
    expect(spec.steps[1]!.tryMode).toBe(true);
    expect(validateSpec(spec)).toEqual([]);
    expect(callEffectClass(spec.steps[0]!.call!)).toBe("pure");
  });

  it("validation parses transform expressions everywhere", () => {
    const spec = specFromJson({
      steps: [
        { if: "status == 999", transform: { ok: "$sum(lines.price)", n: 42, bad: "$sum((" } },
        { transform: { nested: { deep: ["a.b", ")("] } } },
        { pipeline: { steps: [{ transform: "$merge(" }] } },
      ],
    });
    const errors = validateSpec(spec);
    expect(errors.length, errors.join("; ")).toBe(3);
    expect(errors[0]).toContain("/steps[0]");
    expect(errors[0]).toContain("$sum((");
    expect(errors[1]).toContain("/steps[1]");
    expect(errors[2]).toContain("/steps[2]/steps[0]");
  });

  it("validation checks call headers", () => {
    const spec = specFromJson({
      steps: [{ call: { method: "GET", url: "/x", headers: { "bad name": "v", authorization: "Bearer ${unterminated" } } }],
    });
    const errors = validateSpec(spec);
    expect(errors.length, errors.join("; ")).toBe(2);
    const all = errors.join("; ");
    expect(all).toContain("invalid header name 'bad name'");
    expect(all).toContain("in header 'authorization'");
  });

  it("validation catches shape errors", () => {
    const spec = specFromJson({
      steps: [{ as: "$x" }, { if: "status ==", call: { method: "GET", url: "/x" } }, { call: { method: "NOT A METHOD", url: "/x" } }],
    });
    expect(validateSpec(spec).length).toBe(3);
  });

  it("serializes the canonical typed form with serde's field rules", () => {
    const spec: PipelineSpec = { ...emptySpec(), steps: [{ ...callStep("GET", "/x"), tryMode: true, capture: "$a" }, { ...emptyStep(), transform: { a: "b" } }] };
    expect(specToJson(spec)).toEqual({
      mode: "serial",
      steps: [{ call: { method: "GET", url: "/x" }, try: true, as: "$a" }, { transform: { a: "b" } }],
    });
  });
});

describe("dsl", () => {
  it("converts a Restspace-style pipeline", () => {
    const spec = convert([
      "GET /data/orders/${id} :$order",
      "try if ($order.status == 'open') POST /payments/charge",
      { total: "$sum($order.lines.price)" },
      ["parallel", "GET /data/customers/${order.customerId} :customer", "GET /stock/check :stock", "jsonObject"],
    ]);
    expect(spec.steps.length).toBe(4);
    expect(spec.steps[0]!.capture).toBe("$order");
    expect(spec.steps[0]!.call!.url).toBe("/data/orders/${id}");
    expect(spec.steps[1]!.tryMode).toBe(true);
    expect(spec.steps[1]!.condition).toBe("$order.status == 'open'");
    expect(spec.steps[2]!.transform).toBeDefined();
    const sub = spec.steps[3]!.pipeline!;
    expect(sub.mode).toBe("parallel");
    expect(sub.join).toBe("jsonObject");
    expect(sub.steps[0]!.name).toBe("customer");
    expect(validateSpec(spec)).toEqual([]);
  });

  it("mode token with actions", () => {
    const spec = convert(["serial stop end", "GET /x"]);
    expect(spec.mode).toBe("serial");
    expect(spec.onFail).toBe("stop");
    expect(spec.onSucceed).toBe("end");
    expect(() => convert(["GET /x", "serial"])).toThrow();
  });

  it("split and join tokens", () => {
    const spec = convert(["GET /list", "jsonSplit", "GET /detail/${id}", "jsonObject"]);
    expect(spec.steps.length).toBe(3);
    expect(spec.steps[1]!.split).toBe("jsonSplit");
    expect(spec.join).toBe("jsonObject");
  });

  it("rejects bad steps", () => {
    expect(() => convert(["NOSUCHMETHOD"])).toThrow();
    expect(() => convert(["if (unclosed GET /x"])).toThrow();
    expect(() => convert([42])).toThrow();
  });

  it("elevate token sets the flag", () => {
    const spec = convert(["elevate GET /secret/x", "elevate try POST /y"]);
    expect(spec.steps[0]!.elevate).toBe(true);
    expect(spec.steps[1]!.elevate).toBe(true);
    expect(spec.steps[1]!.tryMode).toBe(true);
    expect(convert(["GET /z"]).steps[0]!.elevate).toBe(false);
    expect(() => convert(["elevate"])).toThrow();
  });
});

describe("segments", () => {
  const transformStep = () => ({ ...emptyStep(), transform: { a: "$" } });

  it("transforms partition segments", () => {
    const spec = { ...emptySpec(), steps: [callStep("GET", "/x"), callStep("PUT", "/x"), transformStep(), callStep("GET", "/x"), transformStep()] };
    const p = plan(spec);
    expect(p.segments.map((s) => [s.start, s.end])).toEqual([
      [0, 2],
      [2, 4],
      [4, 5],
    ]);
    expect(p.warnings).toEqual([]);
  });

  it("unsafe step mid-segment warns", () => {
    const spec = { ...emptySpec(), steps: [callStep("POST", "/x"), callStep("GET", "/x"), transformStep()] };
    const p = plan(spec);
    expect(p.warnings.length).toBe(1);
    expect(p.warnings[0]).toContain("steps[0]");
    const keyed = { ...spec, steps: [{ ...callStep("POST", "/x"), call: { method: "POST", url: "/x", effect: "keyed" as const, headers: undefined } }, ...spec.steps.slice(1)] };
    expect(plan(keyed).warnings).toEqual([]);
  });

  it("unsafe step at segment end is fine", () => {
    const spec = { ...emptySpec(), steps: [callStep("GET", "/x"), callStep("POST", "/x"), transformStep(), callStep("GET", "/x")] };
    expect(plan(spec).warnings).toEqual([]);
  });
});

describe("executor", () => {
  it("derived keys are stable and distinct", async () => {
    expect(await deriveKey("inv", "/0")).toBe(await deriveKey("inv", "/0"));
    expect(await deriveKey("inv", "/0")).not.toBe(await deriveKey("inv", "/1"));
    expect(await deriveKey("inv", "/0")).not.toBe(await deriveKey("inv2", "/0"));
  });
});

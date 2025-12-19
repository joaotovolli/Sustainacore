import assert from "node:assert/strict";
import test from "node:test";
import { buildTech100Candidates } from "./tech100_url_candidates.mjs";

test("buildTech100Candidates prioritizes override and de-dupes", () => {
  const result = buildTech100Candidates({
    override: "tech100/index/",
    discovered: ["/tech100/", "/tech100/index/"],
  });
  assert.equal(result[0], "/tech100/index/");
  assert.ok(result.includes("/tech100/"));
  assert.ok(result.includes("/tech100"));
  assert.equal(new Set(result).size, result.length);
});

test("buildTech100Candidates ignores empty values", () => {
  const result = buildTech100Candidates({ override: "  " });
  assert.ok(result.includes("/tech100/index/"));
});

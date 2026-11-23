// Run with: node --test tests/test_comparison_scores.cjs
const test = require("node:test");
const assert = require("node:assert/strict");
const {
  parseScore,
  summarizeScores,
  matchesScore,
  compareScores,
} = require("../prototype/score-tools.js");

test("numeric score parsing keeps zero and decimals, not missing or invalid values", () => {
  for (const value of [
    null,
    undefined,
    "",
    "  ",
    false,
    true,
    [],
    {},
    NaN,
    Infinity,
    -1,
    101,
    "invalid",
  ]) {
    assert.equal(parseScore(value), null, `invalid value: ${String(value)}`);
  }
  for (const [value, expected] of [
    [0, 0],
    ["0", 0],
    [100, 100],
    [" 88.5 ", 88.5],
    [90, 90],
  ]) {
    assert.equal(parseScore(value), expected);
  }
});

test("a grouped official code uses the minimum available score, retaining the range", () => {
  assert.deepEqual(summarizeScores([100, 85.7, "", null]), {
    score: 85.7,
    scoreMax: 100,
  });
  assert.deepEqual(summarizeScores(["", null]), {
    score: null,
    scoreMax: null,
  });
  assert.deepEqual(summarizeScores([100, 0]), { score: 0, scoreMax: 100 });
});

test("range endpoints are inclusive; missing is not zero", () => {
  assert.equal(matchesScore(70, "scored", 70, 90), true);
  assert.equal(matchesScore(90, "scored", 70, 90), true);
  assert.equal(matchesScore(69.9, "scored", 70, 90), false);
  assert.equal(matchesScore(90.1, "scored", 70, 90), false);
  assert.equal(matchesScore(0, "scored", 0, 0), true);
  assert.equal(matchesScore(null, "scored", 0, 100), false);
  assert.equal(matchesScore(100, "scored", 100, null), true);
  assert.equal(matchesScore(0, "scored", null, 0), true);
});

test("availability filtering distinguishes all, scored-only and missing-only", () => {
  assert.equal(matchesScore(null, "all", null, null), true);
  assert.equal(matchesScore(null, "missing", null, null), true);
  assert.equal(matchesScore(0, "missing", null, null), false);
  assert.equal(matchesScore(100, "scored", null, null), true);
});

test("sorting is numeric and missing scores are last in either direction", () => {
  const values = [null, 9, 100, 88.5, 0, null, 90];
  assert.deepEqual(
    [...values].sort((a, b) => compareScores(a, b, "asc")),
    [0, 9, 88.5, 90, 100, null, null],
  );
  assert.deepEqual(
    [...values].sort((a, b) => compareScores(a, b, "desc")),
    [100, 90, 88.5, 9, 0, null, null],
  );
  assert.equal(compareScores(90, 90, "asc"), 0);
  assert.equal(compareScores(null, null, "desc"), 0);
});

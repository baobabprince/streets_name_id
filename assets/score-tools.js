/* Shared score semantics for the comparison UI and offline Node tests. */
(function (root) {
  "use strict";
  function parseScore(value) {
    if (typeof value !== "number" && typeof value !== "string") return null;
    if (typeof value === "string" && value.trim() === "") return null;
    const score = Number(value);
    return Number.isFinite(score) && score >= 0 && score <= 100 ? score : null;
  }
  function summarizeScores(values) {
    const scores = values.map(parseScore).filter((score) => score !== null);
    return {
      score: scores.length ? Math.min(...scores) : null,
      scoreMax: scores.length ? Math.max(...scores) : null,
    };
  }
  function matchesScore(score, mode, min, max) {
    if (mode === "all") return true;
    if (mode === "missing") return score === null;
    return (
      score !== null &&
      (min === null || score >= min) &&
      (max === null || score <= max)
    );
  }
  function compareScores(a, b, direction) {
    // Missing scores always sort last, including descending order. Zero is valid.
    if (a === null) return b === null ? 0 : 1;
    if (b === null) return -1;
    return direction === "desc" ? b - a : a - b;
  }
  const api = { parseScore, summarizeScores, matchesScore, compareScores };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else root.ComparisonScores = api;
})(typeof window !== "undefined" ? window : globalThis);

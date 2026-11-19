/* ==========================================================================
   Settlement map — interactive street inspector
   Vanilla JS, no dependencies. Works on top of the SVG paths emitted by
   generate_html.py (one <path class="street-path"> per OSM way segment).
   ========================================================================== */
(function () {
  "use strict";

  var SVG_NS = "http://www.w3.org/2000/svg";
  var PALETTE = {
    perfect: "#35d0a1",
    partial: "#f6c453",
    ai: "#38bdf8",
    "geo-review": "#a78bfa",
    unmatched: "#fb923c",
    unnamed: "#7c8aa3"
  };
  var LABELS = {
    perfect: "מאושר מדויק",
    partial: "מאושר חלקי",
    ai: "הצעת AI",
    "geo-review": "בדיקה גאוגרפית",
    unmatched: "ללא התאמה מאושרת",
    unnamed: "רחוב ללא שם"
  };
  var PAGE_SIZE = 140;

  var svg = document.getElementById("street-map");
  if (!svg) return;

  var mapWrap = document.getElementById("map-wrap");
  var streetLayer = document.getElementById("street-layer");
  var labelLayer = document.getElementById("map-labels");
  var tooltip = document.getElementById("tooltip");
  var listEl = document.getElementById("street-list");
  var moreBtn = document.getElementById("list-more");
  var emptyEl = document.getElementById("list-empty");
  var resultCount = document.getElementById("result-count");
  var searchInput = document.getElementById("street-search");
  var filterBtns = Array.prototype.slice.call(document.querySelectorAll("[data-filter]"));
  var card = document.getElementById("selection-card");
  var toast = document.getElementById("toast");

  var meta = {};
  try {
    meta = JSON.parse(document.getElementById("page-meta").textContent || "{}") || {};
  } catch (e) { meta = {}; }
  var RECORDS = [];
  try {
    RECORDS = JSON.parse((document.getElementById("street-records") || {}).textContent || "[]") || [];
  } catch (e) { RECORDS = []; }
  var cityName = meta.place || (document.querySelector(".brand h1") || {}).textContent || "";

  /* ------------------------------------------------------------------ *
   * 1. Build a street model out of the SVG paths
   * ------------------------------------------------------------------ */

  var paths = Array.prototype.slice.call(streetLayer.querySelectorAll(".street-path"));
  var records = [];
  var byKey = Object.create(null);
  var totalSegments = paths.length;

  function attr(el, name) { return el.getAttribute(name) || ""; }

  function parseTooltip(el) {
    var raw = attr(el, "data-tooltip").replace(/&#10;/g, "\n");
    var out = { name: "", lamas: "", id: "", score: "", note: "" };
    if (!raw) return out;
    var lines = raw.split("\n").map(function (s) { return s.trim(); }).filter(Boolean);
    if (lines.length) {
      var first = lines[0];
      var arrow = first.split(/\s*->\s*/);
      out.name = arrow[0] || "";
      out.lamas = arrow.length > 1 ? arrow.slice(1).join(" -> ") : "";
      lines.slice(1).forEach(function (line) {
        if (line.indexOf("ציון:") === 0) out.score = line.slice(5).trim();
        else if (line.indexOf("מזהה:") === 0) out.id = line.slice(5).trim();
        else if (line !== "התאמה מאושרת") out.note = line;
      });
    }
    return out;
  }

  paths.forEach(function (path) {
    var status = attr(path, "data-status") || "unmatched";
    var recId = attr(path, "data-rec");
    var raw = recId !== "" ? RECORDS[Number(recId)] : null;
    var fallback = raw ? { name: "", lamas: "", id: "", score: "", note: "" } : parseTooltip(path);
    var name, lamas, lamasId, score, ai, confidence, geo, distance, recStatus;
    if (raw) {
      recStatus = raw[0] || status;
      name = raw[1] || "";
      lamas = raw[2] || "";
      lamasId = raw[3] == null ? "" : String(raw[3]);
      score = raw[4];
      ai = raw[5] == null ? "" : String(raw[5]);
      confidence = raw[6] == null ? "" : String(raw[6]);
      geo = raw[7] == null ? "" : String(raw[7]);
      distance = raw[8] == null ? "" : String(raw[8]);
      status = recStatus;
    } else {
      name = attr(path, "data-osm") || fallback.name || "";
      lamas = attr(path, "data-lamas") || fallback.lamas || "";
      lamasId = attr(path, "data-lamas-id") || fallback.id || "";
      score = attr(path, "data-score") || fallback.score || "";
      ai = attr(path, "data-ai") || "";
      confidence = attr(path, "data-confidence");
      geo = attr(path, "data-geo");
      distance = attr(path, "data-distance");
    }
    var key = recId !== "" ? "r" + recId : [name, lamas, lamasId, status, ai].join("|");
    var rec = byKey[key];
    if (!rec) {
      rec = byKey[key] = {
        key: key,
        name: name,
        lamas: lamas,
        lamasId: lamasId,
        score: (score === "" || score == null) ? null : parseFloat(score),
        status: status,
        ai: ai,
        confidence: confidence,
        geo: geo,
        distance: distance,
        note: fallback.note,
        paths: [],
        length: 0,
        labels: 0
      };
      records.push(rec);
    }
    rec.paths.push(path);
    var segLength = 0;
    try { segLength = path.getTotalLength(); } catch (e) { /* jsdom */ }
    path.__len = segLength;
    rec.length += segLength;
  });

  // Long streets first: they are the ones users look for.
  records.sort(function (a, b) {
    if (b.length !== a.length) return b.length - a.length;
    return String(a.name).localeCompare(String(b.name), "he");
  });
  records.forEach(function (rec, i) { rec.index = i; });

  var counts = { all: records.length, perfect: 0, partial: 0, ai: 0, "geo-review": 0, unmatched: 0, unnamed: 0 };
  var scoreBuckets = new Array(10).fill(0);
  var approved = 0;
  records.forEach(function (rec) {
    counts[rec.status] = (counts[rec.status] || 0) + 1;
    if (rec.status === "perfect" || rec.status === "partial") approved += 1;
    if (rec.score != null && !isNaN(rec.score)) {
      var b = Math.max(0, Math.min(9, Math.floor(rec.score / 10)));
      scoreBuckets[b] += 1;
    }
  });

  /* ------------------------------------------------------------------ *
   * 2. Toolbar UI: counts, kpis, filter + search state
   * ------------------------------------------------------------------ */

  Object.keys(counts).forEach(function (key) {
    document.querySelectorAll('[data-count="' + key + '"]').forEach(function (el) {
      el.textContent = counts[key].toLocaleString("he-IL");
    });
  });

  var noLamasData = !!(meta.diagnostics && meta.diagnostics.total_lamas_streets === 0);
  if (noLamasData) {
    var coveragePill = document.querySelector(".metric-pill.is-good .v");
    var coverageLabel = document.querySelector(".metric-pill.is-good .k");
    if (coveragePill) coveragePill.textContent = "—";
    if (coverageLabel) coverageLabel.textContent = "אין מאגר רשמי להשוואה";
  }

  var kpiUnmatched = document.querySelector('[data-kpi="unmatched"]');
  var kpiAi = document.querySelector('[data-kpi="ai"]');
  var noMatch = counts.unmatched + counts["geo-review"] + counts.ai;
  if (kpiUnmatched) kpiUnmatched.textContent = noMatch.toLocaleString("he-IL");
  if (kpiAi) kpiAi.textContent = counts.ai.toLocaleString("he-IL");

  var state = { filter: "all", query: "", shown: PAGE_SIZE, active: null, labels: true };

  function normalize(text) {
    return String(text || "")
      .toLowerCase()
      .replace(/[\u0591-\u05C7]/g, "")
      .replace(/["'׳״`]/g, "")
      .replace(/[־–—-]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function matches(rec) {
    if (state.filter !== "all" && rec.status !== state.filter) return false;
    if (!state.query) return true;
    var q = state.query;
    var hay = normalize(rec.name) + " " + normalize(rec.lamas) + " " + normalize(rec.lamasId);
    return q.split(" ").every(function (token) { return hay.indexOf(token) !== -1; });
  }

  function filteredRecords() {
    return records.filter(matches);
  }

  /* ------------------------------------------------------------------ *
   * 3. Street list
   * ------------------------------------------------------------------ */

  function statusColor(status) { return PALETTE[status] || PALETTE.unmatched; }

  function scoreText(rec) {
    if (rec.status === "ai") return rec.ai || "AI";
    if (rec.score == null || isNaN(rec.score)) return "—";
    return Math.round(rec.score);
  }

  function renderList(reset) {
    if (reset) { state.shown = PAGE_SIZE; listEl.innerHTML = ""; }
    var list = filteredRecords();
    resultCount.textContent = list.length.toLocaleString("he-IL");
    emptyEl.hidden = list.length !== 0;

    var slice = list.slice(0, state.shown);
    var start = listEl.childElementCount;
    var frag = document.createDocumentFragment();
    slice.slice(start).forEach(function (rec) {
      var li = document.createElement("li");
      li.className = "street-row";
      li.dataset.key = rec.key;
      li.tabIndex = 0;
      li.setAttribute("role", "button");
      var title = rec.name || "רחוב ללא שם";
      var sub = rec.lamas && rec.lamas !== rec.name
        ? rec.lamas
        : (rec.status === "perfect" || rec.status === "partial" ? title : (LABELS[rec.status] || ""));
      li.innerHTML =
        '<span class="swatch" style="background:' + statusColor(rec.status) + '"></span>' +
        '<span class="names"><span class="osm"></span><span class="lamas' + (rec.lamas ? "" : " is-none") + '"></span></span>' +
        '<span class="score"></span>';
      li.querySelector(".osm").textContent = title;
      li.querySelector(".lamas").textContent = sub;
      li.querySelector(".score").textContent = scoreText(rec);
      li.querySelector(".score").style.color = statusColor(rec.status);
      li.addEventListener("click", function () { select(rec, { zoom: true, fromList: true }); });
      li.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); select(rec, { zoom: true, fromList: true }); }
      });
      li.addEventListener("mouseenter", function () { highlight(rec); });
      li.addEventListener("mouseleave", function () { if (!state.active) clearHighlight(); });
      frag.appendChild(li);
    });
    listEl.appendChild(frag);
    moreBtn.hidden = list.length <= state.shown;
    moreBtn.textContent = "הצגת " + Math.min(PAGE_SIZE, Math.max(0, list.length - state.shown)).toLocaleString("he-IL") + " רחובות נוספים";
    if (state.active) markActiveRow(state.active.key);
  }

  moreBtn.addEventListener("click", function () {
    state.shown += PAGE_SIZE;
    renderList(false);
  });

  function markActiveRow(key) {
    listEl.querySelectorAll(".street-row.is-active").forEach(function (el) { el.classList.remove("is-active"); });
    var row = listEl.querySelector('.street-row[data-key="' + cssEscape(key) + '"]');
    if (row) {
      row.classList.add("is-active");
      var scroller = listEl.closest(".side-body") || listEl.parentElement;
      if (!isInView(row, scroller) && row.scrollIntoView) row.scrollIntoView({ block: "nearest" });
    }
  }

  function cssEscape(value) {
    if (window.CSS && CSS.escape) return CSS.escape(value);
    return String(value).replace(/["\\]/g, "\\$&");
  }
  function isInView(el, scroller) {
    if (!scroller) return true;
    var a = el.getBoundingClientRect(), b = scroller.getBoundingClientRect();
    return a.top >= b.top && a.bottom <= b.bottom;
  }

  /* ------------------------------------------------------------------ *
   * 4. Highlight / selection / tooltip
   * ------------------------------------------------------------------ */

  function setPaths(rec, cls, on) {
    rec.paths.forEach(function (p) { p.classList.toggle(cls, on); });
  }

  var highlighted = null;
  function highlight(rec) {
    if (state.active) return;
    if (highlighted === rec) return;
    clearHighlight();
    highlighted = rec;
    setPaths(rec, "is-hover", true);
  }
  function clearHighlight() {
    if (highlighted) setPaths(highlighted, "is-hover", false);
    highlighted = null;
  }

  function select(rec, opts) {
    opts = opts || {};
    if (state.active) {
      setPaths(state.active, "is-pinned", false);
    }
    if (!rec) {
      state.active = null;
      card.hidden = true;
      markActiveRow("");
      updateHash();
      return;
    }
    state.active = rec;
    setPaths(rec, "is-pinned", true);
    showCard(rec);
    markActiveRow(rec.key);
    updateHash();
    if (opts.zoom) zoomToRecord(rec);
    if (!opts.fromList) {
      var row = listEl.querySelector('.street-row[data-key="' + cssEscape(rec.key) + '"]');
      if (!row) {
        // ensure the record is inside the visible page
        var idx = filteredRecords().indexOf(rec);
        if (idx >= state.shown) { state.shown = Math.ceil((idx + 1) / PAGE_SIZE) * PAGE_SIZE; renderList(false); }
      }
      markActiveRow(rec.key);
    }
  }

  function showCard(rec) {
    var title = rec.name || "רחוב ללא שם";
    document.getElementById("sc-title").textContent = title;

    var arrow = document.getElementById("sc-arrow");
    if (rec.lamas && rec.lamas !== rec.name) {
      arrow.innerHTML = '<span aria-hidden="true">↳</span> <strong></strong>';
      arrow.querySelector("strong").textContent = rec.lamas;
    } else {
      arrow.textContent = LABELS[rec.status] || "";
    }

    var badges = document.getElementById("sc-badges");
    badges.innerHTML = "";
    var badge = document.createElement("span");
    badge.className = "badge";
    badge.dataset.tone = rec.status === "geo-review" ? "geo" : rec.status;
    badge.textContent = LABELS[rec.status] || rec.status;
    badges.appendChild(badge);
    if (rec.score != null && !isNaN(rec.score)) {
      var s = document.createElement("span");
      s.className = "badge";
      s.dataset.tone = "partial";
      s.textContent = (rec.status === "ai" ? "דמיון " : "ציון ") + Math.round(rec.score) + "/100";
      badges.appendChild(s);
    }
    if (rec.confidence) {
      var c = document.createElement("span");
      c.className = "badge";
      c.dataset.tone = "ai";
      c.textContent = "ביטחון " + Math.round(parseFloat(rec.confidence) * 100) + "%";
      badges.appendChild(c);
    }

    var dl = document.getElementById("sc-meta");
    dl.innerHTML = "";
    var metaRows = [];
    var approvedStatus = rec.status === "perfect" || rec.status === "partial";
    if (rec.lamasId) metaRows.push([approvedStatus ? "מזהה למ״ס" : "מועמד מוביל (לא מאושר)", rec.lamasId]);
    if (rec.ai) metaRows.push(["הצעת AI (לא מאושרת)", rec.ai]);
    if (rec.geo) metaRows.push(["אימות גאוגרפי", rec.geo]);
    if (rec.distance) metaRows.push(["מרחק מעוגן", Math.round(parseFloat(rec.distance)) + " מ׳"]);
    if (rec.note && metaRows.length < 4) metaRows.push(["הערה", rec.note]);
    metaRows.forEach(function (pair) {
      var dt = document.createElement("dt"); dt.textContent = pair[0];
      var dd = document.createElement("dd"); dd.textContent = pair[1];
      dl.appendChild(dt); dl.appendChild(dd);
    });
    if (!metaRows.length) {
      var dt = document.createElement("dt"); dt.textContent = "סטטוס";
      var dd = document.createElement("dd"); dd.textContent = "אין התאמה מאושרת למאגר הרשמי";
      dl.appendChild(dt); dl.appendChild(dd);
    }

    var gov = document.getElementById("sc-govmap");
    gov.disabled = !rec.name;
    gov.style.opacity = rec.name ? "" : "0.5";
    card.hidden = false;
  }

  document.getElementById("sc-close").addEventListener("click", function () { select(null); });
  document.getElementById("sc-zoom").addEventListener("click", function () {
    if (state.active) zoomToRecord(state.active);
  });
  document.getElementById("sc-govmap").addEventListener("click", function () {
    if (!state.active || !state.active.name) return;
    var q = state.active.name + ", " + cityName;
    window.open("https://www.govmap.gov.il/?q=" + encodeURIComponent(q) + "&z=16", "_blank", "noopener");
  });

  /* tooltip */
  var isTouch = window.matchMedia("(hover: none)").matches;
  function moveTip(ev) {
    var pad = 14;
    var x = ev.clientX + pad, y = ev.clientY + pad;
    if (x + tooltip.offsetWidth > window.innerWidth - 8) x = ev.clientX - tooltip.offsetWidth - pad;
    if (y + tooltip.offsetHeight > window.innerHeight - 8) y = ev.clientY - tooltip.offsetHeight - pad;
    tooltip.style.left = x + "px";
    tooltip.style.top = y + "px";
  }
  function fillTip(rec, ev) {
    var title = rec.name || "רחוב ללא שם";
    var html = '<div class="tip-title"></div>';
    var rows = [[LABELS[rec.status] || rec.status, ""]];
    if (rec.lamas && rec.lamas !== rec.name) rows.push(["שם רשמי", rec.lamas]);
    if (rec.score != null && !isNaN(rec.score)) rows.push(["ציון", Math.round(rec.score) + "/100"]);
    if (rec.lamasId) rows.push(["מזהה למ״ס", rec.lamasId]);
    if (rec.ai) rows.push(["הצעת AI", rec.ai]);
    tooltip.innerHTML = html;
    tooltip.querySelector(".tip-title").textContent = title;
    rows.forEach(function (pair) {
      var row = document.createElement("div");
      row.className = "tip-row";
      var k = document.createElement("span"); k.textContent = pair[0];
      var v = document.createElement("b"); v.textContent = pair[1];
      row.appendChild(k); row.appendChild(v);
      tooltip.appendChild(row);
    });
    tooltip.classList.add("is-visible");
    moveTip(ev);
  }

  function recordFromPath(path) {
    var recId = attr(path, "data-rec");
    if (recId !== "") return byKey["r" + recId] || null;
    var fallback = parseTooltip(path);
    var key = [attr(path, "data-osm") || fallback.name, attr(path, "data-lamas") || fallback.lamas,
      attr(path, "data-lamas-id") || fallback.id, attr(path, "data-status"), attr(path, "data-ai")].join("|");
    return byKey[key] || null;
  }

  if (!isTouch) {
    svg.addEventListener("pointerover", function (ev) {
      var path = ev.target.closest && ev.target.closest(".street-path");
      if (!path) return;
      var rec = recordFromPath(path);
      if (!rec || state.active) return;
      highlight(rec);
      fillTip(rec, ev);
    });
    svg.addEventListener("pointermove", function (ev) {
      if (tooltip.classList.contains("is-visible")) moveTip(ev);
    });
    svg.addEventListener("pointerout", function (ev) {
      var path = ev.target.closest && ev.target.closest(".street-path");
      if (!path) return;
      tooltip.classList.remove("is-visible");
      clearHighlight();
    });
  }

  svg.addEventListener("click", function (ev) {
    var path = ev.target.closest && ev.target.closest(".street-path");
    if (!path) return;
    var rec = recordFromPath(path);
    if (rec) select(rec, { zoom: false });
  });

  svg.addEventListener("dblclick", function (ev) {
    var path = ev.target.closest && ev.target.closest(".street-path");
    if (!path) return;
    var rec = recordFromPath(path);
    if (rec) { select(rec, {}); zoomToRecord(rec); }
  });

  /* ------------------------------------------------------------------ *
   * 5. Filter + search wiring
   * ------------------------------------------------------------------ */

  function setFilter(value) {
    state.filter = value;
    filterBtns.forEach(function (btn) {
      btn.setAttribute("aria-pressed", String(btn.dataset.filter === value));
    });
    applyVisibility();
    renderList(true);
    updateHash();
  }

  filterBtns.forEach(function (btn) {
    btn.addEventListener("click", function () { setFilter(btn.dataset.filter); });
  });

  var searchTimer = null;
  searchInput.addEventListener("input", function () {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(function () {
      state.query = normalize(searchInput.value);
      applyVisibility();
      renderList(true);
      updateHash();
    }, 110);
  });
  searchInput.addEventListener("keydown", function (ev) {
    if (ev.key === "Escape") { searchInput.value = ""; state.query = ""; applyVisibility(); renderList(true); updateHash(); }
  });

  function applyVisibility() {
    var visible = Object.create(null);
    filteredRecords().forEach(function (rec) { visible[rec.key] = true; });
    records.forEach(function (rec) {
      var dim = !visible[rec.key];
      rec.paths.forEach(function (p) { p.classList.toggle("is-dim", dim); });
    });
    if (state.active && !visible[state.active.key]) select(null);
    scheduleLabels(true);
  }

  /* ------------------------------------------------------------------ *
   * 6. SVG viewport: pan / zoom / fit / focus
   * ------------------------------------------------------------------ */

  var base = (function () {
    var vb = svg.viewBox.baseVal;
    return { x: vb.x, y: vb.y, w: vb.width, h: vb.height };
  })();
  var view = { x: base.x, y: base.y, w: base.w, h: base.h };
  var animAt = null;
  var MIN_W = Math.max(base.w / 400, 4);
  var MAX_W = base.w * 1.6;

  function setView(v) {
    view = v;
    svg.setAttribute("viewBox", v.x + " " + v.y + " " + v.w + " " + v.h);
    scheduleLabels();
  }

  function animateTo(target, ms) {
    ms = ms || 320;
    if (animAt) cancelAnimationFrame(animAt);
    var from = { x: view.x, y: view.y, w: view.w, h: view.h };
    var t0 = performance.now();
    function step(now) {
      var t = Math.min(1, (now - t0) / ms);
      var e = 1 - Math.pow(1 - t, 3);
      setView({
        x: from.x + (target.x - from.x) * e,
        y: from.y + (target.y - from.y) * e,
        w: from.w + (target.w - from.w) * e,
        h: from.h + (target.h - from.h) * e
      });
      if (t < 1) animAt = requestAnimationFrame(step);
    }
    animAt = requestAnimationFrame(step);
  }

  function clientToSvg(clientX, clientY) {
    var pt = svg.createSVGPoint();
    pt.x = clientX; pt.y = clientY;
    var m = svg.getScreenCTM();
    return m ? pt.matrixTransform(m.inverse()) : pt;
  }

  function zoomAround(clientX, clientY, factor) {
    var w = Math.max(MIN_W, Math.min(MAX_W, view.w / factor));
    var scale = w / view.w;
    var p = clientToSvg(clientX, clientY);
    var x = p.x - (p.x - view.x) * scale;
    var y = p.y - (p.y - view.y) * scale;
    setView({ x: x, y: y, w: w, h: view.h * scale });
  }

  // wheel zoom
  svg.addEventListener("wheel", function (ev) {
    ev.preventDefault();
    var factor = Math.pow(1.0016, -ev.deltaY);
    zoomAround(ev.clientX, ev.clientY, Math.max(0.6, Math.min(1.7, factor)));
  }, { passive: false });

  // drag pan (pointer events; works for mouse + touch)
  var dragging = false, dragStart = null, moved = false;
  svg.addEventListener("pointerdown", function (ev) {
    if (ev.button != null && ev.button !== 0) return;
    if (ev.target.closest(".street-path")) return;
    dragging = true; moved = false;
    dragStart = { x: ev.clientX, y: ev.clientY, view: { x: view.x, y: view.y } };
    svg.classList.add("is-dragging");
    try { svg.setPointerCapture(ev.pointerId); } catch (e) {}
  });
  svg.addEventListener("pointermove", function (ev) {
    if (!dragging || !dragStart) return;
    var dx = ev.clientX - dragStart.x, dy = ev.clientY - dragStart.y;
    if (Math.abs(dx) + Math.abs(dy) > 3) moved = true;
    var rect = svg.getBoundingClientRect();
    var scaleX = view.w / rect.width, scaleY = view.h / rect.height;
    setView({ x: dragStart.view.x - dx * scaleX, y: dragStart.view.y - dy * scaleY, w: view.w, h: view.h });
  });
  function endDrag(ev) {
    if (!dragging) return;
    dragging = false;
    svg.classList.remove("is-dragging");
    if (ev && ev.pointerId != null) { try { svg.releasePointerCapture(ev.pointerId); } catch (e) {} }
  }
  svg.addEventListener("pointerup", endDrag);
  svg.addEventListener("pointercancel", endDrag);
  svg.addEventListener("pointerleave", function () { if (dragging) endDrag(); });

  document.getElementById("zoom-in").addEventListener("click", function () {
    var r = svg.getBoundingClientRect();
    zoomAround(r.left + r.width / 2, r.top + r.height / 2, 1.4);
  });
  document.getElementById("zoom-out").addEventListener("click", function () {
    var r = svg.getBoundingClientRect();
    zoomAround(r.left + r.width / 2, r.top + r.height / 2, 1 / 1.4);
  });
  document.getElementById("zoom-fit").addEventListener("click", function () { animateTo(base); });
  document.getElementById("zoom-street").addEventListener("click", function () {
    if (state.active) zoomToRecord(state.active);
    else toastMsg("בחרו רחוב כדי להתמקד בו");
  });

  function recordBBox(rec) {
    var box = null;
    rec.paths.forEach(function (p) {
      var b;
      try { b = p.getBBox(); } catch (e) { return; }
      if (!b || !b.width && !b.height) return;
      if (!box) box = { x: b.x, y: b.y, x2: b.x + b.width, y2: b.y + b.height };
      else {
        box.x = Math.min(box.x, b.x);
        box.y = Math.min(box.y, b.y);
        box.x2 = Math.max(box.x2, b.x + b.width);
        box.y2 = Math.max(box.y2, b.y + b.height);
      }
    });
    if (!box) return null;
    return { x: box.x, y: box.y, w: Math.max(box.x2 - box.x, 1), h: Math.max(box.y2 - box.y, 1) };
  }

  function zoomToRecord(rec) {
    var b = recordBBox(rec);
    if (!b) return;
    var pad = Math.max(b.w, b.h) * 0.5 + 24;
    var w = Math.min(MAX_W, Math.max(MIN_W, Math.max(b.w, b.h) + pad * 2));
    var cx = b.x + b.w / 2, cy = b.y + b.h / 2;
    var ratio = base.h / base.w;
    animateTo({ x: cx - w / 2, y: cy - (w * ratio) / 2, w: w, h: w * ratio });
  }

  /* ------------------------------------------------------------------ *
   * 7. Street labels at higher zoom levels
   * ------------------------------------------------------------------ */

  var labelsEnabled = true;
  var labelTimer = null;
  var labelsDirty = false;

  function scheduleLabels(force) {
    labelsDirty = true;
    clearTimeout(labelTimer);
    labelTimer = setTimeout(function () { drawLabels(); }, 110);
  }

  function currentScale() { return base.w / view.w; }

  function drawLabels() {
    labelsDirty = false;
    labelLayer.innerHTML = "";
    if (!labelsEnabled || !state.labels) return;
    var scale = currentScale();
    if (scale < 1.7) return;

    var rect = svg.getBoundingClientRect();
    var pxWidth = rect.width || 900;
    var userPerPx = view.w / pxWidth;
    var fontUser = 11.5 * userPerPx;
    var occupied = Object.create(null);
    var cellW = 92 * userPerPx, cellH = 20 * userPerPx;
    var placed = 0, maxLabels = 260;

    // Prefer long streets, then matched streets.
    for (var i = 0; i < records.length && placed < maxLabels; i++) {
      var rec = records[i];
      if (state.query && !matches(rec)) continue;
      if (!rec.name) continue;
      if (!matches(rec)) continue;
      if (rec.labelPath === undefined) {
        rec.labelPath = null;
        rec.labelLen = -1;
        for (var j = 0; j < rec.paths.length; j++) {
          var candidate = rec.paths[j];
          var candidateLen = candidate.__len || 0;
          if (candidateLen > rec.labelLen) { rec.labelLen = candidateLen; rec.labelPath = candidate; }
        }
      }
      var target = rec.labelPath, best = rec.labelLen;
      if (!target || best < 60 * userPerPx) continue;
      var mid, mid2;
      try {
        mid = target.getPointAtLength(best * 0.5);
        mid2 = target.getPointAtLength(Math.min(best, best * 0.5 + 12 * userPerPx));
      } catch (e) { continue; }
      var angle = Math.atan2(mid2.y - mid.y, mid2.x - mid.x) * 180 / Math.PI;
      if (angle > 90) angle -= 180;
      if (angle < -90) angle += 180;
      if (mid.x < view.x - 40 * userPerPx || mid.x > view.x + view.w + 40 * userPerPx) continue;
      if (mid.y < view.y - 20 * userPerPx || mid.y > view.y + view.h + 20 * userPerPx) continue;
      var cx = Math.floor(mid.x / cellW), cy = Math.floor(mid.y / cellH);
      var clash = false;
      for (var ox = -1; ox <= 1 && !clash; ox++) {
        for (var oy = -1; oy <= 1; oy++) {
          if (occupied[(cx + ox) + ":" + (cy + oy)]) { clash = true; break; }
        }
      }
      if (clash) continue;
      occupied[cx + ":" + cy] = true;

      var text = document.createElementNS(SVG_NS, "text");
      text.textContent = rec.name;
      text.setAttribute("x", 0);
      text.setAttribute("y", 0);
      text.setAttribute("text-anchor", "middle");
      text.setAttribute("font-size", fontUser.toFixed(Math.max(1, Math.round(-Math.log10(fontUser)) + 1)));
      text.setAttribute("transform", "translate(" + mid.x + "," + mid.y + ") rotate(" + angle.toFixed(1) + ")");
      text.setAttribute("opacity", rec.status === "unmatched" ? "0.75" : "1");
      labelLayer.appendChild(text);
      placed++;
    }
  }

  document.getElementById("labels-toggle").addEventListener("click", function () {
    labelsEnabled = !labelsEnabled;
    state.labels = labelsEnabled;
    this.setAttribute("aria-pressed", String(labelsEnabled));
    drawLabels();
  });

  /* ------------------------------------------------------------------ *
   * 8. Insights: donut, histogram, KPI tiles, legacy diagnostics
   * ------------------------------------------------------------------ */

  function renderInsights() {
    var donut = document.getElementById("status-donut");
    var legend = document.getElementById("donut-legend");
    var order = ["perfect", "partial", "ai", "geo-review", "unmatched", "unnamed"];
    var total = counts.all || 1;
    var stops = [];
    var acc = 0;
    order.forEach(function (key) {
      var n = counts[key] || 0;
      if (!n) return;
      var share = n / total * 100;
      stops.push(PALETTE[key] + " " + acc.toFixed(2) + "% " + (acc + share).toFixed(2) + "%");
      acc += share;
    });
    donut.style.background = stops.length ? "conic-gradient(" + stops.join(",") + ")" : "var(--surface-3)";
    document.getElementById("donut-pct").textContent = Math.round(approved / total * 100) + "%";

    legend.innerHTML = "";
    order.forEach(function (key) {
      var n = counts[key] || 0;
      if (!n) return;
      var row = document.createElement("div");
      row.className = "row";
      row.innerHTML = '<span class="key" style="background:' + PALETTE[key] + '"></span><span></span><strong></strong>';
      row.children[1].textContent = LABELS[key];
      row.children[2].textContent = n.toLocaleString("he-IL");
      legend.appendChild(row);
    });

    var hist = document.getElementById("score-hist");
    var max = Math.max.apply(null, scoreBuckets.concat([1]));
    hist.innerHTML = "";
    scoreBuckets.forEach(function (n, i) {
      var bar = document.createElement("div");
      bar.className = "bar";
      bar.style.height = Math.max(3, Math.round(n / max * 88)) + "px";
      bar.title = (i * 10) + "–" + (i * 10 + 9) + ": " + n.toLocaleString("he-IL") + " רחובות";
      var span = document.createElement("span");
      span.textContent = n || "";
      bar.appendChild(span);
      hist.appendChild(bar);
    });

    var tiles = document.getElementById("kpi-tiles");
    var tileData = [
      ["סה״כ רחובות", counts.all],
      ["מאושרים", approved],
      ["הצעות AI", counts.ai],
      ["בדיקה גאוגרפית", counts["geo-review"]],
      ["ללא התאמה", counts.unmatched],
      ["מקטעים במפה", totalSegments]
    ];
    tiles.innerHTML = "";
    tileData.forEach(function (pair) {
      var tile = document.createElement("div");
      tile.className = "diag-tile";
      tile.innerHTML = '<div class="label"></div><div class="value metric"></div>';
      tile.querySelector(".label").textContent = pair[0];
      tile.querySelector(".value").textContent = Number(pair[1]).toLocaleString("he-IL");
      tiles.appendChild(tile);
    });
  }

  /* tabs */
  var tabStreets = document.getElementById("tab-streets");
  var tabInsights = document.getElementById("tab-insights");
  function showTab(which) {
    var insights = which === "insights";
    tabStreets.setAttribute("aria-selected", String(!insights));
    tabInsights.setAttribute("aria-selected", String(insights));
    document.getElementById("panel-streets").hidden = insights;
    document.getElementById("panel-insights").hidden = !insights;
  }
  tabStreets.addEventListener("click", function () { showTab("streets"); });
  tabInsights.addEventListener("click", function () { showTab("insights"); });

  /* ------------------------------------------------------------------ *
   * 9. Shareable URL state, export, theme, shortcuts
   * ------------------------------------------------------------------ */

  function updateHash() {
    var parts = [];
    if (state.query) parts.push("q=" + encodeURIComponent(searchInput.value.trim()));
    if (state.filter !== "all") parts.push("filter=" + state.filter);
    if (state.active) parts.push("street=" + encodeURIComponent(state.active.name || state.active.key));
    var hash = parts.join("&");
    try {
      if (hash) history.replaceState(null, "", "#" + hash);
      else history.replaceState(null, "", location.pathname + location.search);
    } catch (e) { /* file:// or sandboxed iframe */ }
  }

  function readHash() {
    var params = {};
    (location.hash || "").replace(/^#/, "").split("&").forEach(function (part) {
      if (!part) return;
      var idx = part.indexOf("=");
      var k = idx === -1 ? part : part.slice(0, idx);
      var v = idx === -1 ? "" : part.slice(idx + 1);
      try { params[k] = decodeURIComponent(v.replace(/\+/g, " ")); } catch (e) { params[k] = v; }
    });
    if (params.filter && counts[params.filter] != null) setFilter(params.filter);
    if (params.q) {
      searchInput.value = params.q;
      state.query = normalize(params.q);
      applyVisibility();
      renderList(true);
    }
    if (params.street) {
      var want = normalize(params.street);
      var rec = records.find(function (r) { return normalize(r.name) === want || normalize(r.lamas) === want; });
      if (rec) select(rec, { zoom: true });
    }
  }

  document.getElementById("export-btn").addEventListener("click", function () {
    var rows = [["שם OSM", "שם רשמי (למ״ס)", "מזהה למ״ס", "סטטוס", "ציון", "הצעת AI", "אורך (יחידות מפה)"]];
    filteredRecords().forEach(function (rec) {
      rows.push([rec.name, rec.lamas, rec.lamasId, LABELS[rec.status] || rec.status,
        rec.score == null || isNaN(rec.score) ? "" : Math.round(rec.score), rec.ai, Math.round(rec.length)]);
    });
    var csv = "\ufeff" + rows.map(function (row) {
      return row.map(function (cell) { return '"' + String(cell == null ? "" : cell).replace(/"/g, '""') + '"'; }).join(",");
    }).join("\r\n");
    var blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    var a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "streets_" + (cityName || "export") + ".csv";
    document.body.appendChild(a);
    a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 500);
    toastMsg("יוצאו " + filteredRecords().length.toLocaleString("he-IL") + " רחובות");
  });

  document.getElementById("print-btn").addEventListener("click", function () { window.print(); });
  document.getElementById("back-btn").addEventListener("click", function () { location.href = "../index.html"; });

  /* theme */
  var root = document.documentElement;
  try {
    var saved = localStorage.getItem("sni-theme");
    if (saved) root.setAttribute("data-theme", saved);
  } catch (e) {}
  document.getElementById("theme-toggle").addEventListener("click", function () {
    var next = root.getAttribute("data-theme") === "light" ? "dark" : "light";
    root.setAttribute("data-theme", next);
    try { localStorage.setItem("sni-theme", next); } catch (e) {}
    scheduleLabels(true);
  });

  /* keyboard */
  document.addEventListener("keydown", function (ev) {
    var tag = (ev.target.tagName || "").toLowerCase();
    var typing = tag === "input" || tag === "textarea" || tag === "select";
    if (ev.key === "/" && !typing) { ev.preventDefault(); searchInput.focus(); }
    if (ev.key === "Escape") {
      if (state.active) select(null);
      else if (document.activeElement === searchInput) searchInput.blur();
    }
    if (!typing && (ev.key === "+" || ev.key === "=")) {
      ev.preventDefault(); document.getElementById("zoom-in").click();
    }
    if (!typing && ev.key === "-") { ev.preventDefault(); document.getElementById("zoom-out").click(); }
    if (!typing && ev.key === "0") { ev.preventDefault(); animateTo(base); }
  });

  function toastMsg(text) {
    toast.textContent = text;
    toast.classList.add("is-visible");
    clearTimeout(toastMsg.t);
    toastMsg.t = setTimeout(function () { toast.classList.remove("is-visible"); }, 2200);
  }

  /* resize → recompute labels */
  window.addEventListener("resize", function () { scheduleLabels(true); });

  /* expose a tiny API (useful for tests/debug) */
  window.StreetMap = {
    selectByKey: function (key) { if (byKey[key]) select(byKey[key], { zoom: true }); },
    selectByName: function (name) {
      var want = normalize(name);
      var rec = records.find(function (r) { return normalize(r.name) === want; });
      if (rec) { select(rec, { zoom: true }); return true; }
      return false;
    },
    setFilter: setFilter,
    stats: function () { return { records: records.length, segments: totalSegments, counts: counts }; }
  };

  /* boot */
  renderList(true);
  renderInsights();
  readHash();
  setTimeout(function () { drawLabels(); }, 60);
})();

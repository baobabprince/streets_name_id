/* ==========================================================================
   National dashboard — city explorer
   Expects a JSON payload in <script id="dash-data"> and renders the searchable,
   filterable city grid/table. All data is embedded, so it works from file://.
   ========================================================================== */
(function () {
  "use strict";

  var dataEl = document.getElementById("dash-data");
  if (!dataEl) return;
  var DATA;
  try {
    DATA = JSON.parse(dataEl.textContent);
  } catch (e) {
    return;
  }
  var CITIES = DATA.cities || [];

  var PAGE = 12;
  var state = {
    q: "",
    tier: "all",
    bucket: null,
    sort: "streets",
    dir: "desc",
    view: "table",
    shown: PAGE,
  };

  var el = {
    grid: document.getElementById("city-grid"),
    tableWrap: document.getElementById("city-table-wrap"),
    tableBody: document.querySelector("#city-table tbody"),
    results: document.getElementById("results-count"),
    resultsLine: document.getElementById("results-line"),
    clear: document.getElementById("clear-filters"),
    more: document.getElementById("load-more"),
    moreWrap: document.getElementById("load-more-wrap"),
    empty: document.getElementById("explorer-empty"),
    search: document.getElementById("city-search"),
    sort: document.getElementById("sort-select"),
    tierChips: Array.prototype.slice.call(
      document.querySelectorAll("[data-tier]"),
    ),
    viewBtns: Array.prototype.slice.call(
      document.querySelectorAll("[data-view]"),
    ),
    gridView: document.getElementById("view-grid"),
    tableView: document.getElementById("view-table"),
    distCols: Array.prototype.slice.call(
      document.querySelectorAll(".dist .col"),
    ),
  };

  var TIER_LABEL = {
    excellent: "90% ומעלה",
    good: "70–90%",
    fair: "40–70%",
    poor: "פחות מ־40%",
    missing: "ללא מאגר",
    none: "אין נתונים",
  };
  var TIER_TONE = {
    excellent: "perfect",
    good: "ai",
    fair: "partial",
    poor: "unmatched",
    missing: "unnamed",
    none: "unnamed",
  };

  function normalize(text) {
    return String(text || "")
      .toLowerCase()
      .replace(/[\u0591-\u05C7]/g, "")
      .replace(/["'׳״`]/g, "")
      .replace(/[־–—-]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function tierOf(city) {
    if (!city.osm) return "none";
    if (!city.lamas) return "missing";
    if (city.coverage >= 90) return "excellent";
    if (city.coverage >= 70) return "good";
    if (city.coverage >= 40) return "fair";
    return "poor";
  }

  // attach tiers once
  CITIES.forEach(function (c) {
    c.tier = tierOf(c);
  });

  function matches(city) {
    if (state.tier !== "all" && city.tier !== state.tier) return false;
    if (state.bucket != null) {
      if (city.tier === "missing" || city.tier === "none") return false;
      var top = state.bucket === 90;
      if (!(
        city.coverage >= state.bucket &&
        (top || city.coverage < state.bucket + 10)
      ))
        return false;
    }
    if (!state.q) return true;
    var hay = normalize(city.name) + " " + normalize(city.file);
    return state.q.split(" ").every(function (t) {
      return hay.indexOf(t) !== -1;
    });
  }

  function sorted(list) {
    var dir = state.dir === "asc" ? 1 : -1;
    var copy = list.slice();
    copy.sort(function (a, b) {
      switch (state.sort) {
        case "name":
          return a.name.localeCompare(b.name, "he") * dir;
        case "streets":
          return (a.osm - b.osm) * dir || a.name.localeCompare(b.name, "he");
        case "unmatched":
          return (a.unmatched - b.unmatched) * dir || b.osm - a.osm;
        case "ai":
          return (
            ((a.ai_approved || 0) - (b.ai_approved || 0)) * dir || b.osm - a.osm
          );
        default:
          return (a.coverage - b.coverage) * dir || b.osm - a.osm;
      }
    });
    return copy;
  }

  function filtered() {
    return sorted(CITIES.filter(matches));
  }

  function fmt(n) {
    return Number(n || 0).toLocaleString("he-IL");
  }

  function coverageColor(pct) {
    if (pct >= 90) return "var(--perfect)";
    if (pct >= 70) return "var(--ai)";
    if (pct >= 40) return "var(--partial)";
    return "var(--unmatched)";
  }

  function segments(city) {
    var c = city.counts || {};
    var total =
      c.perfect + c.partial + c.ai + c["geo-review"] + c.unmatched + c.unnamed;
    if (!total) return "";
    var order = [
      ["perfect", "var(--perfect)"],
      ["partial", "var(--partial)"],
      ["ai", "var(--ai)"],
      ["geo-review", "var(--geo)"],
      ["unmatched", "var(--unmatched)"],
      ["unnamed", "var(--unnamed)"],
    ];
    return order
      .map(function (pair) {
        var n = c[pair[0]] || 0;
        if (!n) return "";
        return (
          '<i style="width:' +
          ((n / total) * 100).toFixed(2) +
          "%;background:" +
          pair[1] +
          '"></i>'
        );
      })
      .join("");
  }

  function card(city, index) {
    var a = document.createElement("a");
    a.className = "city-card";
    a.href = city.href;
    a.style.animationDelay = Math.min(index, 18) * 18 + "ms";
    a.setAttribute(
      "aria-label",
      city.name +
        " — " +
        city.coverage.toFixed(1) +
        "% כיסוי קישורים בתהליך. פתיחת הנתונים",
    );
    a.innerHTML =
      '<div class="cc-head">' +
      '<h3 class="cc-name"><span class="cc-title"></span><span class="cc-sub-title"></span></h3>' +
      '<span class="badge tier-badge"></span>' +
      "</div>" +
      '<div class="cc-stats">' +
      '<div class="cc-pct"><b></b><span>%</span></div>' +
      '<div class="cc-sub"><b class="cc-matched"></b> עברו סינון<br><b class="cc-unmatched"></b> ללא קישור</div>' +
      "</div>" +
      '<div class="seg-bar" role="img"></div>' +
      '<div class="cc-foot">' +
      '<span class="cc-total"></span>' +
      '<span class="open-link">לנתונים<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M15 18l-6-6 6-6"/></svg></span>' +
      "</div>";
    a.querySelector(".cc-title").textContent = city.name;
    a.querySelector(".cc-sub-title").textContent = city.osm
      ? fmt(city.osm) + " רחובות ב-OSM"
      : "אין נתוני רחובות";
    var badge = a.querySelector(".tier-badge");
    badge.dataset.tone = TIER_TONE[city.tier];
    badge.textContent =
      city.tier === "none"
        ? TIER_LABEL.none
        : city.coverage.toFixed(0) + "% · " + TIER_LABEL[city.tier];
    var missing = city.tier === "missing";
    a.querySelector(".cc-pct b").textContent = missing
      ? "—"
      : city.coverage.toFixed(1);
    a.querySelector(".cc-pct b").style.color = missing
      ? "var(--muted)"
      : coverageColor(city.coverage);
    a.querySelectorAll(".cc-pct span").forEach(function (s) {
      s.style.color = "var(--muted)";
    });
    a.querySelector(".cc-matched").textContent = missing
      ? "0"
      : fmt(city.matched);
    a.querySelector(".cc-unmatched").textContent = missing
      ? "—"
      : fmt(city.unmatched);
    if (missing) {
      a.querySelector(".cc-sub").innerHTML =
        '<span style="color:var(--muted-2)">אין מאגר רחובות רשמי<br>להשוואה</span>';
    }
    var seg = a.querySelector(".seg-bar");
    seg.innerHTML = segments(city);
    seg.setAttribute("aria-label", "פילוח סטטוסים עבור " + city.name);
    a.querySelector(".cc-total").textContent =
      fmt(city.osm) + " רחובות · " + fmt(city.lamas) + " רחובות רשמיים";
    var date = dateLine(city, "cc-date");
    if (date) a.querySelector(".cc-name").insertAdjacentHTML("beforeend", date);
    return a;
  }

  function renderGrid(list) {
    el.grid.innerHTML = "";
    var frag = document.createDocumentFragment();
    list.slice(0, state.shown).forEach(function (city, i) {
      frag.appendChild(card(city, i));
    });
    el.grid.appendChild(frag);
  }

  function renderTable(list) {
    var rows = list
      .slice(0, state.shown)
      .map(function (city) {
        return (
          "<tr>" +
          '<td class="t-name"><a href="' +
          city.href +
          '">' +
          escapeHtml(city.name) +
          "</a>" +
          dateLine(city, "t-date") +
          "</td>" +
          '<td><div class="t-coverage">' +
          (city.tier === "missing"
            ? '<b style="color:var(--muted)">—</b><span style="color:var(--muted-2);font-size:.72rem">ללא מאגר</span>'
            : '<span class="mini-bar"><i style="width:' +
              city.coverage.toFixed(1) +
              "%;background:" +
              coverageColor(city.coverage) +
              '"></i></span><b>' +
              city.coverage.toFixed(1) +
              "%</b>") +
          "</div></td>" +
          '<td class="num">' +
          fmt(city.osm) +
          "</td>" +
          '<td class="num">' +
          fmt(city.matched) +
          "</td>" +
          '<td class="num">' +
          fmt(city.unmatched) +
          "</td>" +
          '<td class="num" title="' +
          (city.ai_unknown
            ? "בחלק מסיכומי AI היסטוריים אין פירוט סטטוס"
            : "קישורי AI שעברו סינון, כלולים בקישורים") +
          '">' +
          (city.ai_unknown && !city.ai_approved
            ? "לא ידוע"
            : fmt(city.ai_approved)) +
          "</td>" +
          '<td><span class="badge" data-tone="' +
          TIER_TONE[city.tier] +
          '">' +
          TIER_LABEL[city.tier] +
          "</span></td>" +
          '<td><a class="t-link" href="' +
          city.href +
          '">פתיחה</a></td>' +
          "</tr>"
        );
      })
      .join("");
    el.tableBody.innerHTML = rows;
  }

  // Only pages generated with recorded provenance carry dates; older pages
  // show nothing rather than an estimated time.
  function dateLine(city, className) {
    var dates = city.dates;
    if (!dates || !dates.generated) return "";
    var title = [];
    if (dates.osm) title.push("נתוני OSM: " + dates.osm);
    if (dates.lamas) title.push("נתוני למ״ס: " + dates.lamas);
    return (
      '<small class="' +
      className +
      '"' +
      (title.length ? ' title="' + escapeHtml(title.join(" · ")) + '"' : "") +
      ">נוצר " +
      escapeHtml(dates.generated) +
      "</small>"
    );
  }

  function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, function (ch) {
      return {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      }[ch];
    });
  }

  function render(reset) {
    if (reset) state.shown = PAGE;
    var list = filtered();
    el.results.textContent = fmt(list.length);
    if (el.resultsLine) el.resultsLine.hidden = false;
    var shownCount = document.getElementById("shown-count");
    if (shownCount)
      shownCount.textContent = fmt(Math.min(state.shown, list.length));
    if (el.clear)
      el.clear.disabled = !(
        state.q ||
        state.tier !== "all" ||
        state.bucket != null
      );
    el.empty.hidden = list.length !== 0;

    var isGrid = state.view === "grid";
    el.grid.hidden = !isGrid || list.length === 0;
    el.tableWrap.hidden = isGrid || list.length === 0;

    if (isGrid) renderGrid(list);
    else renderTable(list);
    el.moreWrap.hidden = list.length <= state.shown;
    if (el.more)
      el.more.textContent =
        "טעינת " +
        fmt(Math.min(PAGE, list.length - state.shown)) +
        " יישובים נוספים";

    // sync controls
    el.viewBtns.forEach(function (btn) {
      btn.setAttribute("aria-pressed", String(btn.dataset.view === state.view));
    });
    el.tierChips.forEach(function (chip) {
      chip.setAttribute(
        "aria-pressed",
        String(chip.dataset.tier === state.tier),
      );
    });
    el.distCols.forEach(function (col) {
      col.dataset.active = String(
        state.bucket != null && Number(col.dataset.bucket) === state.bucket,
      );
    });
    if (el.search.value !== state.qRaw) {
      /* keep user's typed text */
    }
  }

  /* listeners */
  var timer = null;
  el.search.addEventListener("input", function () {
    clearTimeout(timer);
    timer = setTimeout(function () {
      state.q = normalize(el.search.value);
      state.qRaw = el.search.value;
      render(true);
    }, 120);
  });
  el.search.addEventListener("keydown", function (ev) {
    if (ev.key === "Escape") {
      el.search.value = "";
      state.q = "";
      state.qRaw = "";
      render(true);
    }
  });

  el.tierChips.forEach(function (chip) {
    chip.addEventListener("click", function () {
      state.tier = chip.dataset.tier;
      state.bucket = null;
      render(true);
    });
  });

  el.sort.addEventListener("change", function () {
    var parts = el.sort.value.split(":");
    state.sort = parts[0];
    state.dir = parts[1] || "desc";
    render(true);
  });

  el.viewBtns.forEach(function (btn) {
    btn.addEventListener("click", function () {
      state.view = btn.dataset.view;
      try {
        localStorage.setItem("sni-index-v2-view", state.view);
      } catch (e) {}
      render(false);
    });
  });

  el.more.addEventListener("click", function () {
    state.shown += PAGE;
    render(false);
  });

  el.clear.addEventListener("click", function () {
    state.q = "";
    state.qRaw = "";
    state.tier = "all";
    state.bucket = null;
    el.search.value = "";
    render(true);
  });

  el.distCols.forEach(function (col) {
    col.addEventListener("click", function () {
      var bucket = Number(col.dataset.bucket);
      state.bucket = state.bucket === bucket ? null : bucket;
      state.tier = "all";
      render(true);
      var citiesEl = document.getElementById("cities");
      if (citiesEl && citiesEl.scrollIntoView)
        citiesEl.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });

  document.querySelectorAll("[data-sort-preset]").forEach(function (btn) {
    btn.addEventListener("click", function () {
      state.sort = btn.dataset.sortPreset;
      state.dir = btn.dataset.dir || "desc";
      if (el.sort) el.sort.value = state.sort + ":" + state.dir;
      render(true);
      var citiesEl = document.getElementById("cities");
      if (citiesEl && citiesEl.scrollIntoView)
        citiesEl.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });

  /* theme + shortcuts */
  var root = document.documentElement;
  try {
    var saved = localStorage.getItem("sni-theme");
    if (saved) root.setAttribute("data-theme", saved);
    var savedView = localStorage.getItem("sni-index-v2-view");
    if (savedView === "grid" || savedView === "table") state.view = savedView;
  } catch (e) {}
  var themeBtn = document.getElementById("theme-toggle");
  if (themeBtn)
    themeBtn.addEventListener("click", function () {
      var next = root.getAttribute("data-theme") === "light" ? "dark" : "light";
      root.setAttribute("data-theme", next);
      try {
        localStorage.setItem("sni-theme", next);
      } catch (e) {}
    });

  document.addEventListener("keydown", function (ev) {
    var tag = (ev.target.tagName || "").toLowerCase();
    if (
      ev.key === "/" &&
      tag !== "input" &&
      tag !== "select" &&
      tag !== "textarea"
    ) {
      ev.preventDefault();
      el.search.focus();
    }
  });

  /* hero ring animation */
  var ringValue = document.querySelector(".ring .value");
  if (ringValue) {
    var target = Number(ringValue.dataset.offset || 0);
    ringValue.style.strokeDashoffset =
      ringValue.getAttribute("stroke-dasharray");
    requestAnimationFrame(function () {
      setTimeout(function () {
        ringValue.style.strokeDashoffset = target;
      }, 80);
    });
  }

  render(true);
})();

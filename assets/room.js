/* Settlement comparison room: OSM match layer, GovMap reference and the full
   findings table. All data is embedded in the page; no matching or write-back
   occurs here. */
(function () {
  "use strict";
  const $ = (id) => document.getElementById(id);
  window.addEventListener("error", (event) => {
    if (!event.error || !$("app-status")) return;
    $("app-status").dataset.state = "error";
    $("app-status").textContent = "אירעה שגיאה בכלי הטבלה — הפעולה לא הושלמה.";
    $("fatal").hidden = false;
    $("fatal").textContent =
      "פעולה בטבלה נכשלה. רענון העמוד אמור לטעון מחדש את כל הנתונים.";
  });
  const number = (n) => new Intl.NumberFormat("he-IL").format(n);
  const styles = {
    perfect: ["מאושר · ציון 100", "#218c77"],
    partial: ["מאושר · חלקי", "#d39a29"],
    "approved-ai": ["AI מאושר", "#407fd0"],
    proposal: ["הצעת AI", "#8f71bf"],
    "geo-review": ["בדיקה גאוגרפית", "#9865b1"],
    unmatched: ["ללא התאמה מאושרת", "#d06451"],
    unnamed: ["ללא שם", "#87949a"],
    "official-missing": ["ללא התאמה ב־OSM", "#a78351"],
  };
  const state = {
    view: "all",
    query: "",
    page: 0,
    selected: null,
    colors: true,
    scoreMode: "all",
    minScore: null,
    maxScore: null,
    scoreInvalid: false,
    sort: "name-asc",
    pageSize: "all",
  };
  const Scores = window.ComparisonScores;
  const sortLabels = {
    default: "ממצאים תחילה",
    "name-asc": "שם OSM: א–ת",
    "name-desc": "שם OSM: ת–א",
    "official-asc": "שם למ״ס: א–ת",
    "official-desc": "שם למ״ס: ת–א",
    "score-asc": "ציון: מנמוך לגבוה",
    "score-desc": "ציון: מגבוה לנמוך",
    "status-asc": "סטטוס: א–ת",
    "status-desc": "סטטוס: ת–א",
  };
  function pageSize() {
    return state.pageSize === "all"
      ? Math.max(filtered.length, 1)
      : state.pageSize;
  }
  function compareEntries(a, b) {
    const [key, direction] = state.sort.split("-");
    let primary = 0;
    if (key === "score")
      primary = Scores.compareScores(a.score, b.score, direction);
    else {
      const field = (entry) =>
        key === "status"
          ? styles[entry.status][0]
          : key === "official"
            ? entry.official
            : entry.osm || entry.official;
      const x = field(a),
        y = field(b);
      primary = !x
        ? y
          ? 1
          : 0
        : !y
          ? -1
          : x.localeCompare(y, "he") * (direction === "desc" ? -1 : 1);
    }
    return (
      primary ||
      (a.osm || a.official).localeCompare(b.osm || b.official, "he") ||
      a.key.localeCompare(b.key)
    );
  }
  function setSort(order) {
    state.sort = order;
    state.page = 0;
    clearSelection();
    render();
    $("table-scroll").scrollTop = 0;
  }
  let data,
    entries,
    officialEntries,
    layers,
    map,
    cityBounds,
    filtered = [];
  const tabViews = ["all", "unmatched", "official-missing", "ai"];
  let views;

  function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }
  function isApproved(r) {
    return (
      Boolean(r[3]) && !["geo-review", "unmatched", "unnamed"].includes(r[0])
    );
  }
  function recordStatus(r) {
    return r[0] === "ai" ? (isApproved(r) ? "approved-ai" : "proposal") : r[0];
  }
  function reason(entry, short = false) {
    if (entry.status === "approved-ai")
      return short
        ? "השם הרשמי נבחר בעזרת AI."
        : `ה־AI קישר את הרחוב לשם למ״ס ״${entry.official}״ (קוד ${entry.code}). הקישור נכלל בהתאמות המאושרות ובחישוב הכיסוי.`;
    const reasons = {
      perfect: [
        "התאמה מאושרת עם ציון 100.",
        "קיימת התאמה מאושרת עם ציון התאמה 100. הציון עשוי להתבסס על עיבוד ונרמול שמות, ולא בהכרח על כתיב זהה או אימות בשטח.",
      ],
      partial: [
        "התאמה מאושרת עם ציון חלקי.",
        "נמצאה התאמה מאושרת עם ציון נמוך מ־100. הציון מתאר את התאמת השמות באלגוריתם, ולא את הסיכוי שהרחוב נכון.",
      ],
      proposal: [
        "הצעת AI שטרם אושרה.",
        "קיימת הצעת AI ללא מזהה התאמה סופי. זו הצעה בלבד והיא אינה נכללת בכיסוי המאושר.",
      ],
      "geo-review": [
        "ההתאמה דורשת בדיקה גאוגרפית.",
        "הנתונים מסמנים שנדרשת בדיקה גאוגרפית לפני אישור. אין להסיק מכך שהרחוב חסר או שמיקומו שגוי.",
      ],
      unmatched: [
        "לא נמצאה התאמה מאושרת בריצה הקיימת.",
        "לא נמצאה התאמה מאושרת בריצה הקיימת. השם הרשמי המוצג, אם ישנו, הוא מועמד בלבד. הפער עשוי לנבוע מהבדל בשם, משיוך ליישוב או ממגבלת ההתאמה — אין כאן קביעה שהרחוב חסר.",
      ],
      unnamed: [
        "למקטע אין שם בנתוני המקור.",
        "למקטע אין שם בנתוני OSM שנבדקו. הוא אינו מאפשר השוואת שמות רגילה.",
      ],
      "official-missing": [
        "שם רשמי ללא רחוב OSM מזוהה.",
        "השם מופיע ברשימת למ״ס, אך לא קושר לרחוב OSM בריצה הקיימת. אין בנתונים האלה מיקום מזוהה להצגה בצד OSM; אפשר לחפש את השם ב־GovMap. אין להסיק מכך שהרחוב אינו קיים ב־OSM.",
      ],
    };
    return (reasons[entry.status] || reasons.unmatched)[short ? 0 : 1];
  }
  function normalized(text) {
    return String(text)
      .normalize("NFKC")
      .toLocaleLowerCase("he")
      .replace(/["'׳״]/g, "")
      .trim();
  }
  function matches(entry) {
    return (
      !state.query ||
      normalized(
        [entry.osm, entry.official, entry.code, entry.raw && entry.raw[5]].join(
          " ",
        ),
      ).includes(state.query)
    );
  }
  function scoreDisplay(entry) {
    if (entry.score === null) return el("span", "score-missing", "לא זמין");
    const box = el("span", "score-display");
    const value = el("span", "score-value", number(entry.score));
    const meter = el("span", "score-meter");
    meter.setAttribute("aria-hidden", "true");
    const bar = el("i");
    bar.style.width = `${entry.score}%`;
    meter.append(bar);
    box.append(value, meter);
    const grouped = entry.ids.length > 1;
    box.title = grouped
      ? `הציון הנמוך הזמין בין שמות OSM המקושרים: ${entry.score}; טווח ${entry.score}–${entry.scoreMax}. לא אחוז ודאות.`
      : `ציון האלגוריתם: ${entry.score} מתוך 100, לא אחוז ודאות.`;
    if (grouped) box.append(el("span", "score-group-note", "הנמוך בקוד"));
    return box;
  }
  function resetScoreFilter() {
    state.scoreMode = "all";
    state.minScore = null;
    state.maxScore = null;
    state.scoreInvalid = false;
    $("score-mode").value = "all";
    [$("score-min"), $("score-max")].forEach((input) => {
      input.value = "";
      input.disabled = false;
      input.setAttribute("aria-invalid", "false");
    });
    $("score-error").hidden = true;
  }
  function updateScoreFilter(fromRange = true) {
    // Entering either bound explicitly opts into scored-only filtering.
    if (fromRange) $("score-mode").value = "scored";
    state.scoreMode = $("score-mode").value;
    const min = $("score-min"),
      max = $("score-max");
    min.disabled = max.disabled = state.scoreMode === "missing";
    state.minScore = min.value === "" ? null : Number(min.value);
    state.maxScore = max.value === "" ? null : Number(max.value);
    const inverted =
      state.minScore !== null &&
      state.maxScore !== null &&
      state.minScore > state.maxScore;
    state.scoreInvalid = !min.validity.valid || !max.validity.valid || inverted;
    min.setAttribute("aria-invalid", String(!min.validity.valid || inverted));
    max.setAttribute("aria-invalid", String(!max.validity.valid || inverted));
    $("score-error").hidden = !state.scoreInvalid;
    state.page = 0;
    clearSelection();
    render();
  }
  function badge(entry) {
    const [label, color] = styles[entry.status];
    const node = el("span", "status-tag");
    node.style.setProperty("--status", color);
    node.append(el("span", "dot"), document.createTextNode(label));
    return node;
  }
  function govUrl(name) {
    const params = new URLSearchParams();
    const centre = data.meta.govmap;
    if (centre) params.set("c", `${centre.x},${centre.y}`);
    params.set("z", name || !centre ? "12" : "7");
    if (name || !centre)
      params.set("q", name ? `${name}, ${data.meta.place}` : data.meta.place);
    return `https://www.govmap.gov.il/?${params}`;
  }
  function navigateGov(name = "") {
    const url = govUrl(name);
    $("gov-map").src = url;
    $("gov-external").href = url;
    $("gov-fallback").href = url;
    $("gov-context").textContent = name
      ? `חיפוש: ${name} · התוצאה תלויה ב־GovMap`
      : "תצוגת היישוב · ללא סנכרון עם OSM";
  }
  function aiDescription() {
    const ai = entries.filter((e) =>
      ["approved-ai", "proposal"].includes(e.status),
    );
    const approved = ai.filter((e) => e.status === "approved-ai").length;
    const base =
      "התאמות AI: מאושרות והצעות מסומנות בנפרד לפי קיום מזהה סופי.";
    if (!ai.length) return `${base} ביישוב זה אין התאמות AI.`;
    if (approved === ai.length)
      return `${base} ביישוב זה כל ${number(ai.length)} התאמות ה־AI מאושרות.`;
    return `${base} ביישוב זה ${number(approved)} מתוך ${number(ai.length)} התאמות ה־AI מאושרות.`;
  }
  function setupModels() {
    entries = data.records.map((r, i) => ({
      key: `osm-${i}`,
      osm: r[1] || "ללא שם",
      official: r[2],
      code: r[3],
      status: recordStatus(r),
      ...Scores.summarizeScores([r[4]]),
      raw: r,
      ids: [i],
    }));
    const byOfficial = new Map();
    entries
      .filter((e) => isApproved(e.raw))
      .forEach((e) => {
        if (byOfficial.has(e.code)) {
          const current = byOfficial.get(e.code);
          current.ids.push(...e.ids);
          current.osm += ` / ${e.osm}`;
        } else
          byOfficial.set(e.code, {
            ...e,
            key: `official-${e.code}`,
            ids: [...e.ids],
          });
      });
    // A code can link to several OSM names. Use its lowest available score
    // consistently for display, filtering and sorting, not an arbitrary first row.
    byOfficial.forEach((entry) => {
      Object.assign(
        entry,
        Scores.summarizeScores(entry.ids.map((id) => entries[id].raw[4])),
      );
      const representative = entry.ids
        .map((id) => entries[id])
        .find((item) => item.score === entry.score);
      // Explanation/status must describe the same match as the displayed score.
      entry.raw = representative.raw;
      entry.status = representative.status;
    });
    const missing = data.missingOfficial.map((name, i) => ({
      key: `missing-${i}`,
      osm: "",
      official: name,
      code: "",
      status: "official-missing",
      ...Scores.summarizeScores([]),
      ids: [],
    }));
    officialEntries = [...byOfficial.values(), ...missing];
    const approved = entries.filter((e) => isApproved(e.raw));
    views = {
      all: {
        label: "כל שמות OSM",
        items: entries,
        description:
          "כל שמות OSM שנבדקו — עם ובלי התאמה. אפשר למיין לפי שם, ציון או סטטוס בלחיצה על כותרת העמודה.",
      },
      unmatched: {
        label: "OSM ללא התאמה",
        items: entries.filter((e) => e.status === "unmatched"),
        description:
          "שמות OSM ללא התאמה מאושרת בלמ״ס. מועמד להתאמה אינו שם רשמי מאושר.",
      },
      "official-missing": {
        label: "למ״ס ללא התאמה",
        items: missing,
        description:
          "שמות רשמיים שלא קושרו ל־OSM. מיקומם אינו ידוע בנתונים שנבדקו; ניתן לחפש אותם ב־GovMap.",
      },
      ai: {
        label: "התאמות AI",
        items: entries.filter((e) =>
          ["approved-ai", "proposal"].includes(e.status),
        ),
        description: aiDescription(),
      },
      "geo-review": {
        label: "בדיקה גאוגרפית",
        items: entries.filter((e) => e.status === "geo-review"),
        description:
          "התאמות המסומנות לבדיקה גאוגרפית. אינן נכללות בכיסוי המאושר.",
      },
      approved: {
        label: "OSM עם התאמה מאושרת",
        items: approved,
        description:
          "שמות OSM עם התאמה מאושרת, כולל AI מאושר. כמה שמות עשויים להיות מקושרים לאותו קוד למ״ס.",
      },
      "official-all": {
        label: "כל רחובות למ״ס",
        items: officialEntries,
        description:
          "רשימה לפי קוד רשמי עבור ההתאמות המאושרות, ולפי השמות ברשימת הפערים עבור היתר. כל קוד מאושר מוצג פעם אחת.",
      },
      "official-approved": {
        label: "למ״ס עם התאמה מאושרת",
        items: [...byOfficial.values()],
        description:
          "קודי למ״ס ייחודיים עם התאמה מאושרת ל־OSM. כל קוד מוצג פעם אחת, גם אם קושר למספר שמות OSM.",
      },
    };
    Object.keys(styles).forEach((status) => {
      if (!(status in views) && status !== "official-missing")
        views[status] = {
          label: styles[status][0],
          items: entries.filter((e) => e.status === status),
          description:
            "שמות OSM המסוננים לפי סטטוס ההתאמה שנבחר בהרכב ההתאמות.",
        };
    });
    const rank = {
      unmatched: 0,
      "geo-review": 1,
      proposal: 2,
      "approved-ai": 3,
      partial: 4,
      perfect: 5,
      unnamed: 6,
    };
    Object.values(views).forEach(
      (v) =>
        (v.items = [...v.items].sort(
          (a, b) =>
            (rank[a.status] ?? 0) - (rank[b.status] ?? 0) ||
            (a.osm || a.official).localeCompare(b.osm || b.official, "he"),
        )),
    );
  }
  function setupOverview() {
    const d = data.meta.diagnostics || {};
    const approved = views.approved.items.length;
    const official = data.approvedOfficialCount;
    const totalOsm = d.total_osm_streets ?? entries.length;
    const totalOfficial = d.total_lamas_streets ?? officialEntries.length;
    const pct = (part, whole) =>
      whole ? ((100 * part) / whole).toFixed(1) : null;
    $("snapshot-osm").textContent = number(views.unmatched.items.length);
    $("snapshot-official").textContent = number(
      views["official-missing"].items.length,
    );
    $("osm-total").textContent = number(totalOsm);
    $("official-total").textContent = number(totalOfficial);
    const osmPct = pct(approved, totalOsm);
    const officialPct = pct(official, totalOfficial);
    $("osm-coverage").textContent = osmPct === null ? "—" : `${osmPct}%`;
    $("official-coverage").textContent =
      officialPct === null ? "—" : `${officialPct}%`;
    $("osm-bar").style.width = `${osmPct || 0}%`;
    $("official-bar").style.width = `${officialPct || 0}%`;
    $("osm-ratio").textContent =
      `${number(approved)} מתוך ${number(totalOsm)} שמות · כולל AI מאושר`;
    $("official-ratio").textContent = totalOfficial
      ? `${number(official)} מתוך ${number(totalOfficial)} קודים רשמיים`
      : "אין רחובות ברשימת למ״ס להשוואה";
    if (data.missingOfficialListed < data.missingOfficialTotal)
      $("official-missing-note").textContent =
        `ברשימה מוצגים ${number(data.missingOfficialListed)} מתוך ${number(data.missingOfficialTotal)} שמות למ״ס ללא התאמה; בעמוד הקודם נשמרו רק אלה. הרצה מחדש של היישוב תציג את הרשימה המלאה.`;
    const counts = entries.reduce(
      (a, e) => ((a[e.status] = (a[e.status] || 0) + 1), a),
      {},
    );
    Object.entries(styles).forEach(([status, [label, color]]) => {
      const count = counts[status] || 0;
      if (!count) return;
      const button = el("button");
      button.style.setProperty("--status", color);
      button.title = `${label}: ${count}`;
      button.setAttribute("aria-label", `סינון ${label}: ${count}`);
      button.style.flex = count;
      button.onclick = () => applyView(status);
      $("status-bar").append(button);
      const legend = el("button");
      legend.style.setProperty("--status", color);
      legend.append(
        el("span", "dot"),
        document.createTextNode(label),
        el("b", "", number(count)),
      );
      legend.onclick = () => applyView(status);
      $("legend").append(legend);
    });
    tabViews.forEach((view) => {
      const button = el("button", "", views[view].label);
      button.dataset.view = view;
      button.setAttribute("aria-pressed", "false");
      button.append(el("span", "", number(views[view].items.length)));
      button.onclick = () => applyView(view);
      $("tabs").append(button);
    });
  }
  function setupMap() {
    map = L.map("osm-map", {
      zoomControl: false,
      scrollWheelZoom: true,
      attributionControl: true,
    });
    L.control.zoom({ position: "topleft" }).addTo(map);
    L.control.scale({ position: "bottomleft", imperial: false }).addTo(map);
    let tileLoaded = false;
    const tiles = L.tileLayer(
      "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
      {
        maxZoom: 19,
        attribution:
          '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap contributors</a>',
      },
    );
    tiles.on("tileload", () => {
      tileLoaded = true;
      $("tile-warning").hidden = true;
    });
    tiles.on("tileerror", () => {
      if (!tileLoaded) $("tile-warning").hidden = false;
    });
    tiles.addTo(map);
    setTimeout(() => {
      if (!tileLoaded) $("tile-warning").hidden = false;
    }, 7000);
    layers = entries.map((entry, i) => {
      const layer = L.polyline(data.geometry[i] || [], {
        color: styles[entry.status][1],
        weight: 3,
        opacity: 0.9,
        lineCap: "round",
      }).addTo(map);
      layer.bindTooltip(document.createTextNode(entry.osm), {
        direction: "top",
      });
      layer.on("click", () =>
        select(filtered.find((item) => item.ids.includes(i)) || entry),
      );
      return layer;
    });
    cityBounds = L.featureGroup(layers).getBounds();
    if (cityBounds.isValid()) map.fitBounds(cityBounds, { padding: [15, 15] });
    else {
      cityBounds = null;
      map.setView([31.5, 34.9], 8);
      $("tile-warning").hidden = false;
      $("tile-warning").textContent =
        "לעמוד זה אין גאומטריית רחובות להצגה. הטבלה והחיפוש פעילים.";
    }
  }
  function paintMap() {
    // A map-library or tile failure must never disable the data table.
    if (!map || !layers) return;
    const visibleIds = new Set(filtered.flatMap((e) => e.ids));
    const activeIds = new Set(state.selected ? state.selected.ids : []);
    const noGeometry = filtered.length > 0 && visibleIds.size === 0;
    layers.forEach((layer, i) => {
      const visible = visibleIds.has(i);
      const selected = activeIds.has(i);
      layer.setStyle({
        color: state.colors ? styles[entries[i].status][1] : "#687e70",
        weight: selected ? 7 : visible ? 3 : 1.5,
        opacity: selected ? 1 : visible ? (activeIds.size ? 0.3 : 0.9) : 0.08,
        interactive: visible,
      });
      // Leaflet's path interactivity is DOM based once created.
      if (layer.getElement())
        layer.getElement().style.pointerEvents = visible ? "auto" : "none";
      if (selected) layer.bringToFront();
    });
    $("no-geometry").hidden = !noGeometry;
  }
  function applyView(view) {
    if (!views[view]) return;
    if (view === "all") {
      // "All names" must actually restore all names, not keep hidden filters.
      state.query = "";
      $("search").value = "";
      resetScoreFilter();
      state.sort = "name-asc";
      state.pageSize = "all";
    }
    state.view = view;
    state.page = 0;
    clearSelection();
    render();
  }
  function render() {
    const view = views[state.view];
    filtered = state.scoreInvalid
      ? []
      : view.items.filter(
          (entry) =>
            matches(entry) &&
            Scores.matchesScore(
              entry.score,
              state.scoreMode,
              state.minScore,
              state.maxScore,
            ),
        );
    if (state.sort !== "default") filtered.sort(compareEntries);
    // Keep a chosen street visible only if it still belongs to the filtered set.
    if (
      state.selected &&
      !filtered.some((entry) => entry.key === state.selected.key)
    )
      clearSelection();
    const officialView =
      state.view.startsWith("official") && state.view !== "official-missing";
    $("score-filter-note").textContent = officialView
      ? "בקוד עם כמה שמות OSM מוצג הציון הנמוך הזמין; הסינון והמיון פועלים לפיו. 0–100, לא אחוז ודאות."
      : "0–100 · ציון האלגוריתם להתאמה או למועמד, לא אחוז ודאות. הזנת טווח מציגה רק רשומות עם ציון.";
    $("sort-order").value = state.sort;
    ["name", "official", "score", "status"].forEach((key) => {
      const direction =
        state.sort === `${key}-asc`
          ? "ascending"
          : state.sort === `${key}-desc`
            ? "descending"
            : "none";
      $(key + "-heading").setAttribute("aria-sort", direction);
      $(key + "-sort-icon").textContent =
        direction === "ascending"
          ? "↑"
          : direction === "descending"
            ? "↓"
            : "↕";
      $("sort-" + key).setAttribute(
        "aria-label",
        `מיון לפי ${key === "score" ? "ציון" : key === "name" ? "שם OSM" : key === "official" ? "שם למ״ס" : "סטטוס"}: ${direction === "ascending" ? "סדר יורד" : "סדר עולה"}`,
      );
    });
    $("table-view-title").textContent =
      `${view.label} · ${number(filtered.length)}`;
    $("sort-feedback").textContent =
      `מיון: ${sortLabels[state.sort]}${state.sort.startsWith("score-") ? " · ללא ציון בסוף" : ""}`;
    const filters = [];
    if (state.query) filters.push(`חיפוש: ״${$("search").value}״`);
    if (state.scoreMode === "scored")
      filters.push(`ציון ${state.minScore ?? 0}–${state.maxScore ?? 100}`);
    if (state.scoreMode === "missing") filters.push("ללא ציון בלבד");
    if (state.scoreInvalid) filters.push("טווח ציונים לא תקין");
    $("active-filters").hidden = !filters.length;
    $("filter-description").textContent = `סינון פעיל: ${filters.join(" · ")}`;
    $("page-size").value = String(state.pageSize);
    document.querySelectorAll("[data-view]").forEach((button) => {
      button.setAttribute(
        "aria-pressed",
        String(button.dataset.view === state.view),
      );
    });
    $("current-view").textContent = view.label;
    $("map-count").textContent = number(filtered.length);
    $("table-description").textContent = view.description;
    $("results").textContent =
      `${number(filtered.length)} ${state.view.startsWith("official") ? "שמות / קודים רשמיים" : "שמות OSM"}`;
    $("name-heading-label").textContent =
      state.view === "official-missing" ? "שם ב־OSM (לא זוהה)" : "שם ב־OSM";
    $("rows").replaceChildren();
    filtered
      .slice(state.page * pageSize(), (state.page + 1) * pageSize())
      .forEach((entry) => {
        const tr = el(
          "tr",
          state.selected && entry.key === state.selected.key ? "selected" : "",
        );
        tr.append(
          el("td", "", entry.osm || "—"),
          el("td", "", entry.official || "—"),
        );
        const statusCell = el("td");
        statusCell.append(badge(entry));
        const actionCell = el("td");
        const button = el("button", "row-open", "לפרטים ←");
        button.setAttribute(
          "aria-label",
          `פרטים על ${entry.osm || entry.official}`,
        );
        actionCell.append(button);
        const scoreCell = el("td", "score-cell");
        scoreCell.append(scoreDisplay(entry));
        tr.append(
          scoreCell,
          statusCell,
          el("td", "", reason(entry, true)),
          actionCell,
        );
        tr.onclick = () => select(entry);
        $("rows").append(tr);
      });
    $("empty").hidden = filtered.length > 0;
    $("page-info").textContent = !filtered.length
      ? "0 ממצאים"
      : state.pageSize === "all"
        ? `כל ${number(filtered.length)} השורות מוצגות בטבלה`
        : `${number(state.page * pageSize() + 1)}–${number(Math.min((state.page + 1) * pageSize(), filtered.length))} מתוך ${number(filtered.length)}`;
    const first = filtered[0];
    $("first-row-summary").textContent = first
      ? `ראשון בטבלה: ${first.osm || first.official} · ציון: ${first.score === null ? "לא זמין" : number(first.score)}`
      : "אין שורות להצגה במסננים הנוכחיים";
    $("prev").disabled = state.page === 0;
    $("next").disabled = (state.page + 1) * pageSize() >= filtered.length;
    $("export").disabled = !filtered.length;
    paintMap();
  }
  function clearSelection() {
    state.selected = null;
    $("selection").hidden = true;
  }
  function focusSelected(scroll = false) {
    if (!map || !layers || !state.selected || !state.selected.ids.length)
      return;
    const bounds = L.featureGroup(
      state.selected.ids.map((i) => layers[i]),
    ).getBounds();
    if (!bounds.isValid()) return;
    map.fitBounds(bounds, { padding: [65, 65], maxZoom: 17 });
    if (scroll)
      $("osm-map").scrollIntoView({ behavior: "smooth", block: "center" });
  }
  function renderTechnical(entry) {
    const r = entry.raw;
    const hasValue = (value) =>
      value !== null && value !== undefined && value !== "";
    const method = ["approved-ai", "proposal"].includes(entry.status)
      ? "AI"
      : ["perfect", "partial"].includes(entry.status)
        ? "השוואת שמות"
        : "לא נקבעה התאמה מאושרת";
    const segments = entry.ids.reduce(
      (total, id) => total + (data.geometry[id] || []).length,
      0,
    );
    const fields = [
      ["קוד רחוב למ״ס", entry.code || "אין התאמה מאושרת"],
      ["שיטת התאמה", method],
      [
        "ציון התאמת שמות (0–100)",
        entry.score === null ? "לא זמין" : number(entry.score),
      ],
      ["סטטוס", styles[entry.status][0]],
      [
        "ביטחון שדיווח AI (0–1)",
        r && hasValue(r[6]) ? String(r[6]) : "לא זמין",
      ],
      ["מקטעי OSM בתצוגה", number(segments)],
    ];
    if (r && hasValue(r[5]) && String(r[5]) !== String(entry.code))
      fields.push(["קוד הרחוב שהציע AI", r[5]]);
    if (entry.ids.length > 1)
      fields.push([
        "טווח ציונים בקוד המקובץ",
        entry.score === null ? "לא זמין" : `${entry.score}–${entry.scoreMax}`,
      ]);
    if (r && hasValue(r[7])) fields.push(["אימות גאוגרפי", r[7]]);
    if (r && hasValue(r[8])) fields.push(["מרחק מעוגן התאמה", `${r[8]} מ׳`]);
    $("technical").replaceChildren();
    fields.forEach(([label, value]) => {
      const item = el("div", "technical-item");
      const term = el("dt");
      label.split(/(\d+–\d+)/).forEach((part) => {
        if (/^\d+–\d+$/.test(part)) {
          const range = el("bdi", "", part);
          range.dir = "ltr";
          term.append(range);
        } else term.append(document.createTextNode(part));
      });
      const definition = el("dd", "", value);
      if (/^\d+(?:\.\d+)?–\d+(?:\.\d+)?$/.test(String(value))) {
        definition.dir = "ltr";
        definition.style.textAlign = "right";
      }
      item.append(term, definition);
      $("technical").append(item);
    });
    $("technical-source").textContent =
      `מקור: תוצאות ההתאמה של ${data.meta.place}. ציון התאמת השמות אינו ביטחון מודל ה־AI ואינו אחוז ודאות.${entry.ids.length > 1 ? " בקוד מקובץ הציון והסטטוס מתייחסים להתאמה בעלת הציון הנמוך הזמין." : ""} ${data.geometryNote || ""}`.trim();
  }
  function select(entry) {
    state.selected = entry;
    $("selection").hidden = false;
    $("selection-title").textContent = entry.osm || entry.official;
    $("selected-osm").textContent = entry.osm || "לא זוהה רחוב בנתוני OSM";
    $("selected-official").textContent = entry.official || "לא זוהה שם רשמי";
    $("selected-reason").textContent = reason(entry);
    $("locate-osm").disabled = !entry.ids.some(
      (id) => data.geometry[id] && data.geometry[id].length,
    );
    $("selected-score").replaceChildren(scoreDisplay(entry));
    renderTechnical(entry);
    render();
    focusSelected();
    $("selection").scrollIntoView({ behavior: "smooth", block: "nearest" });
  }
  function reset() {
    state.query = "";
    $("search").value = "";
    resetScoreFilter();
    state.pageSize = "all";
    applyView("all");
    if (map && cityBounds) map.fitBounds(cityBounds, { padding: [15, 15] });
  }
  function exportCSV() {
    const safe = (v) => {
      let text = String(v == null ? "" : v);
      if (/^[\s]*[=+@-]/.test(text) || /^[\t\r\n]/.test(text))
        text = "'" + text;
      return '"' + text.replace(/"/g, '""') + '"';
    };
    const rows = [
      [
        "שם OSM",
        "שם למ״ס / מועמד",
        "קוד למ״ס סופי",
        "ציון התאמה (0–100; בקוד מקובץ: הנמוך הזמין)",
        "ציון מרבי בקוד מקובץ",
        "סטטוס",
        "למה זה סומן",
      ],
      ...filtered.map((e) => [
        e.osm,
        e.official,
        e.code,
        e.score === null ? "לא זמין" : e.score,
        e.scoreMax === null ? "לא זמין" : e.scoreMax,
        styles[e.status][0],
        reason(e, true),
      ]),
    ];
    const blob = new Blob(
      ["\uFEFF" + rows.map((row) => row.map(safe).join(",")).join("\r\n")],
      { type: "text/csv;charset=utf-8" },
    );
    const url = URL.createObjectURL(blob);
    const anchor = el("a");
    anchor.href = url;
    anchor.download = `${String(data.meta.place).replace(/[\\/:*?"<>|\s]+/g, "_")}-streets.csv`;
    anchor.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  function bindEvents() {
    $("score-min").oninput = updateScoreFilter;
    $("score-max").oninput = updateScoreFilter;
    $("score-mode").onchange = () => {
      const mode = $("score-mode").value;
      if (mode !== "scored") {
        $("score-min").value = "";
        $("score-max").value = "";
      }
      updateScoreFilter(false);
    };
    $("clear-scores").onclick = () => {
      resetScoreFilter();
      state.page = 0;
      clearSelection();
      render();
    };
    $("sort-order").onchange = () => setSort($("sort-order").value);
    document.querySelectorAll("[data-sort-key]").forEach((button) => {
      button.onclick = () =>
        setSort(
          state.sort === `${button.dataset.sortKey}-asc`
            ? `${button.dataset.sortKey}-desc`
            : `${button.dataset.sortKey}-asc`,
        );
    });
    $("page-size").onchange = () => {
      state.pageSize =
        $("page-size").value === "all" ? "all" : Number($("page-size").value);
      state.page = 0;
      render();
    };
    $("clear-table-filters").onclick = () => {
      state.query = "";
      $("search").value = "";
      resetScoreFilter();
      state.page = 0;
      clearSelection();
      render();
    };
    document
      .querySelectorAll(".metric-card, .snapshot [data-view]")
      .forEach(
        (button) => (button.onclick = () => applyView(button.dataset.view)),
      );
    $("search").oninput = (event) => {
      state.query = normalized(event.target.value);
      state.page = 0;
      clearSelection();
      render();
    };
    $("reset").onclick = reset;
    $("empty-reset").onclick = reset;
    $("fit-city").onclick = () => {
      if (map && cityBounds) map.fitBounds(cityBounds, { padding: [15, 15] });
    };
    $("gov-city").onclick = () => navigateGov();
    $("dismiss-gov-help").onclick = () => {
      $("gov-help").hidden = true;
    };
    // Cross-origin iframe load events cannot prove that a usable map rendered.
    // Offer a dismissible, conditional-language hint rather than claiming success.
    setTimeout(() => {
      $("gov-help").hidden = false;
    }, 9000);
    $("locate-gov").onclick = () => {
      if (state.selected)
        navigateGov(state.selected.osm || state.selected.official);
      $("gov-map").scrollIntoView({ behavior: "smooth", block: "center" });
    };
    $("locate-osm").onclick = () => focusSelected(true);
    $("close-selection").onclick = () => {
      clearSelection();
      render();
    };
    $("layers-toggle").onclick = () => {
      state.colors = !state.colors;
      $("layers-toggle").setAttribute("aria-pressed", String(state.colors));
      $("layers-toggle").textContent = state.colors
        ? "הסתרת צבעי התאמה"
        : "הצגת צבעי התאמה";
      paintMap();
    };
    $("prev").onclick = () => {
      if (state.page > 0) {
        state.page--;
        render();
      }
    };
    $("next").onclick = () => {
      if ((state.page + 1) * pageSize() < filtered.length) {
        state.page++;
        render();
      }
    };
    $("export").onclick = exportCSV;
    document.addEventListener("keydown", (event) => {
      const editing =
        /INPUT|TEXTAREA|SELECT/.test(event.target.tagName) ||
        event.target.isContentEditable;
      if (event.key === "/" && !editing) {
        event.preventDefault();
        $("search").focus();
      }
      if (event.key === "Escape" && !editing) {
        clearSelection();
        render();
      }
    });
  }
  function embedded(id) {
    const node = $(id);
    if (!node) throw new Error(`Missing embedded data: ${id}`);
    return JSON.parse(node.textContent);
  }
  // Lines are stored as integer degrees * 1e5, delta-encoded after the first
  // point (see settlement_page.py); Leaflet needs [lat, lon] pairs.
  function decodeLine(flat) {
    const points = [];
    let lat = 0;
    let lon = 0;
    for (let i = 0; i + 1 < flat.length; i += 2) {
      lat += flat[i];
      lon += flat[i + 1];
      points.push([lat / 1e5, lon / 1e5]);
    }
    return points;
  }
  function readEmbeddedData() {
    const room = embedded("room-data");
    if (room.geometryEncoding === "delta-e5")
      room.geometry = room.geometry.map((lines) => lines.map(decodeLine));
    return {
      meta: embedded("page-meta"),
      records: embedded("street-records"),
      ...room,
    };
  }
  async function init() {
    try {
      data = readEmbeddedData();
      if (!Scores) throw new Error("The score helper could not be loaded");
      // Initialize the table first. Leaflet is optional for all table operations.
      setupModels();
      setupOverview();
      bindEvents();
      render();
      $("app-status").dataset.state = "ready";
      $("app-status").textContent =
        `הטבלה והמיון פעילים · ${number(entries.length)} שמות OSM נטענו`;
      try {
        if (typeof L === "undefined") throw new Error("Leaflet unavailable");
        setupMap();
        paintMap();
      } catch (mapError) {
        console.warn("Map unavailable; table remains usable:", mapError);
        $("tile-warning").hidden = false;
        $("tile-warning").textContent =
          "לא ניתן לטעון את המפה. הטבלה, החיפוש והמיון פעילים ללא מפה.";
        $("fit-city").disabled = true;
      }
      navigateGov();
    } catch (error) {
      console.error(error);
      $("app-status").dataset.state = "error";
      $("app-status").textContent =
        "כלי הטבלה לא נטענו — המיון אינו פעיל.";
      $("fatal").hidden = false;
      $("fatal").textContent =
        "לא ניתן לטעון את נתוני העמוד. יש לוודא שתיקיית assets זמינה לצד תיקיית HTML ולרענן את העמוד.";
    }
  }
  init();
})();

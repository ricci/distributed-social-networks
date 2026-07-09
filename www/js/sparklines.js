// Shannon-index sparklines, rendered with uPlot into per-card placeholder
// elements. Driven from the `sparklines` list in index.html (mirroring the
// `gauges` list) rather than by DOM discovery. Data comes from the `history`
// object in history.js; a card is only wired up if that section has history.

var LINE = "#aebeda";
var AXIS = "#a7a9ab";
var GRID = "rgba(255,255,255,0.08)";
var SPARK_FONT = '10px "Kade-Regular", sans-serif';

// history.js stores ISO timestamps; uPlot wants seconds since the epoch.
function sparklineData(series) {
  var xs = series.t.map(function (t) {
    return Math.round(Date.parse(t) / 1000);
  });
  return [xs, series.shannon];
}

// A compact "Jan – Jul 2026" caption for the span the sparkline covers.
function sparklineSpan(times) {
  function month(iso) {
    return new Date(iso).toLocaleDateString(undefined, {
      month: "short",
      timeZone: "UTC",
    });
  }
  var last = times[times.length - 1];
  return month(times[0]) + " – " + month(last) + " " + new Date(last).getUTCFullYear();
}

function makeMiniSparkline(el, data) {
  return new uPlot(
    {
      width: el.clientWidth || 116,
      height: 26,
      padding: [3, 3, 3, 3],
      cursor: { show: false },
      legend: { show: false },
      select: { show: false },
      axes: [{ show: false }, { show: false }],
      series: [{}, { stroke: LINE, width: 1.25, points: { show: false } }],
    },
    data,
    el
  );
}

function makeDetailSparkline(el, data) {
  // Our own readout instead of uPlot's legend: no series label/marker, just the
  // hovered date and value. Fixed-width digits (tabular-nums) plus a numeric
  // date and fixed decimals keep it from shifting as the cursor moves.
  var readout = document.createElement("div");
  readout.className = "spark-readout";
  var dateEl = document.createElement("span");
  dateEl.className = "spark-ro-date";
  var valEl = document.createElement("span");
  valEl.className = "spark-ro-val";
  readout.appendChild(dateEl);
  readout.appendChild(valEl);

  function isoDate(seconds) {
    var d = new Date(seconds * 1000);
    var m = String(d.getUTCMonth() + 1).padStart(2, "0");
    var day = String(d.getUTCDate()).padStart(2, "0");
    return d.getUTCFullYear() + "-" + m + "-" + day;
  }
  function update(idx) {
    if (idx == null) {
      idx = data[0].length - 1; // default to the latest point
    }
    dateEl.textContent = isoDate(data[0][idx]);
    var v = data[1][idx];
    valEl.textContent = v == null ? "" : v.toFixed(4);
  }

  var u = new uPlot(
    {
      width: el.clientWidth || 248,
      height: 148,
      padding: [6, 8, 2, 2],
      cursor: { points: { size: 5 } },
      legend: { show: false },
      scales: { x: { time: true } },
      series: [{}, { stroke: LINE, width: 1.5, points: { show: false } }],
      axes: [
        {
          stroke: AXIS,
          grid: { show: false },
          ticks: { stroke: GRID, size: 3 },
          font: SPARK_FONT,
          size: 24,
          // Month-only ticks; drop uPlot's default second-tier year row.
          values: function (u, splits) {
            return splits.map(function (s) {
              return new Date(s * 1000).toLocaleDateString(undefined, {
                month: "short",
                timeZone: "UTC",
              });
            });
          },
        },
        {
          stroke: AXIS,
          grid: { stroke: GRID, width: 1 },
          ticks: { show: false },
          font: SPARK_FONT,
          size: 30,
        },
      ],
      hooks: {
        ready: [function () { update(null); }],
        setCursor: [function (self) { update(self.cursor.idx); }],
      },
    },
    data,
    el
  );

  // Append after uPlot builds its DOM so the readout sits below the chart.
  el.appendChild(readout);
  return u;
}

// cfg: { mini: elementId, detail: elementId, section: historyKey }
function renderSparkline(cfg) {
  if (typeof historyData === "undefined" || !historyData.sections || typeof uPlot === "undefined") {
    return;
  }
  var series = historyData.sections[cfg.section];
  if (!series || !series.shannon || series.shannon.length < 2) {
    return;
  }
  var miniEl = document.getElementById(cfg.mini);
  var detailEl = document.getElementById(cfg.detail);
  if (!miniEl || !detailEl) {
    return;
  }

  var data = sparklineData(series);

  // Full-width sparkline with a centered span caption below it. The summary is
  // visible (details closed), so clientWidth is meaningful for sizing.
  makeMiniSparkline(miniEl, data);
  var caption = document.createElement("div");
  caption.className = "spark-span";
  caption.textContent = sparklineSpan(series.t);
  miniEl.insertAdjacentElement("afterend", caption);

  // The detail chart sits in a hidden panel (zero width until opened), so
  // build it the first time the disclosure is opened.
  var details = detailEl.closest("details");
  var built = false;
  details.addEventListener("toggle", function () {
    if (details.open && !built) {
      built = true;
      makeDetailSparkline(detailEl, data);
    }
  });

  // Keep the weekly trend text and its "Details" disclosure grouped together by
  // moving the historical sparkline to the bottom of the Shannon box.
  details.parentNode.appendChild(details);
}

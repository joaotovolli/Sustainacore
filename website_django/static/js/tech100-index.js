(() => {
  const parseJson = (id) => {
    const el = document.getElementById(id);
    if (!el) return null;
    try {
      return JSON.parse(el.textContent || "null");
    } catch (err) {
      console.warn("Unable to parse JSON script", id, err);
      return null;
    }
  };

  const formatNumber = (value, decimals = 2) => {
    if (value === null || value === undefined || Number.isNaN(value)) return "—";
    return Number(value).toFixed(decimals);
  };

  const formatPct = (value, decimals = 2) => {
    if (value === null || value === undefined || Number.isNaN(value)) return "—";
    return `${Number(value).toFixed(decimals)}%`;
  };

  const formatDate = (value) => {
    if (!value) return "—";
    return value;
  };

  const calcDrawdown = (levels) => {
    if (!levels?.length) return [];
    let peak = levels[0].level;
    return levels.map((point) => {
      if (point.level > peak) peak = point.level;
      const drawdown = peak ? (point.level / peak) - 1 : 0;
      return { date: point.date, drawdown };
    });
  };

  const calcRollingVol = (levels, window = 30) => {
    if (!levels?.length || levels.length <= window) return [];
    const returns = [];
    for (let i = 1; i < levels.length; i += 1) {
      const prev = levels[i - 1].level;
      const curr = levels[i].level;
      returns.push({ date: levels[i].date, ret: prev ? (curr / prev) - 1 : 0 });
    }
    const series = [];
    for (let i = window - 1; i < returns.length; i += 1) {
      const windowRets = returns.slice(i - window + 1, i + 1).map((row) => row.ret);
      const mean = windowRets.reduce((sum, val) => sum + val, 0) / windowRets.length;
      const variance = windowRets.reduce((sum, val) => sum + (val - mean) ** 2, 0) / windowRets.length;
      const vol = Math.sqrt(variance) * Math.sqrt(252);
      series.push({ date: returns[i].date, vol });
    }
    return series;
  };

  const ensureChartDefaults = () => {
    if (typeof Chart === "undefined") return;
    Chart.defaults.animation = false;
    Chart.defaults.transitions.active.animation = false;
  };

  const initOverview = () => {
    const chartEl = document.getElementById("tech100-level-chart");
    if (!chartEl || typeof Chart === "undefined") return;

    const levelsData = parseJson("tech100-levels-data") || [];
    ensureChartDefaults();

    const buildDataset = (data) => ({
      labels: data.map((point) => point.date),
      datasets: [
        {
          label: "Index level",
          data: data.map((point) => point.level),
          borderColor: "#1c2b4a",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.15,
        },
      ],
    });

    const chart = new Chart(chartEl.getContext("2d"), {
      type: "line",
      data: buildDataset(levelsData),
      options: {
        animation: false,
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { ticks: { maxTicksLimit: 8 } },
          y: { beginAtZero: false },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => `Level: ${formatNumber(context.raw, 2)}`,
            },
          },
        },
      },
    });

    const updateRisk = (data) => {
      if (!data.length) return;
      let peak = data[0].level;
      let maxDrawdown = 0;
      let best = { date: data[0].date, ret: -Infinity };
      let worst = { date: data[0].date, ret: Infinity };

      for (let i = 1; i < data.length; i += 1) {
        const prev = data[i - 1].level;
        const curr = data[i].level;
        const ret = prev ? (curr / prev) - 1 : 0;
        if (ret > best.ret) best = { date: data[i].date, ret };
        if (ret < worst.ret) worst = { date: data[i].date, ret };

        if (curr > peak) peak = curr;
        if (peak) {
          const dd = (curr / peak) - 1;
          if (dd < maxDrawdown) maxDrawdown = dd;
        }
      }

      const drawdownEl = document.getElementById("risk-drawdown");
      const bestDayEl = document.getElementById("risk-best-day");
      const bestReturnEl = document.getElementById("risk-best-return");
      const worstDayEl = document.getElementById("risk-worst-day");
      const worstReturnEl = document.getElementById("risk-worst-return");
      if (drawdownEl) drawdownEl.textContent = formatPct(maxDrawdown * 100, 2);
      if (bestDayEl) bestDayEl.textContent = formatDate(best.date);
      if (bestReturnEl) bestReturnEl.textContent = formatNumber(best.ret * 100, 2);
      if (worstDayEl) worstDayEl.textContent = formatDate(worst.date);
      if (worstReturnEl) worstReturnEl.textContent = formatNumber(worst.ret * 100, 2);
    };

    updateRisk(levelsData);

    document.querySelectorAll(".tech100-range-btn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const range = btn.dataset.range;
        if (!range) return;
        document.querySelectorAll(".tech100-range-btn").forEach((el) =>
          el.classList.toggle("is-active", el === btn)
        );
        try {
          const resp = await fetch(`/tech100/index-levels/?range=${range}`);
          const payload = await resp.json();
          if (!payload.levels) return;
          chart.data = buildDataset(payload.levels);
          chart.update();
          updateRisk(payload.levels);
        } catch (err) {
          console.warn("Unable to update chart range", err);
        }
      });
    });
  };

  const initHomeSnapshot = () => {
    const chartEl = document.getElementById("tech100-home-level-chart");
    if (!chartEl || typeof Chart === "undefined") return;
    ensureChartDefaults();

    const levelsData = parseJson("tech100-home-levels") || [];
    const chart = new Chart(chartEl.getContext("2d"), {
      type: "line",
      data: {
        labels: levelsData.map((point) => point.date),
        datasets: [
          {
            label: "Index level",
            data: levelsData.map((point) => point.level),
            borderColor: "#1c2b4a",
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.2,
          },
        ],
      },
      options: {
        animation: false,
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { ticks: { maxTicksLimit: 6 } },
          y: { beginAtZero: false },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => `Level: ${formatNumber(context.raw, 2)}`,
            },
          },
        },
      },
    });

    document.querySelectorAll(".tech100-home-range").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const range = btn.dataset.range;
        if (!range) return;
        document.querySelectorAll(".tech100-home-range").forEach((el) =>
          el.classList.toggle("is-active", el === btn)
        );
        try {
          const resp = await fetch(`/tech100/index-levels/?range=${range}`);
          const payload = await resp.json();
          if (!payload.levels) return;
          chart.data.labels = payload.levels.map((point) => point.date);
          chart.data.datasets[0].data = payload.levels.map((point) => point.level);
          chart.update();
        } catch (err) {
          console.warn("Unable to update home snapshot range", err);
        }
      });
    });
  };

  const initPerformance = () => {
    const levelCanvas = document.getElementById("tech100-performance-level-chart");
    if (!levelCanvas || typeof Chart === "undefined") return;
    ensureChartDefaults();

    let levels = parseJson("tech100-performance-levels") || [];
    let drawdown = parseJson("tech100-performance-drawdown") || calcDrawdown(levels);
    let volSeries = parseJson("tech100-performance-vol") || calcRollingVol(levels, 30);

    const buildLineChart = (canvas, label, data, color) =>
      new Chart(canvas.getContext("2d"), {
        type: "line",
        data: {
          labels: data.map((point) => point.date),
          datasets: [
            {
              label,
              data: data.map((point) => point.value),
              borderColor: color,
              borderWidth: 2,
              pointRadius: 0,
              tension: 0.2,
            },
          ],
        },
        options: {
          animation: false,
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            x: { ticks: { maxTicksLimit: 6 } },
            y: { beginAtZero: false },
          },
          plugins: { legend: { display: false } },
        },
      });

    const levelChart = buildLineChart(
      levelCanvas,
      "Index level",
      levels.map((point) => ({ date: point.date, value: point.level })),
      "#1c2b4a"
    );
    const drawdownCanvas = document.getElementById("tech100-performance-drawdown-chart");
    const drawdownChart = drawdownCanvas
      ? buildLineChart(
          drawdownCanvas,
          "Drawdown",
          drawdown.map((point) => ({ date: point.date, value: point.drawdown * 100 })),
          "#b44a3c"
        )
      : null;
    const volCanvas = document.getElementById("tech100-performance-vol-chart");
    const volChart = volCanvas
      ? buildLineChart(
          volCanvas,
          "Rolling vol",
          volSeries.map((point) => ({ date: point.date, value: point.vol * 100 })),
          "#2f6f6f"
        )
      : null;

    document.querySelectorAll(".tech100-performance__overview .tech100-range-btn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const range = btn.dataset.range;
        if (!range) return;
        document
          .querySelectorAll(".tech100-performance__overview .tech100-range-btn")
          .forEach((el) => el.classList.toggle("is-active", el === btn));
        try {
          const resp = await fetch(`/tech100/index-levels/?range=${range}`);
          const payload = await resp.json();
          if (!payload.levels) return;
          levels = payload.levels;
          drawdown = calcDrawdown(levels);
          volSeries = calcRollingVol(levels, 30);

          levelChart.data.labels = levels.map((point) => point.date);
          levelChart.data.datasets[0].data = levels.map((point) => point.level);
          levelChart.update();

          if (drawdownChart) {
            drawdownChart.data.labels = drawdown.map((point) => point.date);
            drawdownChart.data.datasets[0].data = drawdown.map((point) => point.drawdown * 100);
            drawdownChart.update();
          }
          if (volChart) {
            volChart.data.labels = volSeries.map((point) => point.date);
            volChart.data.datasets[0].data = volSeries.map((point) => point.vol * 100);
            volChart.update();
          }
        } catch (err) {
          console.warn("Unable to update performance range", err);
        }
      });
    });

    const holdingsRows = parseJson("tech100-performance-holdings") || [];
    const holdingsBody = document.getElementById("tech100-holdings-body");
    if (holdingsBody) {
      holdingsBody.innerHTML = holdingsRows
        .map(
          (row) => `<tr>
            <td class="text-left">${row.ticker || "—"}</td>
            <td class="text-left">${row.name || "—"}</td>
            <td class="text-left">${row.sector || "—"}</td>
            <td class="text-right">${formatNumber((row.weight || 0) * 100, 2)}</td>
            <td class="text-right">${formatNumber((row.ret_1d || 0) * 100, 2)}</td>
            <td class="text-right">${formatNumber((row.contribution || 0) * 10000, 2)}</td>
            <td class="text-center">${row.quality || "—"}</td>
          </tr>`
        )
        .join("");
    }

    const sectors = parseJson("tech100-performance-sectors") || [];
    const sectorCanvas = document.getElementById("tech100-sector-chart");
    if (sectorCanvas && sectors.length) {
      new Chart(sectorCanvas.getContext("2d"), {
        type: "doughnut",
        data: {
          labels: sectors.map((row) => row.sector),
          datasets: [
            {
              data: sectors.map((row) => (row.weight || 0) * 100),
              backgroundColor: ["#1c2b4a", "#3a5a88", "#6a8cbc", "#9bb4d6", "#c1d2ea"],
              borderWidth: 0,
            },
          ],
        },
        options: {
          animation: false,
          plugins: { legend: { position: "bottom" } },
        },
      });
    }

    const attributionRows = parseJson("tech100-performance-attribution") || [];
    const attrBody = document.getElementById("tech100-attr-body");
    const attrSearch = document.getElementById("tech100-attr-search");
    const attrImputed = document.getElementById("tech100-attr-imputed");
    const attrSector = document.getElementById("tech100-attr-sector");

    const renderAttributionTable = (rows) => {
      if (!attrBody) return;
      attrBody.innerHTML = rows
        .map(
          (row) => `<tr>
            <td class="text-left">${row.ticker || "—"}</td>
            <td class="text-left">${row.name || "—"}</td>
            <td class="text-right">${formatNumber((row.weight || 0) * 100, 2)}</td>
            <td class="text-right">${formatNumber((row.ret_1d || 0) * 100, 2)}</td>
            <td class="text-right">${formatNumber((row.contribution || 0) * 10000, 2)}</td>
            <td class="text-right">${formatNumber((row.contrib_mtd || 0) * 10000, 2)}</td>
            <td class="text-right">${formatNumber((row.contrib_ytd || 0) * 10000, 2)}</td>
            <td class="text-center">${row.quality || "—"}</td>
          </tr>`
        )
        .join("");
    };

    const updateFilters = () => {
      const query = (attrSearch?.value || "").toLowerCase().trim();
      const imputedOnly = attrImputed?.checked;
      const sector = attrSector?.value || "";
      const filtered = attributionRows.filter((row) => {
        const ticker = (row.ticker || "").toLowerCase();
        const name = (row.name || "").toLowerCase();
        if (query && !ticker.includes(query) && !name.includes(query)) return false;
        if (imputedOnly && row.quality !== "IMPUTED") return false;
        if (sector && (row.sector || "") !== sector) return false;
        return true;
      });
      renderAttributionTable(filtered);
    };

    if (attrSector && attributionRows.length) {
      const sectorList = Array.from(
        new Set(attributionRows.map((row) => row.sector).filter(Boolean))
      ).sort();
      attrSector.innerHTML = `<option value="">All sectors</option>${sectorList
        .map((item) => `<option value="${item}">${item}</option>`)
        .join("")}`;
    }

    attrSearch?.addEventListener("input", updateFilters);
    attrImputed?.addEventListener("change", updateFilters);
    attrSector?.addEventListener("change", updateFilters);
    updateFilters();

    const setBarChart = (canvasId, rows, color) => {
      const el = document.getElementById(canvasId);
      if (!el) return null;
      return new Chart(el.getContext("2d"), {
        type: "bar",
        data: {
          labels: rows.map((row) => row.ticker),
          datasets: [
            {
              label: "Contribution",
              data: rows.map((row) => (row.contribution || 0) * 10000),
              backgroundColor: color,
            },
          ],
        },
        options: {
          indexAxis: "y",
          animation: false,
          plugins: { legend: { display: false } },
        },
      });
    };

    const topSets = {
      "1d": parseJson("tech100-performance-top-1d") || [],
      mtd: parseJson("tech100-performance-top-mtd") || [],
      ytd: parseJson("tech100-performance-top-ytd") || [],
    };
    const worstSets = {
      "1d": parseJson("tech100-performance-worst-1d") || [],
      mtd: parseJson("tech100-performance-worst-mtd") || [],
      ytd: parseJson("tech100-performance-worst-ytd") || [],
    };

    let topChart = setBarChart("tech100-top-contrib-chart", topSets["1d"], "#2f6f6f");
    let worstChart = setBarChart("tech100-worst-contrib-chart", worstSets["1d"], "#b44a3c");

    document.querySelectorAll(".tech100-attr-range").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const range = btn.dataset.range || "1d";
        document.querySelectorAll(".tech100-attr-range").forEach((el) =>
          el.classList.toggle("is-active", el === btn)
        );
        try {
          const resp = await fetch(`/tech100/performance/attribution/?range=${range}`);
          const payload = await resp.json();
          const top = payload.top || [];
          const worst = payload.worst || [];
          if (topChart) {
            topChart.data.labels = top.map((row) => row.ticker);
            topChart.data.datasets[0].data = top.map((row) => (row.contribution || 0) * 10000);
            topChart.update();
          } else {
            topChart = setBarChart("tech100-top-contrib-chart", top, "#2f6f6f");
          }
          if (worstChart) {
            worstChart.data.labels = worst.map((row) => row.ticker);
            worstChart.data.datasets[0].data = worst.map((row) => (row.contribution || 0) * 10000);
            worstChart.update();
          } else {
            worstChart = setBarChart("tech100-worst-contrib-chart", worst, "#b44a3c");
          }
        } catch (err) {
          console.warn("Unable to update attribution range", err);
        }
      });
    });
  };

  const initConstituents = () => {
    const tableBody = document.getElementById("constituents-body");
    if (!tableBody) return;
    const dateInput = document.getElementById("constituents-date");
    const searchInput = document.getElementById("constituents-search");
    const imputedToggle = document.getElementById("constituents-imputed");
    const weightSumEl = document.getElementById("constituents-weight-sum");

    let rows = parseJson("tech100-constituents-data") || [];
    let sortKey = "weight";
    let sortDir = "desc";

    const render = () => {
      const query = (searchInput?.value || "").toLowerCase().trim();
      const imputedOnly = imputedToggle?.checked;

      let filtered = rows.filter((row) => {
        const ticker = (row.ticker || "").toLowerCase();
        const name = (row.name || "").toLowerCase();
        if (query && !ticker.includes(query) && !name.includes(query)) return false;
        if (imputedOnly && row.quality !== "IMPUTED") return false;
        return true;
      });

      filtered = filtered.sort((a, b) => {
        const aVal = a[sortKey];
        const bVal = b[sortKey];
        if (typeof aVal === "number" && typeof bVal === "number") {
          return sortDir === "asc" ? aVal - bVal : bVal - aVal;
        }
        return sortDir === "asc"
          ? String(aVal || "").localeCompare(String(bVal || ""))
          : String(bVal || "").localeCompare(String(aVal || ""));
      });

      tableBody.innerHTML = filtered
        .map(
          (row) => `<tr>
            <td class="text-left">${row.ticker || "—"}</td>
            <td class="text-left">${row.name || "—"}</td>
            <td class="text-right">${formatNumber((row.weight || 0) * 100, 2)}</td>
            <td class="text-right">${formatNumber(row.price, 2)}</td>
            <td class="text-right">${formatNumber((row.ret_1d || 0) * 100, 2)}</td>
            <td class="text-center">${row.quality || "—"}</td>
          </tr>`
        )
        .join("");

      const weightSum = filtered.reduce((sum, row) => sum + (row.weight || 0), 0);
      if (weightSumEl) weightSumEl.textContent = `${formatNumber(weightSum * 100, 2)}%`;
    };

    document.querySelectorAll(".table-sort").forEach((button) => {
      button.addEventListener("click", () => {
        const key = button.dataset.key;
        if (!key) return;
        if (sortKey === key) {
          sortDir = sortDir === "asc" ? "desc" : "asc";
        } else {
          sortKey = key;
          sortDir = "desc";
        }
        render();
      });
    });

    searchInput?.addEventListener("input", render);
    imputedToggle?.addEventListener("change", render);

    dateInput?.addEventListener("change", async () => {
      if (!dateInput.value) return;
      try {
        const resp = await fetch(`/tech100/constituents/data/?date=${dateInput.value}`);
        const payload = await resp.json();
        rows = payload.rows || [];
        render();
      } catch (err) {
        console.warn("Unable to load constituents", err);
      }
    });

    render();
  };

  const initAttribution = () => {
    const topBody = document.getElementById("attribution-top");
    const bottomBody = document.getElementById("attribution-bottom");
    if (!topBody || !bottomBody) return;
    const dateInput = document.getElementById("attribution-date");
    const indexReturnEl = document.getElementById("attribution-index-return");
    const sumEl = document.getElementById("attribution-sum");

    let rows = [
      ...(parseJson("tech100-attribution-top") || []),
      ...(parseJson("tech100-attribution-bottom") || []),
    ];

    const renderTables = (data) => {
      const positives = data.filter((row) => (row.contribution || 0) > 0).slice(0, 15);
      const negatives = data.filter((row) => (row.contribution || 0) < 0).slice(-15).reverse();

      const renderRow = (row) => `<tr>
          <td class="text-left">${row.ticker || "—"}</td>
          <td class="text-right">${formatNumber((row.weight || row.weight_prev || 0) * 100, 2)}</td>
          <td class="text-right">${formatNumber((row.ret_1d || 0) * 100, 2)}</td>
          <td class="text-right">${formatNumber((row.contribution || 0) * 100, 2)}</td>
          <td class="text-center">${row.quality || "—"}</td>
        </tr>`;

      topBody.innerHTML = positives.map(renderRow).join("");
      bottomBody.innerHTML = negatives.map(renderRow).join("");

      const sum = data.reduce((acc, row) => acc + (row.contribution || 0), 0);
      if (sumEl) sumEl.textContent = formatPct(sum * 100, 2);
    };

    dateInput?.addEventListener("change", async () => {
      if (!dateInput.value) return;
      try {
        const resp = await fetch(`/tech100/attribution/data/?date=${dateInput.value}`);
        const payload = await resp.json();
        rows = payload.rows || [];
        renderTables(rows);
        if (indexReturnEl) indexReturnEl.textContent = formatPct(payload.index_return, 2);
      } catch (err) {
        console.warn("Unable to load attribution", err);
      }
    });

    renderTables(rows);
  };

  const initStats = () => {
    const grid = document.getElementById("stats-grid");
    const dateInput = document.getElementById("stats-date");
    if (!grid || !dateInput) return;

    const renderStats = (stats) => {
      if (!stats) {
        grid.innerHTML = "<p class=\"muted\">No stats available for the selected date.</p>";
        return;
      }
      grid.innerHTML = `
        <div class="stat-tile"><span class="muted">Level TR</span><strong>${formatNumber(stats.level_tr, 2)}</strong></div>
        <div class="stat-tile"><span class="muted">1D Return</span><strong>${formatNumber(stats.ret_1d, 4)}</strong></div>
        <div class="stat-tile"><span class="muted">5D Return</span><strong>${formatNumber(stats.ret_5d, 4)}</strong></div>
        <div class="stat-tile"><span class="muted">20D Return</span><strong>${formatNumber(stats.ret_20d, 4)}</strong></div>
        <div class="stat-tile"><span class="muted">20D Volatility</span><strong>${formatNumber(stats.vol_20d, 4)}</strong></div>
        <div class="stat-tile"><span class="muted">Max Drawdown (252D)</span><strong>${formatNumber(stats.max_drawdown_252d, 4)}</strong></div>
        <div class="stat-tile"><span class="muted">Constituents</span><strong>${stats.n_constituents ?? "—"}</strong></div>
        <div class="stat-tile"><span class="muted">Imputed</span><strong>${stats.n_imputed ?? "—"}</strong></div>
        <div class="stat-tile"><span class="muted">Top 5 Weight</span><strong>${formatNumber(stats.top5_weight, 4)}</strong></div>
        <div class="stat-tile"><span class="muted">Herfindahl</span><strong>${formatNumber(stats.herfindahl, 4)}</strong></div>
      `;
    };

    renderStats(parseJson("tech100-stats-data"));

    dateInput.addEventListener("change", async () => {
      if (!dateInput.value) return;
      try {
        const resp = await fetch(`/tech100/stats/data/?date=${dateInput.value}`);
        const payload = await resp.json();
        renderStats(payload.stats);
      } catch (err) {
        console.warn("Unable to load stats", err);
      }
    });
  };

  initOverview();
  initHomeSnapshot();
  initPerformance();
  initConstituents();
  initAttribution();
  initStats();
})();

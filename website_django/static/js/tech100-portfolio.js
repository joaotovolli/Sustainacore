(() => {
  const dateFormat = window.SCDateFormat || {};

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

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const formatNumber = (value, decimals = 2) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    return Number(value).toFixed(decimals);
  };

  const formatPct = (value, decimals = 2) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    return `${Number(value).toFixed(decimals)}%`;
  };

  const formatBp = (value, decimals = 2) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    return `${Number(value).toFixed(decimals)} bp`;
  };

  const formatAsOfDate = (value) =>
    typeof dateFormat.formatAsOfDate === "function" ? dateFormat.formatAsOfDate(value) : value || "—";

  const resolveAxisFormatter = (rangeKey, data) => {
    if (typeof dateFormat.chooseAxisFormatter === "function") {
      return dateFormat.chooseAxisFormatter(rangeKey, data);
    }
    return (value) => value;
  };

  const setAxisFormatter = (chart, rangeKey, data) => {
    if (!chart?.options?.scales?.x) return;
    const formatter = resolveAxisFormatter(rangeKey, data);
    const labels = chart.data?.labels || [];
    const shortFormatter =
      typeof dateFormat.formatAxisShort === "function"
        ? dateFormat.formatAxisShort
        : formatter;
    chart.options.scales.x.ticks = {
      ...(chart.options.scales.x.ticks || {}),
      autoSkip: true,
      callback: (value, index) => {
        const label = typeof value === "number" ? labels[value] : labels[index] ?? value;
        const formatted = formatter(label ?? value);
        const prevLabel = index > 0 ? labels[index - 1] : null;
        const prevFormatted = prevLabel ? formatter(prevLabel) : null;
        if (formatted && prevFormatted && formatted === prevFormatted) {
          return shortFormatter(label ?? value);
        }
        return formatted;
      },
    };
  };

  const withAlpha = (hex, alpha) => {
    if (!hex || typeof hex !== "string") return hex;
    const cleaned = hex.replace("#", "");
    if (cleaned.length !== 6) return hex;
    const r = parseInt(cleaned.slice(0, 2), 16);
    const g = parseInt(cleaned.slice(2, 4), 16);
    const b = parseInt(cleaned.slice(4, 6), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  };

  const toDate = (value) => new Date(`${value}T00:00:00Z`);

  const filterByRange = (points, rangeKey) => {
    if (!Array.isArray(points) || !points.length || rangeKey === "max") return points || [];
    const latest = toDate(points[points.length - 1].date);
    if (rangeKey === "ytd") {
      const start = new Date(Date.UTC(latest.getUTCFullYear(), 0, 1));
      return points.filter((point) => toDate(point.date) >= start);
    }
    const daysByRange = {
      "3m": 90,
      "6m": 180,
      "1y": 365,
    };
    const days = daysByRange[rangeKey] || 180;
    const cutoff = new Date(latest.getTime() - days * 24 * 60 * 60 * 1000);
    return points.filter((point) => toDate(point.date) >= cutoff);
  };

  const toRelativeSeries = (points) => {
    if (!points?.length) return [];
    const base = Number(points[0].level || 0);
    if (!base) return [];
    return points.map((point) => ({
      date: point.date,
      value: ((Number(point.level || 0) / base) - 1) * 100,
    }));
  };

  const toMetricSeries = (points, metricKey) => {
    const field = metricKey === "volatility" ? "vol20" : "drawdown";
    return (points || []).map((point) => ({
      date: point.date,
      value:
        point[field] === null || point[field] === undefined ? null : Number(point[field]) * 100,
    }));
  };

  const alignSeries = (seriesMap, codes) => {
    const labels = Array.from(
      new Set(
        codes.flatMap((code) => (seriesMap[code] || []).map((point) => point.date))
      )
    ).sort();
    const aligned = {};
    codes.forEach((code) => {
      const byDate = new Map((seriesMap[code] || []).map((point) => [point.date, point.value]));
      aligned[code] = labels.map((date) => (byDate.has(date) ? byDate.get(date) : null));
    });
    return { labels, aligned };
  };

  const bpFieldForKey = (metricKey) => {
    switch (metricKey) {
      case "contrib_1d":
        return "contrib_1d_bp";
      case "contrib_5d":
        return "contrib_5d_bp";
      case "contrib_20d":
        return "contrib_20d_bp";
      case "contrib_mtd":
        return "contrib_mtd_bp";
      case "contrib_ytd":
      default:
        return "contrib_ytd_bp";
    }
  };

  const ensureChartDefaults = () => {
    if (typeof Chart === "undefined") return;
    Chart.defaults.animation = false;
    Chart.defaults.transitions.active.animation = false;
  };

  const statusEl = document.getElementById("tech100-portfolio-status");
  const workspace = parseJson("tech100-portfolio-workspace");
  if (!statusEl || !workspace) {
    dateFormat.applyDateFormatting?.();
    return;
  }

  const modelButtons = Array.from(document.querySelectorAll("[data-portfolio-model-button]"));
  const matrixButtons = Array.from(document.querySelectorAll("[data-model-code]")).filter((el) =>
    el.classList.contains("tech100-portfolio__matrix-row")
  );
  const rangeButtons = Array.from(document.querySelectorAll(".tech100-portfolio-range"));
  const metricButtons = Array.from(document.querySelectorAll(".tech100-portfolio-metric"));
  const viewButtons = Array.from(document.querySelectorAll(".tech100-portfolio-view"));
  const holdingsViewButtons = Array.from(
    document.querySelectorAll(".tech100-portfolio-holdings-view")
  );
  const benchmarkSelect = document.getElementById("tech100-portfolio-benchmark");
  const attributionSelect = document.getElementById("tech100-portfolio-window");
  const mainChartCanvas = document.getElementById("tech100-portfolio-level-chart");
  const sectorChartCanvas = document.getElementById("tech100-portfolio-sector-chart");

  const state = {
    selectedModelCode: statusEl.dataset.selectedModel || workspace.selectedModelCode,
    benchmarkCode: statusEl.dataset.benchmarkCode || workspace.benchmarkCode || "TECH100",
    rangeKey: statusEl.dataset.rangeKey || workspace.rangeKey || "6m",
    metricKey: statusEl.dataset.metricKey || workspace.metricKey || "relative",
    viewMode: statusEl.dataset.viewMode || workspace.viewMode || "absolute",
    attributionKey: statusEl.dataset.attributionKey || workspace.attributionKey || "contrib_ytd",
    holdingsView: statusEl.dataset.holdingsView || workspace.holdingsView || "core",
  };

  let mainChart = null;
  let sectorChart = null;

  const getModel = (code) => workspace.models?.[code] || null;

  const ensureValidState = () => {
    const available = workspace.modelOrder || [];
    if (!available.includes(state.selectedModelCode)) {
      state.selectedModelCode = available[0];
    }
    if (!available.includes(state.benchmarkCode)) {
      state.benchmarkCode = available.includes("TECH100") ? "TECH100" : available[0];
    }
    if (!state.selectedModelCode) {
      state.selectedModelCode = available[0];
    }
  };

  const buildSnapshotCards = (summary) => [
    { label: "Index level", value: summary?.level_tr_fmt, suffix: "", decimals: 2 },
    { label: "20D return", value: summary?.ret_20d_pct, suffix: "%", decimals: 2 },
    { label: "YTD return", value: summary?.ret_ytd_pct, suffix: "%", decimals: 2 },
    { label: "20D vol", value: summary?.vol_20d_pct, suffix: "%", decimals: 2 },
    { label: "Drawdown", value: summary?.drawdown_to_date_pct, suffix: "%", decimals: 2 },
    { label: "Top 5 weight", value: summary?.top5_weight_pct, suffix: "%", decimals: 2 },
  ];

  const buildDeltaCards = (summary, benchmarkSummary) => {
    const delta = (selected, benchmark) =>
      selected === null || selected === undefined || benchmark === null || benchmark === undefined
        ? null
        : Number(selected) - Number(benchmark);
    const benchmarkLabel = benchmarkSummary?.short_label || benchmarkSummary?.label || "benchmark";
    return [
      {
        label: "Vs benchmark YTD",
        value: delta(summary?.ret_ytd_pct, benchmarkSummary?.summary?.ret_ytd_pct),
        suffix: "%",
        decimals: 2,
        detail: `Active return versus ${benchmarkLabel}.`,
      },
      {
        label: "Top 5 delta",
        value: delta(summary?.top5_weight_pct, benchmarkSummary?.summary?.top5_weight_pct),
        suffix: "%",
        decimals: 2,
        detail: `Concentration difference versus ${benchmarkLabel}.`,
      },
      {
        label: "Governance delta",
        value: delta(summary?.avg_governance_score, benchmarkSummary?.summary?.avg_governance_score),
        suffix: "pts",
        decimals: 1,
        detail: `Weighted governance composite difference versus ${benchmarkLabel}.`,
      },
    ];
  };

  const buildFactorCards = (summary, benchmarkSummary) => {
    const delta = (selected, benchmark) =>
      selected === null || selected === undefined || benchmark === null || benchmark === undefined
        ? null
        : Number(selected) - Number(benchmark);
    const governanceDelta = delta(
      summary?.avg_governance_score,
      benchmarkSummary?.summary?.avg_governance_score
    );
    const top5Delta = delta(summary?.top5_weight_pct, benchmarkSummary?.summary?.top5_weight_pct);
    return [
      {
        label: "Governance composite",
        value: summary?.avg_governance_score,
        suffix: "pts",
        detail:
          governanceDelta === null
            ? "Weighted average from the live TECH100 governance dataset."
            : `${governanceDelta >= 0 ? "+" : ""}${formatNumber(governanceDelta, 2)} vs benchmark.`,
      },
      {
        label: "Momentum lens",
        value: summary?.avg_momentum_20d_pct,
        suffix: "%",
        detail: "Uses only the supported 20-day momentum signal.",
      },
      {
        label: "Low-vol lens",
        value: summary?.avg_low_vol_60d_pct,
        suffix: "%",
        detail: "Uses only the supported 60-day low-volatility signal.",
      },
      {
        label: "Concentration",
        value: summary?.top5_weight_pct,
        suffix: "%",
        detail:
          top5Delta === null
            ? "Top 5 weight share."
            : `${top5Delta >= 0 ? "+" : ""}${formatNumber(top5Delta, 2)} pts vs benchmark.`,
      },
      {
        label: "Sector tilt",
        value: summary?.factor_sector_tilt_abs_pct,
        suffix: "%",
        detail: "Absolute sector deviation from the official TECH100 mix.",
      },
    ];
  };

  const holdingsConfig = (view, attributionKey) => {
    if (view === "signals") {
      return {
        note: "Signal view swaps in governance, momentum, and low-vol fields so the table stays readable without horizontal sprawl.",
        headers: ["Company", "Weight", "Active", "Gov.", "20D Mom", "60D Low Vol"],
        cells: (row) => [
          formatPct(row.model_weight_pct, 2),
          formatPct(row.active_weight_pct, 2),
          row.governance_score === null || row.governance_score === undefined
            ? "—"
            : formatNumber(row.governance_score, 1),
          formatPct(row.momentum_20d_pct, 2),
          formatPct(row.low_vol_60d_pct, 2),
        ],
      };
    }
    if (view === "attribution") {
      const field = bpFieldForKey(attributionKey);
      const label = (workspace.attributionKeyLabels || {})[attributionKey];
      return {
        note: `Attribution view keeps values in basis points and mirrors the selected ${label || attributionKey} window from the control bar.`,
        headers: ["Company", "Weight", "Active", "1D bp", `${label || "Selected"} bp`, "Gov."],
        cells: (row) => [
          formatPct(row.model_weight_pct, 2),
          formatPct(row.active_weight_pct, 2),
          row.contrib_1d_bp === null || row.contrib_1d_bp === undefined
            ? "—"
            : formatNumber(row.contrib_1d_bp, 2),
          row[field] === null || row[field] === undefined ? "—" : formatNumber(row[field], 2),
          row.governance_score === null || row.governance_score === undefined
            ? "—"
            : formatNumber(row.governance_score, 1),
        ],
      };
    }
    return {
      note: "Core view prioritizes benchmark weight, active weight, and contribution so the table reads as portfolio construction before it reads as raw data dump.",
      headers: ["Company", "Weight", "Active", "Benchmark", "YTD bp", "Quality"],
      cells: (row) => [
        formatPct(row.model_weight_pct, 2),
        formatPct(row.active_weight_pct, 2),
        formatPct(row.benchmark_weight_pct, 2),
        row.contrib_ytd_bp === null || row.contrib_ytd_bp === undefined
          ? "—"
          : formatNumber(row.contrib_ytd_bp, 2),
        escapeHtml(row.price_quality || "—"),
      ],
    };
  };

  const buildAttributionRankings = (positions, attributionKey) => {
    const field = bpFieldForKey(attributionKey);
    const label = (workspace.attributionKeyLabels || {})[attributionKey] || attributionKey;
    const ranked = [...(positions || [])]
      .filter((row) => row[field] !== null && row[field] !== undefined)
      .map((row) => ({
        ...row,
        metric_bp: Number(row[field]),
      }));
    ranked.sort((left, right) => right.metric_bp - left.metric_bp);
    return {
      label,
      top: ranked.slice(0, 5),
      bottom: [...ranked].sort((left, right) => left.metric_bp - right.metric_bp).slice(0, 5),
    };
  };

  const renderSummary = () => {
    const model = getModel(state.selectedModelCode);
    const benchmark = getModel(state.benchmarkCode);
    if (!model || !benchmark) return;

    const labelEl = document.getElementById("portfolio-selected-label");
    const descriptionEl = document.getElementById("portfolio-selected-description");
    const noteEl = document.getElementById("portfolio-selected-note");
    const asOfEl = document.getElementById("portfolio-summary-as-of");
    const rebalanceEl = document.getElementById("portfolio-summary-rebalance");
    const historyEl = document.getElementById("portfolio-history-start");
    const snapshotGrid = document.getElementById("portfolio-snapshot-grid");
    const deltaGrid = document.getElementById("portfolio-delta-grid");
    const factorGrid = document.getElementById("portfolio-factor-grid");
    const constraintList = document.getElementById("portfolio-constraint-list");
    const constraintEmpty = document.getElementById("portfolio-constraint-empty");

    if (labelEl) labelEl.textContent = model.label;
    if (descriptionEl) descriptionEl.textContent = model.description;
    if (noteEl) noteEl.textContent = model.model_note || model.description;
    if (asOfEl) asOfEl.innerHTML = `As of ${escapeHtml(formatAsOfDate(workspace.latestTradeDate))}`;
    if (rebalanceEl) {
      if (model.summary?.rebalance_date) {
        rebalanceEl.hidden = false;
        rebalanceEl.innerHTML = `Rebalanced ${escapeHtml(formatAsOfDate(model.summary.rebalance_date))}`;
      } else {
        rebalanceEl.hidden = true;
      }
    }
    if (historyEl) {
      if (workspace.historyStartDate) {
        historyEl.hidden = false;
        historyEl.innerHTML = `History from ${escapeHtml(formatAsOfDate(workspace.historyStartDate))}`;
      } else {
        historyEl.hidden = true;
      }
    }

    if (snapshotGrid) {
      snapshotGrid.innerHTML = buildSnapshotCards(model.summary)
        .map(
          (card) => `
            <div class="tech100-portfolio__stat-card">
              <span class="kpi-label">${escapeHtml(card.label)}</span>
              <strong class="kpi-value">${
                card.value === null || card.value === undefined ? "—" : `${formatNumber(card.value, card.decimals)}${escapeHtml(card.suffix)}`
              }</strong>
            </div>
          `
        )
        .join("");
    }

    if (deltaGrid) {
      deltaGrid.innerHTML = buildDeltaCards(model.summary, benchmark)
        .map(
          (card) => `
            <div class="tech100-portfolio__delta-card">
              <span class="kpi-label">${escapeHtml(card.label)}</span>
              <strong class="kpi-value">${
                card.value === null || card.value === undefined ? "—" : `${formatNumber(card.value, card.decimals)}${escapeHtml(card.suffix)}`
              }</strong>
              <span class="muted">${escapeHtml(card.detail)}</span>
            </div>
          `
        )
        .join("");
    }

    if (factorGrid) {
      factorGrid.innerHTML = buildFactorCards(model.summary, benchmark)
        .map(
          (card) => `
            <div class="tech100-portfolio__factor-card">
              <span class="kpi-label">${escapeHtml(card.label)}</span>
              <strong class="tech100-portfolio__factor-value">${
                card.value === null || card.value === undefined ? "—" : `${formatNumber(card.value, 2)}${escapeHtml(card.suffix)}`
              }</strong>
              <span class="muted">${escapeHtml(card.detail)}</span>
            </div>
          `
        )
        .join("");
    }

    if (constraintList && constraintEmpty) {
      const constraints = model.constraints || [];
      if (!constraints.length) {
        constraintList.hidden = true;
        constraintEmpty.hidden = false;
      } else {
        constraintList.hidden = false;
        constraintEmpty.hidden = true;
        constraintList.innerHTML = constraints
          .map(
            (row) =>
              `<li><strong>${escapeHtml(row.label)}:</strong> ${escapeHtml(row.value)}</li>`
          )
          .join("");
      }
    }
  };

  const renderModelSelection = () => {
    modelButtons.forEach((button) => {
      const isActive = button.dataset.modelCode === state.selectedModelCode;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    matrixButtons.forEach((button) => {
      button.classList.toggle("is-active", button.dataset.modelCode === state.selectedModelCode);
    });
    if (benchmarkSelect) {
      benchmarkSelect.value = state.benchmarkCode;
    }
  };

  const renderHoldings = () => {
    const model = getModel(state.selectedModelCode);
    const table = document.getElementById("portfolio-holdings-table");
    const noteEl = document.getElementById("portfolio-holdings-note");
    if (!model || !table) return;

    const config = holdingsConfig(state.holdingsView, state.attributionKey);
    const positions = [...(model.positions || [])].slice(0, 12);
    const thead = table.querySelector("thead");
    const tbody = document.getElementById("portfolio-holdings-body");
    if (noteEl) noteEl.textContent = config.note;

    if (thead) {
      thead.innerHTML = `
        <tr>
          ${config.headers
            .map((label, index) => {
              const align = index === 0 ? "text-left" : "text-right";
              return `<th class="${align}">${escapeHtml(label)}</th>`;
            })
            .join("")}
        </tr>
      `;
    }

    if (!tbody) return;
    if (!positions.length) {
      tbody.innerHTML = `<tr><td colspan="6" class="text-center muted">No portfolio holdings available.</td></tr>`;
      return;
    }

    tbody.innerHTML = positions
      .map((row) => {
        const cells = config.cells(row);
        return `
          <tr>
            <td class="text-left">
              <div class="tech100-portfolio__company-cell">
                <a class="text-link tech100-portfolio__company-ticker" href="/tech100/company/${encodeURIComponent(
                  row.ticker
                )}/">${escapeHtml(row.ticker)}</a>
                <div>
                  <a class="text-link" href="/tech100/company/${encodeURIComponent(
                    row.ticker
                  )}/">${escapeHtml(row.company_name || "—")}</a>
                  <div class="muted">${escapeHtml(row.sector || "Unclassified")}</div>
                </div>
              </div>
            </td>
            ${cells
              .map((value) => `<td class="text-right">${value}</td>`)
              .join("")}
          </tr>
        `;
      })
      .join("");
  };

  const renderAttribution = () => {
    const model = getModel(state.selectedModelCode);
    if (!model) return;
    const current = buildAttributionRankings(model.positions || [], state.attributionKey);
    const labelTop = document.getElementById("portfolio-attribution-label-top");
    const labelBottom = document.getElementById("portfolio-attribution-label-bottom");
    const topList = document.getElementById("portfolio-attribution-top");
    const bottomList = document.getElementById("portfolio-attribution-bottom");

    if (labelTop) labelTop.textContent = current.label;
    if (labelBottom) labelBottom.textContent = current.label;

    const renderList = (rows) => {
      if (!rows?.length) {
        return `<li class="is-empty">No attribution rows available.</li>`;
      }
      return rows
        .map(
          (row) => `
            <li>
              <div>
                <a class="text-link" href="/tech100/company/${encodeURIComponent(row.ticker)}/">${escapeHtml(
                  row.ticker
                )}</a>
                <span class="muted">${escapeHtml(row.sector || "Unclassified")}</span>
              </div>
              <strong>${formatBp(row.metric_bp, 2)}</strong>
            </li>
          `
        )
        .join("");
    };

    if (topList) topList.innerHTML = renderList(current.top);
    if (bottomList) bottomList.innerHTML = renderList(current.bottom);
  };

  const renderSector = () => {
    const model = getModel(state.selectedModelCode);
    const chartWrap = document.getElementById("portfolio-sector-chart-wrap");
    const tableWrap = document.getElementById("portfolio-sector-table-wrap");
    const emptyEl = document.getElementById("portfolio-sector-empty");
    const tbody = document.getElementById("portfolio-sector-body");
    const rows = [...(model?.sectors || [])];

    if (!rows.length) {
      chartWrap && (chartWrap.hidden = true);
      tableWrap && (tableWrap.hidden = true);
      emptyEl && (emptyEl.hidden = false);
      if (sectorChart) {
        sectorChart.destroy();
        sectorChart = null;
      }
      return;
    }

    chartWrap && (chartWrap.hidden = false);
    tableWrap && (tableWrap.hidden = false);
    emptyEl && (emptyEl.hidden = true);

    if (tbody) {
      tbody.innerHTML = rows
        .map(
          (row) => `
            <tr>
              <td class="text-left">${escapeHtml(row.sector)}</td>
              <td class="text-right">${formatPct(row.active_sector_weight_pct, 2)}</td>
              <td class="text-right">${
                row.contrib_ytd_bp === null || row.contrib_ytd_bp === undefined
                  ? "—"
                  : formatNumber(row.contrib_ytd_bp, 2)
              }</td>
            </tr>
          `
        )
        .join("");
    }

    if (!sectorChartCanvas || typeof Chart === "undefined") return;
    const sorted = [...rows].sort(
      (left, right) =>
        Math.abs(Number(right.active_sector_weight_pct || 0)) -
        Math.abs(Number(left.active_sector_weight_pct || 0))
    );
    if (sectorChart) sectorChart.destroy();
    sectorChart = new Chart(sectorChartCanvas.getContext("2d"), {
      type: "bar",
      data: {
        labels: sorted.map((row) => row.sector),
        datasets: [
          {
            label: "Active sector weight (%)",
            data: sorted.map((row) => Number(row.active_sector_weight_pct || 0)),
            backgroundColor: sorted.map((row) =>
              Number(row.active_sector_weight_pct || 0) >= 0
                ? withAlpha("#183153", 0.82)
                : withAlpha("#b44a3c", 0.82)
            ),
            borderRadius: 8,
          },
        ],
      },
      options: {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: {
            ticks: {
              callback: (value) => `${formatNumber(value, 1)}%`,
            },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => `${context.dataset.label}: ${formatPct(context.raw, 2)}`,
            },
          },
        },
      },
    });
  };

  const syncControlState = () => {
    rangeButtons.forEach((button) =>
      button.classList.toggle("is-active", button.dataset.range === state.rangeKey)
    );
    metricButtons.forEach((button) =>
      button.classList.toggle("is-active", button.dataset.metric === state.metricKey)
    );
    viewButtons.forEach((button) =>
      button.classList.toggle("is-active", button.dataset.view === state.viewMode)
    );
    holdingsViewButtons.forEach((button) =>
      button.classList.toggle("is-active", button.dataset.holdingsView === state.holdingsView)
    );
    if (attributionSelect) attributionSelect.value = state.attributionKey;
  };

  const buildLegend = (items) => {
    const legendEl = document.getElementById("tech100-portfolio-legend");
    if (!legendEl) return;
    legendEl.innerHTML = items
      .map(
        (item) => `
          <span class="tech100-portfolio__legend-item ${item.muted ? "is-muted" : ""}">
            <span class="tech100-portfolio__legend-dot" style="background:${escapeHtml(
              item.color
            )}"></span>
            ${escapeHtml(item.label)}
          </span>
        `
      )
      .join("");
  };

  const buildChartConfig = () => {
    ensureValidState();
    const selected = getModel(state.selectedModelCode);
    const benchmark = getModel(state.benchmarkCode);
    const effectiveActive =
      state.viewMode === "active" && selected && benchmark && state.selectedModelCode !== state.benchmarkCode;
    const seriesByModel = workspace.seriesByModel || {};

    if (!selected || !benchmark) {
      return {
        labels: [],
        datasets: [],
        title: "Portfolio comparison",
        caption: "No portfolio series are available for the current selection.",
        legend: [],
      };
    }

    if (state.metricKey === "relative") {
      if (effectiveActive) {
        const selectedSeries = toRelativeSeries(
          filterByRange(seriesByModel[state.selectedModelCode] || [], state.rangeKey)
        );
        const benchmarkSeries = toRelativeSeries(
          filterByRange(seriesByModel[state.benchmarkCode] || [], state.rangeKey)
        );
        const { labels, aligned } = alignSeries(
          {
            selected: selectedSeries,
            benchmark: benchmarkSeries,
          },
          ["selected", "benchmark"]
        );
        return {
          labels,
          datasets: [
            {
              label: `${selected.short_label} active return`,
              data: labels.map((date, index) => {
                const selectedValue = aligned.selected[index];
                const benchmarkValue = aligned.benchmark[index];
                return selectedValue === null || benchmarkValue === null
                  ? null
                  : selectedValue - benchmarkValue;
              }),
              borderColor: selected.color,
              backgroundColor: withAlpha(selected.color, 0.18),
              borderWidth: 3,
              pointRadius: 0,
              tension: 0.18,
              fill: true,
            },
          ],
          title: `Active return versus ${benchmark.short_label}`,
          caption: `${selected.label} is shown as cumulative return spread versus ${benchmark.label} across the selected range.`,
          legend: [{ label: `${selected.short_label} vs ${benchmark.short_label}`, color: selected.color }],
        };
      }

      const codes = workspace.modelOrder || [];
      const seriesMap = {};
      codes.forEach((code) => {
        seriesMap[code] = toRelativeSeries(filterByRange(seriesByModel[code] || [], state.rangeKey));
      });
      const { labels, aligned } = alignSeries(seriesMap, codes);
      return {
        labels,
        datasets: codes.map((code) => {
          const model = getModel(code);
          return {
            label: model.short_label,
            data: aligned[code],
            borderColor:
              code === state.selectedModelCode ? model.color : withAlpha(model.color, 0.72),
            backgroundColor: withAlpha(model.color, code === state.selectedModelCode ? 0.18 : 0.08),
            borderWidth: code === state.selectedModelCode ? 3 : code === state.benchmarkCode ? 2.2 : 1.5,
            borderDash: code === state.benchmarkCode && code !== state.selectedModelCode ? [7, 4] : undefined,
            pointRadius: 0,
            tension: 0.18,
            spanGaps: false,
          };
        }),
        title: "Relative performance since the range start",
        caption: "All supported models are rebased to the selected range so differences read as cumulative return, not raw index points.",
        legend: codes.map((code) => {
          const model = getModel(code);
          return {
            label:
              code === state.selectedModelCode
                ? `${model.short_label} (selected)`
                : code === state.benchmarkCode
                ? `${model.short_label} (benchmark)`
                : model.short_label,
            color: model.color,
            muted: code !== state.selectedModelCode && code !== state.benchmarkCode,
          };
        }),
      };
    }

    const title =
      state.metricKey === "volatility"
        ? "Rolling 20D volatility"
        : "Drawdown history";
    const caption =
      state.metricKey === "volatility"
        ? "Volatility is annualized and limited to the supported rolling 20-day measure published by the backend."
        : "Drawdown is shown as the current decline from the running peak within the selected range.";
    const selectedSeries = toMetricSeries(
      filterByRange(seriesByModel[state.selectedModelCode] || [], state.rangeKey),
      state.metricKey
    );
    const benchmarkSeries = toMetricSeries(
      filterByRange(seriesByModel[state.benchmarkCode] || [], state.rangeKey),
      state.metricKey
    );

    if (effectiveActive) {
      const { labels, aligned } = alignSeries(
        {
          selected: selectedSeries,
          benchmark: benchmarkSeries,
        },
        ["selected", "benchmark"]
      );
      return {
        labels,
        datasets: [
          {
            label: `${selected.short_label} spread`,
            data: labels.map((date, index) => {
              const selectedValue = aligned.selected[index];
              const benchmarkValue = aligned.benchmark[index];
              return selectedValue === null || benchmarkValue === null
                ? null
                : selectedValue - benchmarkValue;
            }),
            borderColor: selected.color,
            backgroundColor: withAlpha(selected.color, 0.18),
            borderWidth: 3,
            pointRadius: 0,
            tension: 0.18,
            fill: true,
          },
        ],
        title: `${title} spread versus ${benchmark.short_label}`,
        caption: `${caption} The active view subtracts ${benchmark.label} from ${selected.label}.`,
        legend: [{ label: `${selected.short_label} vs ${benchmark.short_label}`, color: selected.color }],
      };
    }

    const codes = state.selectedModelCode === state.benchmarkCode
      ? [state.selectedModelCode]
      : [state.selectedModelCode, state.benchmarkCode];
    const { labels, aligned } = alignSeries(
      {
        [codes[0]]: selectedSeries,
        ...(codes[1] ? { [codes[1]]: benchmarkSeries } : {}),
      },
      codes
    );
    return {
      labels,
      datasets: codes.map((code) => {
        const model = getModel(code);
        return {
          label:
            code === state.selectedModelCode
              ? `${model.short_label} (selected)`
              : `${model.short_label} (benchmark)`,
          data: aligned[code],
          borderColor: model.color,
          backgroundColor: withAlpha(model.color, 0.16),
          borderWidth: code === state.selectedModelCode ? 3 : 2.2,
          borderDash: code === state.selectedModelCode ? undefined : [7, 4],
          pointRadius: 0,
          tension: 0.18,
          spanGaps: false,
        };
      }),
      title,
      caption,
      legend: codes.map((code) => {
        const model = getModel(code);
        return {
          label:
            code === state.selectedModelCode
              ? `${model.short_label} (selected)`
              : `${model.short_label} (benchmark)`,
          color: model.color,
        };
      }),
    };
  };

  const initCharts = () => {
    if (!mainChartCanvas || typeof Chart === "undefined") return;
    ensureChartDefaults();
    mainChart = new Chart(mainChartCanvas.getContext("2d"), {
      type: "line",
      data: { labels: [], datasets: [] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x: {
            ticks: {
              color: "rgba(223, 234, 252, 0.84)",
              maxTicksLimit: 8,
              autoSkip: true,
            },
            grid: {
              color: "rgba(164, 181, 212, 0.12)",
            },
          },
          y: {
            ticks: {
              color: "rgba(223, 234, 252, 0.84)",
              callback: (value) => formatPct(value, 1),
            },
            grid: {
              color: "rgba(164, 181, 212, 0.12)",
            },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "rgba(9, 16, 28, 0.96)",
            borderColor: "rgba(164, 181, 212, 0.22)",
            borderWidth: 1,
            titleColor: "#f7fbff",
            bodyColor: "#f7fbff",
            callbacks: {
              label: (context) => `${context.dataset.label}: ${formatPct(context.raw, 2)}`,
            },
          },
        },
      },
    });
  };

  const renderChart = () => {
    const titleEl = document.getElementById("portfolio-chart-title");
    const captionEl = document.getElementById("portfolio-chart-caption");
    const config = buildChartConfig();

    if (titleEl) titleEl.textContent = config.title;
    if (captionEl) captionEl.textContent = config.caption;
    buildLegend(config.legend || []);

    if (!mainChart) return;
    mainChart.data = {
      labels: config.labels,
      datasets: config.datasets,
    };
    setAxisFormatter(mainChart, state.rangeKey, config.labels);
    mainChart.update();
  };

  const updateUrl = () => {
    if (!window.history?.replaceState) return;
    const url = new URL(window.location.href);
    url.searchParams.set("model", state.selectedModelCode);
    url.searchParams.set("benchmark", state.benchmarkCode);
    url.searchParams.set("range", state.rangeKey);
    url.searchParams.set("metric", state.metricKey);
    url.searchParams.set("view", state.viewMode);
    url.searchParams.set("window", state.attributionKey);
    url.searchParams.set("holdings", state.holdingsView);
    window.history.replaceState({}, "", `${url.pathname}?${url.searchParams.toString()}#comparison`);
  };

  const renderAll = () => {
    ensureValidState();
    renderModelSelection();
    syncControlState();
    renderSummary();
    renderChart();
    renderHoldings();
    renderAttribution();
    renderSector();
    updateUrl();
    dateFormat.applyDateFormatting?.();
  };

  modelButtons.forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      if (!button.dataset.modelCode) return;
      state.selectedModelCode = button.dataset.modelCode;
      renderAll();
    });
  });

  matrixButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (!button.dataset.modelCode) return;
      state.selectedModelCode = button.dataset.modelCode;
      renderAll();
    });
  });

  rangeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (!button.dataset.range) return;
      state.rangeKey = button.dataset.range;
      renderAll();
    });
  });

  metricButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (!button.dataset.metric) return;
      state.metricKey = button.dataset.metric;
      renderAll();
    });
  });

  viewButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (!button.dataset.view) return;
      state.viewMode = button.dataset.view;
      renderAll();
    });
  });

  holdingsViewButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (!button.dataset.holdingsView) return;
      state.holdingsView = button.dataset.holdingsView;
      renderAll();
    });
  });

  benchmarkSelect?.addEventListener("change", () => {
    state.benchmarkCode = benchmarkSelect.value;
    renderAll();
  });

  attributionSelect?.addEventListener("change", () => {
    state.attributionKey = attributionSelect.value;
    renderAll();
  });

  initCharts();
  renderAll();
})();

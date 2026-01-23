(() => {
  const root = document.getElementById("tech100-company-root");
  if (!root) return;

  const ticker = (root.dataset.ticker || "").trim().toUpperCase();
  if (!ticker) return;

  const summaryUrl = `/api/tech100/company/${ticker}/summary`;
  const historyUrl = `/api/tech100/company/${ticker}/history`;
  const seriesUrl = (companyTicker, metric, range) =>
    `/api/tech100/company/${companyTicker}/series?metric=${encodeURIComponent(metric)}&baseline=top25_avg&range=${encodeURIComponent(range)}`;
  const companiesUrl = "/api/tech100/companies";

  const metricSelect = document.getElementById("company-metric");
  const rangeButtons = Array.from(document.querySelectorAll("[data-range]"));
  const historyBody = document.getElementById("company-history-body");
  const sortButton = document.getElementById("history-sort");
  const chartCanvas = document.getElementById("company-series-chart");
  const companySelector = document.getElementById("company-selector");
  const compareSelector = document.getElementById("company-compare");
  const companyOptions = document.getElementById("tech100-company-options");
  const chartStatus = document.querySelector("[data-chart-status]");
  const historyStatus = document.querySelector("[data-history-status]");
  const retryChartButton = document.querySelector("[data-retry-chart]");
  const retryHistoryButton = document.querySelector("[data-retry-history]");
  const compareLegend = document.querySelector("[data-compare-legend]");
  const compareLabel = document.querySelector("[data-compare-label]");

  const formatScore = (value) => {
    if (value === null || value === undefined || Number.isNaN(value)) return "—";
    return Number(value).toFixed(1);
  };

  const formatWeight = (value) => {
    if (value === null || value === undefined || Number.isNaN(value)) return "—";
    const num = Number(value);
    const decimals = Math.abs(num) >= 10 ? 1 : 2;
    return `${num.toFixed(decimals)}%`;
  };

  const formatDate = (value) => {
    const formatter = window.SCDateFormat;
    if (formatter?.formatAsOfDate) {
      return formatter.formatAsOfDate(value);
    }
    return value || "—";
  };

  const fetchWithTimeout = async (url, options = {}, timeoutMs = 8000) => {
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), timeoutMs);
    try {
      const resp = await fetch(url, { ...options, signal: controller.signal });
      return resp;
    } finally {
      window.clearTimeout(timer);
    }
  };

  const describeFetchError = (err) => {
    if (err?.name === "AbortError") {
      return "Request timed out";
    }
    const statusCode = err?.message?.match(/\d{3}/)?.[0];
    if (statusCode) {
      return `HTTP ${statusCode}`;
    }
    return "Request failed";
  };

  const setText = (selector, value) => {
    const el = document.querySelector(selector);
    if (el) el.textContent = value ?? "—";
  };

  const setBadge = (selector, value) => {
    const el = document.querySelector(selector);
    if (el) el.textContent = value ?? "—";
  };

  const setStatus = (container, message, showRetry, retryButton) => {
    if (!container) return;
    if (!message) {
      container.hidden = true;
      return;
    }
    const textEl = container.querySelector(".tech100-company__status-text");
    if (textEl) textEl.textContent = message;
    if (retryButton) retryButton.hidden = !showRetry;
    container.hidden = false;
  };

  const updateSummary = async () => {
    try {
      const resp = await fetchWithTimeout(summaryUrl);
      if (!resp.ok) return;
      const data = await resp.json();
      if (!data) return;
      const scores = data.latest_scores || {};
      setText("[data-company-name]", data.company_name || data.ticker);
      setText("[data-company-sector]", data.sector || "—");
      setText("[data-company-latest-date]", formatDate(data.latest_date));
      setBadge("[data-company-latest-composite]", formatScore(scores.composite));
    } catch (err) {
      console.warn("Company summary failed", err);
    }
  };

  const state = {
    metric: metricSelect?.value || "composite",
    range: "6m",
    sortDesc: true,
    chart: null,
    series: [],
    compareSeries: [],
    compareTicker: null,
    compareLabel: null,
    companies: [],
    companyLookup: new Map(),
    labelLookup: new Map(),
    history: [],
  };

  const renderHistory = () => {
    if (!historyBody) return;
    historyBody.innerHTML = "";
    if (!state.history?.length) {
      historyBody.innerHTML = '<tr><td colspan="9" class="muted">No history available.</td></tr>';
      return;
    }

    const rows = [...state.history].sort((a, b) => {
      const aDate = a.date || "";
      const bDate = b.date || "";
      if (aDate === bDate) return 0;
      return state.sortDesc ? (aDate < bDate ? 1 : -1) : (aDate < bDate ? -1 : 1);
    });

    const fragment = document.createDocumentFragment();
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="text-left" data-date-format="as-of" data-date-value="${row.date || ""}">${formatDate(row.date)}</td>
        <td class="text-center">${row.rank ?? "—"}</td>
        <td class="text-right">${formatWeight(row.weight)}</td>
        <td class="text-right">${formatScore(row.composite)}</td>
        <td class="text-right">${formatScore(row.transparency)}</td>
        <td class="text-right">${formatScore(row.ethical_principles)}</td>
        <td class="text-right">${formatScore(row.governance_structure)}</td>
        <td class="text-right">${formatScore(row.regulatory_alignment)}</td>
        <td class="text-right">${formatScore(row.stakeholder_engagement)}</td>
      `;
      fragment.appendChild(tr);
    });
    historyBody.appendChild(fragment);
    window.SCDateFormat?.applyDateFormatting?.(historyBody);
  };

  const loadHistory = async () => {
    setStatus(historyStatus, "Loading history…", false, retryHistoryButton);
    try {
      const resp = await fetchWithTimeout(historyUrl);
      if (!resp.ok) throw new Error(`history ${resp.status}`);
      const data = await resp.json();
      state.history = data.history || [];
      renderHistory();
      setStatus(historyStatus, null, false, retryHistoryButton);
    } catch (err) {
      console.warn("Company history failed", err);
      if (historyBody) {
        historyBody.innerHTML = '<tr><td colspan="9" class="muted">History unavailable.</td></tr>';
      }
      const statusLabel = describeFetchError(err);
      setStatus(
        historyStatus,
        `Couldn't load history (${statusLabel}).`,
        true,
        retryHistoryButton
      );
    }
  };

  const buildChart = (labels, datasets, series) => {
    if (!chartCanvas || !window.Chart) return null;
    return new Chart(chartCanvas.getContext("2d"), {
      type: "line",
      data: {
        labels,
        datasets,
      },
      options: {
        responsive: true,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: (items) => formatDate(items[0]?.label),
              label: (ctx) => {
                const label = ctx.dataset.label || "";
                return `${label}: ${formatScore(ctx.parsed.y)}`;
              },
              afterBody: (items) => {
                const idx = items[0]?.dataIndex ?? 0;
                const point = series[idx];
                if (!point) return "";
                const delta = point.delta;
                return `Delta: ${formatScore(delta)}`;
              },
            },
          },
        },
        scales: {
          x: {
            ticks: {
              callback: (value) => formatDate(labels[value]),
              maxTicksLimit: 8,
            },
          },
          y: {
            ticks: {
              callback: (val) => formatScore(val),
            },
          },
        },
      },
    });
  };

  const updateChart = (series, compareSeries) => {
    if (!chartCanvas || !window.Chart) return;
    const labels = series.map((point) => point.date);
    const baselineValues = series.map((point) => point.baseline);
    const compareMap = new Map(
      (compareSeries || []).map((point) => [point.date, point.company])
    );

    const datasets = [
      {
        label: "Company",
        data: series.map((point) => point.company),
        borderColor: "#1f5fbf",
        backgroundColor: "rgba(31, 95, 191, 0.1)",
        tension: 0.25,
      },
      {
        label: "Top 25 average",
        data: baselineValues,
        borderColor: "#9aa4b2",
        backgroundColor: "rgba(154, 164, 178, 0.15)",
        tension: 0.25,
      },
    ];

    if (state.compareTicker) {
      datasets.push({
        label: state.compareLabel || "Compare",
        data: labels.map((date) => compareMap.get(date) ?? null),
        borderColor: "#c0841f",
        backgroundColor: "rgba(192, 132, 31, 0.12)",
        tension: 0.25,
      });
    }

    if (!state.chart) {
      state.chart = buildChart(labels, datasets, series);
      return;
    }

    state.chart.data.labels = labels;
    state.chart.data.datasets = datasets;
    state.chart.update();
  };

  const fetchSeries = async (companyTicker, metric, range) => {
    const resp = await fetchWithTimeout(seriesUrl(companyTicker, metric, range));
    if (!resp.ok) throw new Error(`series ${resp.status}`);
    const data = await resp.json();
    return data.series || [];
  };

  const loadSeries = async () => {
    setStatus(chartStatus, "Loading series…", false, retryChartButton);
    try {
      const [series, compareSeries] = await Promise.all([
        fetchSeries(ticker, state.metric, state.range),
        state.compareTicker ? fetchSeries(state.compareTicker, state.metric, state.range) : Promise.resolve([]),
      ]);
      state.series = series;
      state.compareSeries = compareSeries;
      updateChart(series, compareSeries);
      setStatus(chartStatus, null, false, retryChartButton);
    } catch (err) {
      console.warn("Company series failed", err);
      const statusLabel = describeFetchError(err);
      setStatus(
        chartStatus,
        `Couldn't load series (${statusLabel}).`,
        true,
        retryChartButton
      );
    }
  };

  const formatCompanyLabel = (company) => {
    const name = company.company_name || company.ticker || "";
    const symbol = (company.ticker || "").toUpperCase();
    return symbol ? `${name} (${symbol})` : name;
  };

  const extractTicker = (value) => {
    if (!value) return "";
    const trimmed = value.trim();
    if (!trimmed) return "";
    const match = trimmed.match(/\(([^)]+)\)\s*$/);
    if (match) return match[1].trim().toUpperCase();
    if (state.labelLookup.has(trimmed.toLowerCase())) {
      return state.labelLookup.get(trimmed.toLowerCase());
    }
    return trimmed.toUpperCase();
  };

  const updateCompareLegend = () => {
    if (!compareLegend || !compareLabel) return;
    if (state.compareTicker) {
      compareLegend.hidden = false;
      compareLabel.textContent = state.compareLabel || state.compareTicker;
    } else {
      compareLegend.hidden = true;
    }
  };

  const loadCompanies = async () => {
    if (!companyOptions) return;
    try {
      const resp = await fetchWithTimeout(companiesUrl);
      if (!resp.ok) throw new Error(`companies ${resp.status}`);
      const data = await resp.json();
      state.companies = data.companies || [];
      companyOptions.innerHTML = "";
      state.companyLookup.clear();
      state.labelLookup.clear();
      const fragment = document.createDocumentFragment();
      state.companies.forEach((company) => {
        const symbol = (company.ticker || "").toUpperCase();
        const label = formatCompanyLabel(company);
        if (!symbol) return;
        state.companyLookup.set(symbol, label);
        state.labelLookup.set(label.toLowerCase(), symbol);
        const option = document.createElement("option");
        option.value = label;
        fragment.appendChild(option);
      });
      companyOptions.appendChild(fragment);
    } catch (err) {
      console.warn("Company list failed", err);
    }
  };

  const handleCompanyChange = (event) => {
    const nextTicker = extractTicker(event.target.value);
    if (!nextTicker || nextTicker === ticker) return;
    window.location.assign(`/tech100/company/${nextTicker}/`);
  };

  const handleCompareChange = async (event) => {
    const nextTicker = extractTicker(event.target.value);
    if (!nextTicker || nextTicker === ticker) {
      state.compareTicker = null;
      state.compareLabel = null;
      updateCompareLegend();
      loadSeries();
      return;
    }
    state.compareTicker = nextTicker;
    state.compareLabel = state.companyLookup.get(nextTicker) || nextTicker;
    updateCompareLegend();
    loadSeries();
  };

  metricSelect?.addEventListener("change", (event) => {
    state.metric = event.target.value;
    loadSeries();
  });

  rangeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      rangeButtons.forEach((el) => el.classList.remove("is-active"));
      button.classList.add("is-active");
      state.range = button.dataset.range || "6m";
      loadSeries();
    });
  });

  sortButton?.addEventListener("click", () => {
    state.sortDesc = !state.sortDesc;
    renderHistory();
  });

  companySelector?.addEventListener("change", handleCompanyChange);
  compareSelector?.addEventListener("change", handleCompareChange);
  retryChartButton?.addEventListener("click", loadSeries);
  retryHistoryButton?.addEventListener("click", loadHistory);

  updateSummary();
  loadCompanies();
  loadHistory();
  loadSeries();
})();

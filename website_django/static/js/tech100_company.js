(() => {
  const root = document.getElementById("tech100-company-root");
  if (!root) return;

  const ticker = (root.dataset.ticker || "").trim().toUpperCase();
  if (!ticker) return;

  const METRICS = [
    { key: "composite", label: "Composite AI Gov & Ethics" },
    { key: "transparency", label: "Transparency" },
    { key: "ethical_principles", label: "Ethical Principles" },
    { key: "governance_structure", label: "Governance Structure" },
    { key: "regulatory_alignment", label: "Regulatory Alignment" },
    { key: "stakeholder_engagement", label: "Stakeholder Engagement" },
  ];
  const METRIC_KEYS = METRICS.map((metric) => metric.key);

  const bundleUrl = (compare, includeCompanies) => {
    const params = new URLSearchParams({
      metrics: METRIC_KEYS.join(","),
    });
    if (compare) params.set("compare", compare);
    if (includeCompanies) params.set("companies", "1");
    return `/api/tech100/company/${ticker}/bundle?${params.toString()}`;
  };

  const historyBody = document.getElementById("company-history-body");
  const sortButton = document.getElementById("history-sort");
  const companySelector = document.getElementById("company-selector");
  const compareSelector = document.getElementById("company-compare");
  const companyOptions = document.getElementById("tech100-company-options");
  const compareLegend = document.querySelector("[data-compare-legend]");
  const compareLabel = document.querySelector("[data-compare-label]");
  const historyCountEl = document.querySelector("[data-history-count]");
  const historyUpdatedEl = document.querySelector("[data-history-updated]");
  const summaryBody = document.getElementById("company-summary-body");
  const summaryTextEl = document.querySelector("[data-company-summary]");

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

  const fetchWithTimeout = async (url, options = {}, timeoutMs = 10000) => {
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

  const chartStatuses = new Map();
  document.querySelectorAll("[data-chart-status]").forEach((el) => {
    const key = el.getAttribute("data-chart-status");
    if (key) chartStatuses.set(key, el);
  });
  const historyStatus = document.querySelector("[data-history-status]");
  const summaryStatus = document.querySelector("[data-summary-status]");
  const retryChartButtons = Array.from(document.querySelectorAll("[data-retry-chart]"));
  const retryHistoryButton = document.querySelector("[data-retry-history]");
  const retrySummaryButton = document.querySelector("[data-retry-summary]");

  const state = {
    sortDesc: true,
    charts: new Map(),
    compareTicker: null,
    compareLabel: null,
    companies: [],
    companyLookup: new Map(),
    labelLookup: new Map(),
    history: [],
    summaryHistory: [],
  };

  const applySummary = (summary) => {
    if (!summary) return;
    const scores = summary.latest_scores || {};
    setText("[data-company-name]", summary.company_name || summary.ticker);
    setText("[data-company-sector]", summary.sector || "—");
    setText("[data-company-latest-date]", formatDate(summary.latest_date));
    setBadge("[data-company-latest-composite]", formatScore(scores.composite));
    if (summaryTextEl) {
      summaryTextEl.textContent = summary.summary || "Summary not available yet.";
    }
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

  const truncateText = (text, maxLen = 280) => {
    if (!text) return { short: "—", full: "" };
    if (text.length <= maxLen) return { short: text, full: text };
    return { short: `${text.slice(0, maxLen).trim()}…`, full: text };
  };

  const renderSummaryHistory = () => {
    if (!summaryBody) return;
    summaryBody.innerHTML = "";
    if (!state.summaryHistory?.length) {
      summaryBody.innerHTML = '<tr><td colspan="5" class="muted">No summaries available.</td></tr>';
      return;
    }
    const fragment = document.createDocumentFragment();
    state.summaryHistory.forEach((row) => {
      const tr = document.createElement("tr");
      const text = row.summary || "";
      const { short, full } = truncateText(text, 300);
      const isTruncated = full.length > short.length;
      const summaryCell = document.createElement("td");
      summaryCell.className = "summary-cell";
      const summarySpan = document.createElement("span");
      summarySpan.className = "summary-text";
      summarySpan.textContent = short;
      summarySpan.dataset.full = full;
      summarySpan.dataset.short = short;
      summaryCell.appendChild(summarySpan);
      if (isTruncated) {
        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "summary-toggle text-link";
        toggle.textContent = "Expand";
        summaryCell.appendChild(toggle);
      }
      tr.innerHTML = `
        <td class="text-left" data-date-format="as-of" data-date-value="${row.date || ""}">${formatDate(row.date)}</td>
        <td class="text-center">${row.rank ?? "—"}</td>
        <td class="text-right">${formatWeight(row.weight)}</td>
        <td class="text-right">${formatScore(row.composite)}</td>
      `;
      tr.appendChild(summaryCell);
      fragment.appendChild(tr);
    });
    summaryBody.appendChild(fragment);
    window.SCDateFormat?.applyDateFormatting?.(summaryBody);
  };

  const updateHistoryMeta = (history, latestDate) => {
    if (historyCountEl) historyCountEl.textContent = history?.length ? String(history.length) : "0";
    if (historyUpdatedEl) {
      const value = latestDate || history?.[0]?.date;
      historyUpdatedEl.textContent = formatDate(value);
    }
  };

  const buildChart = (canvas, labels, datasets, series) => {
    if (!canvas || !window.Chart) return null;
    return new Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels,
        datasets,
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
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

  const updateChart = (metricKey, series) => {
    const canvas = document.getElementById(`company-chart-${metricKey}`);
    if (!canvas || !window.Chart) return;
    const labels = series.map((point) => point.date);
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
        data: series.map((point) => point.baseline),
        borderColor: "#9aa4b2",
        backgroundColor: "rgba(154, 164, 178, 0.15)",
        tension: 0.25,
      },
    ];
    if (state.compareTicker) {
      datasets.push({
        label: state.compareLabel || "Compare",
        data: series.map((point) => point.compare ?? null),
        borderColor: "#c0841f",
        backgroundColor: "rgba(192, 132, 31, 0.12)",
        tension: 0.25,
      });
    }
    if (!state.charts.has(metricKey)) {
      state.charts.set(metricKey, buildChart(canvas, labels, datasets, series));
      return;
    }
    const chart = state.charts.get(metricKey);
    chart.data.labels = labels;
    chart.data.datasets = datasets;
    chart.update();
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

  const populateCompanies = (companies) => {
    if (!companyOptions) return;
    if (!Array.isArray(companies)) return;
    state.companies = companies;
    companyOptions.innerHTML = "";
    state.companyLookup.clear();
    state.labelLookup.clear();
    const fragment = document.createDocumentFragment();
    companies.forEach((company) => {
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
  };

  const setChartStatuses = (message, showRetry) => {
    chartStatuses.forEach((container) => {
      setStatus(container, message, showRetry, container.querySelector("[data-retry-chart]"));
    });
  };

  const loadBundle = async (includeCompanies = false) => {
    setChartStatuses("Loading charts…", false);
    setStatus(historyStatus, "Loading history…", false, retryHistoryButton);
    setStatus(summaryStatus, "Loading summaries…", false, retrySummaryButton);
    try {
      const resp = await fetchWithTimeout(bundleUrl(state.compareTicker, includeCompanies));
      if (!resp.ok) throw new Error(`bundle ${resp.status}`);
      const data = await resp.json();
      applySummary(data.summary);
      state.history = data.history || [];
      state.summaryHistory = data.summary_history || [];
      updateHistoryMeta(state.history, data.summary?.latest_date);
      renderHistory();
      renderSummaryHistory();

      const seriesByMetric = data.series || {};
      METRIC_KEYS.forEach((metricKey) => {
        const series = seriesByMetric[metricKey] || [];
        updateChart(metricKey, series);
      });

      if (includeCompanies && data.companies) {
        populateCompanies(data.companies);
      }
      setChartStatuses(null, false);
      setStatus(historyStatus, null, false, retryHistoryButton);
      setStatus(summaryStatus, null, false, retrySummaryButton);
    } catch (err) {
      console.warn("Company bundle failed", err);
      const statusLabel = describeFetchError(err);
      setChartStatuses(`Couldn't load charts (${statusLabel}).`, true);
      setStatus(historyStatus, `Couldn't load history (${statusLabel}).`, true, retryHistoryButton);
      setStatus(summaryStatus, `Couldn't load summaries (${statusLabel}).`, true, retrySummaryButton);
    }
  };

  const handleCompanyChange = (event) => {
    const nextTicker = extractTicker(event.target.value);
    if (!nextTicker || nextTicker === ticker) return;
    window.location.assign(`/tech100/company/${nextTicker}/`);
  };

  const handleCompareChange = (event) => {
    const nextTicker = extractTicker(event.target.value);
    if (!nextTicker || nextTicker === ticker) {
      state.compareTicker = null;
      state.compareLabel = null;
      updateCompareLegend();
      loadBundle(false);
      return;
    }
    state.compareTicker = nextTicker;
    state.compareLabel = state.companyLookup.get(nextTicker) || nextTicker;
    updateCompareLegend();
    loadBundle(false);
  };

  sortButton?.addEventListener("click", () => {
    state.sortDesc = !state.sortDesc;
    renderHistory();
  });

  summaryBody?.addEventListener("click", (event) => {
    const target = event.target;
    if (!target || !target.classList.contains("summary-toggle")) return;
    const cell = target.closest(".summary-cell");
    const textEl = cell?.querySelector(".summary-text");
    if (!textEl) return;
    const isExpanded = textEl.classList.toggle("is-expanded");
    textEl.textContent = isExpanded ? textEl.dataset.full : textEl.dataset.short;
    target.textContent = isExpanded ? "Collapse" : "Expand";
  });

  companySelector?.addEventListener("change", handleCompanyChange);
  compareSelector?.addEventListener("change", handleCompareChange);
  retryChartButtons.forEach((btn) => btn.addEventListener("click", () => loadBundle(false)));
  retryHistoryButton?.addEventListener("click", () => loadBundle(false));
  retrySummaryButton?.addEventListener("click", () => loadBundle(false));

  loadBundle(true);
})();

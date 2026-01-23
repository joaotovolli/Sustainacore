(() => {
  const root = document.getElementById("tech100-company-root");
  if (!root) return;

  const ticker = (root.dataset.ticker || "").trim().toUpperCase();
  if (!ticker) return;

  const summaryUrl = `/api/tech100/company/${ticker}/summary`;
  const historyUrl = `/api/tech100/company/${ticker}/history`;
  const seriesUrl = (metric, range) =>
    `/api/tech100/company/${ticker}/series?metric=${encodeURIComponent(metric)}&baseline=top25_avg&range=${encodeURIComponent(range)}`;

  const metricSelect = document.getElementById("company-metric");
  const rangeButtons = Array.from(document.querySelectorAll("[data-range]"));
  const historyBody = document.getElementById("company-history-body");
  const sortButton = document.getElementById("history-sort");
  const chartCanvas = document.getElementById("company-series-chart");

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

  const setText = (selector, value) => {
    const el = document.querySelector(selector);
    if (el) el.textContent = value ?? "—";
  };

  const setBadge = (selector, value) => {
    const el = document.querySelector(selector);
    if (el) el.textContent = value ?? "—";
  };

  const updateSummary = async () => {
    try {
      const resp = await fetch(summaryUrl);
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
    history: [],
  };

  const renderHistory = () => {
    if (!historyBody) return;
    historyBody.innerHTML = "";
    if (!state.history.length) {
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
    try {
      const resp = await fetch(historyUrl);
      if (!resp.ok) throw new Error(`history ${resp.status}`);
      const data = await resp.json();
      state.history = data.history || [];
      renderHistory();
    } catch (err) {
      console.warn("Company history failed", err);
      if (historyBody) {
        historyBody.innerHTML = '<tr><td colspan="9" class="muted">History unavailable.</td></tr>';
      }
    }
  };

  const buildChart = (series) => {
    if (!chartCanvas || !window.Chart) return null;
    const labels = series.map((point) => point.date);
    const companyValues = series.map((point) => point.company);
    const baselineValues = series.map((point) => point.baseline);

    return new Chart(chartCanvas.getContext("2d"), {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Company",
            data: companyValues,
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
        ],
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

  const updateChart = (series) => {
    if (!chartCanvas || !window.Chart) return;
    if (!state.chart) {
      state.chart = buildChart(series);
      return;
    }
    state.chart.data.labels = series.map((point) => point.date);
    state.chart.data.datasets[0].data = series.map((point) => point.company);
    state.chart.data.datasets[1].data = series.map((point) => point.baseline);
    state.chart.update();
  };

  const loadSeries = async () => {
    const range = state.range;
    const metric = state.metric;
    try {
      const resp = await fetch(seriesUrl(metric, range));
      if (!resp.ok) throw new Error(`series ${resp.status}`);
      const data = await resp.json();
      state.series = data.series || [];
      updateChart(state.series);
    } catch (err) {
      console.warn("Company series failed", err);
    }
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

  updateSummary();
  loadHistory();
  loadSeries();
})();

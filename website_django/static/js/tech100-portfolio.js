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

  const formatNumber = (value, decimals = 2) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    return Number(value).toFixed(decimals);
  };

  const formatPct = (value, decimals = 2) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    return `${Number(value).toFixed(decimals)}%`;
  };

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
    chart.options.scales.x.ticks = {
      ...(chart.options.scales.x.ticks || {}),
      autoSkip: true,
      callback: (value, index) => {
        const label = typeof value === "number" ? labels[value] : labels[index] ?? value;
        return formatter(label ?? value);
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
    const days = daysByRange[rangeKey] || 90;
    const cutoff = new Date(latest.getTime() - days * 24 * 60 * 60 * 1000);
    return points.filter((point) => toDate(point.date) >= cutoff);
  };

  const indexedSeries = (points) => {
    if (!points?.length) return [];
    const base = Number(points[0].level || 0);
    if (!base) return points.map((point) => ({ date: point.date, value: point.level }));
    return points.map((point) => ({
      date: point.date,
      value: (Number(point.level || 0) / base) * 100,
    }));
  };

  const buildLevelDatasets = (seriesByModel, chartMeta, selectedModel, rangeKey) =>
    chartMeta
      .map((meta) => {
        const filtered = indexedSeries(filterByRange(seriesByModel[meta.code] || [], rangeKey));
        if (!filtered.length) return null;
        return {
          label: meta.label,
          data: filtered.map((point) => point.value),
          labels: filtered.map((point) => point.date),
          borderColor: meta.code === selectedModel ? meta.color : withAlpha(meta.color, 0.65),
          backgroundColor: withAlpha(meta.color, meta.code === selectedModel ? 0.18 : 0.08),
          borderWidth: meta.code === selectedModel ? 3 : meta.code === "TECH100" ? 2.3 : 1.6,
          pointRadius: 0,
          tension: 0.18,
        };
      })
      .filter(Boolean);

  const buildSingleSeries = (points, key, rangeKey) => {
    const filtered = filterByRange(points || [], rangeKey);
    return {
      labels: filtered.map((point) => point.date),
      values: filtered.map((point) => {
        const value = point[key];
        return value === null || value === undefined ? null : Number(value) * 100;
      }),
    };
  };

  const ensureChartDefaults = () => {
    if (typeof Chart === "undefined") return;
    Chart.defaults.animation = false;
    Chart.defaults.transitions.active.animation = false;
  };

  const initPortfolioCharts = () => {
    const statusEl = document.getElementById("tech100-portfolio-status");
    const levelCanvas = document.getElementById("tech100-portfolio-level-chart");
    if (!statusEl || !levelCanvas || typeof Chart === "undefined") return;

    const seriesByModel = parseJson("tech100-portfolio-series") || {};
    const chartMeta = parseJson("tech100-portfolio-chart-meta") || [];
    const selectedModel = statusEl.dataset.selectedModel || "TECH100";
    const selectedSeries = seriesByModel[selectedModel] || [];
    let currentRange = "3m";

    ensureChartDefaults();

    const levelChart = new Chart(levelCanvas.getContext("2d"), {
      type: "line",
      data: { labels: [], datasets: [] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x: { ticks: { autoSkip: true, maxTicksLimit: 8 } },
          y: {
            beginAtZero: false,
            ticks: {
              callback: (value) => `${value}`,
            },
          },
        },
        plugins: {
          legend: { position: "bottom" },
          tooltip: {
            callbacks: {
              label: (context) => `${context.dataset.label}: ${formatNumber(context.raw, 2)}`,
            },
          },
        },
      },
    });

    const volCanvas = document.getElementById("tech100-portfolio-vol-chart");
    const volChart = volCanvas
      ? new Chart(volCanvas.getContext("2d"), {
          type: "line",
          data: { labels: [], datasets: [] },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: { ticks: { autoSkip: true, maxTicksLimit: 6 } },
              y: {
                ticks: {
                  callback: (value) => `${value}%`,
                },
              },
            },
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (context) => `Vol: ${formatPct(context.raw, 2)}`,
                },
              },
            },
          },
        })
      : null;

    const drawdownCanvas = document.getElementById("tech100-portfolio-drawdown-chart");
    const drawdownChart = drawdownCanvas
      ? new Chart(drawdownCanvas.getContext("2d"), {
          type: "line",
          data: { labels: [], datasets: [] },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: { ticks: { autoSkip: true, maxTicksLimit: 6 } },
              y: {
                ticks: {
                  callback: (value) => `${value}%`,
                },
              },
            },
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (context) => `Drawdown: ${formatPct(context.raw, 2)}`,
                },
              },
            },
          },
        })
      : null;

    const renderCharts = () => {
      const datasets = buildLevelDatasets(seriesByModel, chartMeta, selectedModel, currentRange);
      const labels = datasets[0]?.labels || [];
      levelChart.data = {
        labels,
        datasets: datasets.map(({ labels: _labels, ...dataset }) => dataset),
      };
      setAxisFormatter(levelChart, currentRange, labels);
      levelChart.update();

      if (volChart) {
        const volSeries = buildSingleSeries(selectedSeries, "vol20", currentRange);
        volChart.data = {
          labels: volSeries.labels,
          datasets: [
            {
              label: "Volatility",
              data: volSeries.values,
              borderColor: "#2f6f6f",
              backgroundColor: withAlpha("#2f6f6f", 0.12),
              borderWidth: 2,
              pointRadius: 0,
              tension: 0.18,
            },
          ],
        };
        setAxisFormatter(volChart, currentRange, volSeries.labels);
        volChart.update();
      }

      if (drawdownChart) {
        const drawdownSeries = buildSingleSeries(selectedSeries, "drawdown", currentRange);
        drawdownChart.data = {
          labels: drawdownSeries.labels,
          datasets: [
            {
              label: "Drawdown",
              data: drawdownSeries.values,
              borderColor: "#b44a3c",
              backgroundColor: withAlpha("#b44a3c", 0.14),
              borderWidth: 2,
              pointRadius: 0,
              tension: 0.18,
              fill: true,
            },
          ],
        };
        setAxisFormatter(drawdownChart, currentRange, drawdownSeries.labels);
        drawdownChart.update();
      }
    };

    renderCharts();

    document.querySelectorAll(".tech100-portfolio-range").forEach((btn) => {
      btn.addEventListener("click", () => {
        const range = btn.dataset.range;
        if (!range) return;
        currentRange = range;
        document.querySelectorAll(".tech100-portfolio-range").forEach((el) =>
          el.classList.toggle("is-active", el === btn)
        );
        renderCharts();
      });
    });
  };

  const initSectorChart = () => {
    const canvas = document.getElementById("tech100-portfolio-sector-chart");
    if (!canvas || typeof Chart === "undefined") return;
    const rows = parseJson("tech100-portfolio-sector-data") || [];
    if (!rows.length) return;

    const useActive = rows.some((row) => Math.abs(Number(row.active_sector_weight_pct || 0)) > 0.01);
    const sorted = [...rows].sort((left, right) => {
      const leftValue = Number(useActive ? left.active_sector_weight_pct : left.sector_weight_pct);
      const rightValue = Number(useActive ? right.active_sector_weight_pct : right.sector_weight_pct);
      return Math.abs(rightValue) - Math.abs(leftValue);
    });

    new Chart(canvas.getContext("2d"), {
      type: "bar",
      data: {
        labels: sorted.map((row) => row.sector),
        datasets: [
          {
            label: useActive ? "Active sector weight (%)" : "Sector weight (%)",
            data: sorted.map((row) =>
              Number(useActive ? row.active_sector_weight_pct : row.sector_weight_pct)
            ),
            backgroundColor: sorted.map((row) =>
              Number(useActive ? row.active_sector_weight_pct : row.sector_weight_pct) >= 0
                ? withAlpha("#183153", 0.82)
                : withAlpha("#b44a3c", 0.78)
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
              callback: (value) => `${value}%`,
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

  dateFormat.applyDateFormatting?.();
  initPortfolioCharts();
  initSectorChart();
})();

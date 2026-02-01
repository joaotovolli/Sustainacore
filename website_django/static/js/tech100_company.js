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
  const companyTrigger = document.querySelector("[data-company-trigger]");
  const companyPanel = document.querySelector("[data-company-panel]");
  const companySearch = document.getElementById("company-select-search");
  const companyList = document.querySelector("[data-company-list]");
  const compareTrigger = document.querySelector("[data-compare-trigger]");
  const comparePanel = document.querySelector("[data-compare-panel]");
  const compareSearch = document.getElementById("compare-select-search");
  const compareList = document.querySelector("[data-compare-list]");
  const compareLegend = document.querySelector("[data-compare-legend]");
  const compareLabel = document.querySelector("[data-compare-label]");
  const summaryBody = document.getElementById("company-summary-body");
  const historyError = document.querySelector("[data-history-error]");
  const summaryError = document.querySelector("[data-summary-error]");
  const headerDate = document.querySelector("[data-company-latest-date]");
  const headerComposite = document.querySelector("[data-company-latest-composite]");

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

  const parseDate = (value) => {
    if (window.SCDateFormat?.parseDateLike) {
      return window.SCDateFormat.parseDateLike(value);
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
  };

  const formatMonthYear = (value, shortYear = false) => {
    const date = parseDate(value);
    if (!date) return "—";
    const parts = new Intl.DateTimeFormat("en-GB", {
      timeZone: "UTC",
      month: "short",
      year: shortYear ? "2-digit" : "numeric",
    }).formatToParts(date);
    const month = parts.find((part) => part.type === "month")?.value;
    const year = parts.find((part) => part.type === "year")?.value;
    if (!month || !year) return "—";
    return `${month} ${year}`;
  };

  const formatAxisMonthYear = (value) => formatMonthYear(value, true);

  const fetchWithTimeout = async (url, options = {}, timeoutMs = 20000) => {
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

  const chartErrors = new Map();
  document.querySelectorAll("[data-chart-error]").forEach((el) => {
    const key = el.getAttribute("data-chart-error");
    if (key) chartErrors.set(key, el);
  });

  const state = {
    charts: new Map(),
    compareTicker: null,
    compareLabel: null,
    companies: [],
    companyLookup: new Map(),
    labelLookup: new Map(),
    companyList: [],
    history: [],
    summaryHistory: [],
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
      return aDate < bDate ? 1 : -1;
    });

    const fragment = document.createDocumentFragment();
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="text-left">${formatMonthYear(row.date)}</td>
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
  };

  const renderSummaryHistory = () => {
    if (!summaryBody) return;
    summaryBody.innerHTML = "";
    if (!state.summaryHistory?.length) {
      summaryBody.innerHTML = '<tr><td colspan="4" class="muted">No summaries available.</td></tr>';
      return;
    }
    const fragment = document.createDocumentFragment();
    const rows = [...state.summaryHistory].sort((a, b) => {
      const aDate = a.date || "";
      const bDate = b.date || "";
      if (aDate === bDate) return 0;
      return aDate < bDate ? 1 : -1;
    });
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      const text = row.summary || "";
      const isTruncated = text.length > 0;
      const summaryCell = document.createElement("td");
      summaryCell.className = "summary-cell";
      const summarySpan = document.createElement("span");
      summarySpan.className = "summary-text";
      summarySpan.textContent = text || "—";
      summaryCell.appendChild(summarySpan);
      if (isTruncated) {
        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "summary-toggle text-link";
        toggle.textContent = "Expand";
        toggle.setAttribute("aria-expanded", "false");
        summaryCell.appendChild(toggle);
      }
      tr.innerHTML = `
        <td class="text-left">${formatMonthYear(row.date)}</td>
        <td class="text-center">${row.rank ?? "—"}</td>
        <td class="text-right">${formatScore(row.composite)}</td>
      `;
      tr.appendChild(summaryCell);
      fragment.appendChild(tr);
    });
    summaryBody.appendChild(fragment);
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
              title: (items) => formatMonthYear(items[0]?.label),
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
              callback: (value) => formatAxisMonthYear(labels[value]),
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
    if (!Array.isArray(companies)) return;
    state.companies = companies;
    state.companyLookup.clear();
    state.labelLookup.clear();
    state.companyList = companies
      .map((company) => {
        const symbol = (company.ticker || "").toUpperCase();
        if (!symbol) return null;
        const label = formatCompanyLabel(company);
        state.companyLookup.set(symbol, label);
        state.labelLookup.set(label.toLowerCase(), symbol);
        return { symbol, label };
      })
      .filter(Boolean)
      .sort((a, b) => a.label.localeCompare(b.label));
  };

  const setChartError = (metricKey, message) => {
    const el = chartErrors.get(metricKey);
    if (!el) return;
    if (!message) {
      el.hidden = true;
      el.textContent = "";
      return;
    }
    el.textContent = message;
    el.hidden = false;
  };

  const loadBundle = async (includeCompanies = false) => {
    if (historyError) historyError.hidden = true;
    if (summaryError) summaryError.hidden = true;
    try {
      const resp = await fetchWithTimeout(bundleUrl(state.compareTicker, includeCompanies));
      if (!resp.ok) throw new Error(`bundle ${resp.status}`);
      const data = await resp.json();
      state.history = data.history || [];
      state.summaryHistory = data.summary_history || [];
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
      METRIC_KEYS.forEach((metricKey) => setChartError(metricKey, null));
    } catch (err) {
      console.warn("Company bundle failed", err);
      const statusLabel = describeFetchError(err);
      METRIC_KEYS.forEach((metricKey) => setChartError(metricKey, `Couldn't load chart (${statusLabel}).`));
      if (historyError) {
        historyError.textContent = `Couldn't load history (${statusLabel}).`;
        historyError.hidden = false;
      }
      if (summaryError) {
        summaryError.textContent = `Couldn't load summaries (${statusLabel}).`;
        summaryError.hidden = false;
      }
    }
  };

  const handleCompanyChange = (nextLabel) => {
    const nextTicker = extractTicker(nextLabel);
    if (!nextTicker || nextTicker === ticker) return;
    window.location.assign(`/tech100/company/${nextTicker}/`);
  };

  const handleCompareChange = (nextLabel) => {
    const nextTicker = extractTicker(nextLabel);
    if (!nextTicker || nextTicker === ticker) {
      state.compareTicker = null;
      state.compareLabel = null;
      updateCompareLegend();
      if (compareTrigger) compareTrigger.textContent = "None";
      loadBundle(false);
      return;
    }
    state.compareTicker = nextTicker;
    state.compareLabel = state.companyLookup.get(nextTicker) || nextTicker;
    updateCompareLegend();
    if (compareTrigger) compareTrigger.textContent = state.compareLabel;
    loadBundle(false);
  };

  summaryBody?.addEventListener("click", (event) => {
    const target = event.target;
    if (!target || !target.classList.contains("summary-toggle")) return;
    const cell = target.closest(".summary-cell");
    const textEl = cell?.querySelector(".summary-text");
    if (!textEl) return;
    const isExpanded = textEl.classList.toggle("is-expanded");
    target.textContent = isExpanded ? "Collapse" : "Expand";
    target.setAttribute("aria-expanded", isExpanded ? "true" : "false");
  });

  const closePanel = (panel, trigger) => {
    if (!panel || !trigger) return;
    panel.hidden = true;
    trigger.setAttribute("aria-expanded", "false");
  };

  const openPanel = (panel, trigger, search) => {
    if (!panel || !trigger) return;
    panel.hidden = false;
    trigger.setAttribute("aria-expanded", "true");
    if (search) {
      search.focus();
      search.select();
    }
  };

  const updateList = (listEl, filterText, allowNone, activeValue) => {
    if (!listEl) return [];
    const query = (filterText || "").trim().toLowerCase();
    const options = state.companyList.filter((option) => option.label.toLowerCase().includes(query));
    const finalOptions = allowNone
      ? [{ symbol: "", label: "None" }, ...options]
      : options;
    listEl.innerHTML = "";
    const fragment = document.createDocumentFragment();
    if (!finalOptions.length) {
      const empty = document.createElement("li");
      empty.className = "tech100-company__select-option is-empty";
      empty.setAttribute("role", "option");
      empty.setAttribute("aria-disabled", "true");
      empty.textContent = "No matches";
      fragment.appendChild(empty);
      listEl.appendChild(fragment);
      return [];
    }
    finalOptions.forEach((option, idx) => {
      const li = document.createElement("li");
      li.className = "tech100-company__select-option";
      li.setAttribute("role", "option");
      li.dataset.value = option.symbol;
      li.dataset.label = option.label;
      li.id = `${listEl.id || "company-option"}-${idx}`;
      li.textContent = option.label;
      if (option.label === activeValue) {
        li.classList.add("is-selected");
        li.setAttribute("aria-selected", "true");
      }
      fragment.appendChild(li);
    });
    listEl.appendChild(fragment);
    return Array.from(listEl.querySelectorAll(".tech100-company__select-option"));
  };

  const setupSelector = ({
    trigger,
    panel,
    search,
    list,
    allowNone,
    onSelect,
  }) => {
    if (!trigger || !panel || !search || !list) return;
    let activeIndex = -1;
    let options = [];

    const refresh = () => {
      options = updateList(list, search.value, allowNone, trigger.textContent.trim());
      activeIndex = options.length ? 0 : -1;
      options.forEach((opt, idx) => {
        opt.classList.toggle("is-active", idx === activeIndex);
      });
    };

    const setActive = (idx) => {
      if (!options.length) return;
      activeIndex = Math.max(0, Math.min(idx, options.length - 1));
      options.forEach((opt, i) => opt.classList.toggle("is-active", i === activeIndex));
      const active = options[activeIndex];
      if (active) {
        list.setAttribute("aria-activedescendant", active.id);
        active.scrollIntoView({ block: "nearest" });
      }
    };

    trigger.addEventListener("click", () => {
      if (panel.hidden) {
        refresh();
        openPanel(panel, trigger, search);
      } else {
        closePanel(panel, trigger);
      }
    });

    trigger.addEventListener("keydown", (event) => {
      if (event.key === "ArrowDown") {
        event.preventDefault();
        refresh();
        openPanel(panel, trigger, search);
      }
    });

    search.addEventListener("input", () => {
      refresh();
    });

    search.addEventListener("keydown", (event) => {
      if (event.key === "ArrowDown") {
        event.preventDefault();
        setActive(activeIndex + 1);
      } else if (event.key === "ArrowUp") {
        event.preventDefault();
        setActive(activeIndex - 1);
      } else if (event.key === "Enter") {
        event.preventDefault();
        const active = options[activeIndex];
        if (active) {
          onSelect(active.dataset.label, active.dataset.value);
          closePanel(panel, trigger);
        }
      } else if (event.key === "Escape") {
        event.preventDefault();
        closePanel(panel, trigger);
        trigger.focus();
      }
    });

    list.addEventListener("click", (event) => {
      const option = event.target.closest(".tech100-company__select-option");
      if (!option) return;
      onSelect(option.dataset.label, option.dataset.value);
      closePanel(panel, trigger);
    });

    list.addEventListener("mousemove", (event) => {
      const option = event.target.closest(".tech100-company__select-option");
      if (!option) return;
      const idx = options.indexOf(option);
      if (idx >= 0) setActive(idx);
    });
  };

  document.addEventListener("click", (event) => {
    if (companyPanel && !companyPanel.hidden && !companyPanel.contains(event.target) && !companyTrigger.contains(event.target)) {
      closePanel(companyPanel, companyTrigger);
    }
    if (comparePanel && !comparePanel.hidden && !comparePanel.contains(event.target) && !compareTrigger.contains(event.target)) {
      closePanel(comparePanel, compareTrigger);
    }
  });

  const updateHeaderFacts = () => {
    if (headerDate) {
      headerDate.textContent = formatMonthYear(headerDate.dataset.dateValue || headerDate.textContent);
    }
    if (headerComposite) {
      headerComposite.textContent = headerComposite.textContent?.trim() || "—";
    }
  };

  setupSelector({
    trigger: companyTrigger,
    panel: companyPanel,
    search: companySearch,
    list: companyList,
    allowNone: false,
    onSelect: (label) => handleCompanyChange(label),
  });

  setupSelector({
    trigger: compareTrigger,
    panel: comparePanel,
    search: compareSearch,
    list: compareList,
    allowNone: true,
    onSelect: (label, value) => {
      if (!value) {
        handleCompareChange("");
        return;
      }
      handleCompareChange(label);
    },
  });

  updateHeaderFacts();
  loadBundle(true);
})();

(() => {
  const DEFAULT_LOCALE = "en-GB";

  const parseDateLike = (value) => {
    if (!value) return null;
    if (value instanceof Date) {
      return Number.isNaN(value.getTime()) ? null : value;
    }
    if (typeof value === "number") {
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? null : date;
    }

    const text = String(value).trim();
    if (!text) return null;

    const dateOnlyMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
    if (dateOnlyMatch) {
      const [, year, month, day] = dateOnlyMatch;
      const date = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
      return Number.isNaN(date.getTime()) ? null : date;
    }

    const stripped = text.replace("Z", "");
    const isoDate = new Date(stripped);
    if (!Number.isNaN(isoDate.getTime())) return isoDate;

    const datePart = text.split("T")[0];
    if (datePart && datePart !== text) {
      const fallback = parseDateLike(datePart);
      if (fallback) return fallback;
    }

    return null;
  };

  const formatParts = (date, options) => {
    try {
      return new Intl.DateTimeFormat(DEFAULT_LOCALE, {
        timeZone: "UTC",
        ...options,
      }).formatToParts(date);
    } catch (error) {
      return null;
    }
  };

  const getPart = (parts, type) => parts?.find((part) => part.type === type)?.value;

  const formatAsOfDate = (value) => {
    const date = parseDateLike(value);
    if (!date) return "—";
    const parts = formatParts(date, { day: "2-digit", month: "short", year: "numeric" });
    const day = getPart(parts, "day");
    const month = getPart(parts, "month");
    const year = getPart(parts, "year");
    if (!day || !month || !year) return "—";
    return `${day}-${month}-${year}`;
  };

  const formatAxisShort = (value) => {
    const date = parseDateLike(value);
    if (!date) return "";
    const parts = formatParts(date, { day: "2-digit", month: "short", year: "2-digit" });
    const day = getPart(parts, "day");
    const month = getPart(parts, "month");
    const year = getPart(parts, "year");
    if (!day || !month || !year) return "";
    return `${day}-${month}-${year}`;
  };

  const formatAxisLong = (value) => {
    const date = parseDateLike(value);
    if (!date) return "";
    const parts = formatParts(date, { month: "short", year: "2-digit" });
    const month = getPart(parts, "month");
    const year = getPart(parts, "year");
    if (!month || !year) return "";
    return `${month}-${year}`;
  };

  const formatMonthYear = (value) => {
    const date = parseDateLike(value);
    if (!date) return "—";
    const parts = formatParts(date, { month: "short", year: "numeric" });
    const month = getPart(parts, "month");
    const year = getPart(parts, "year");
    if (!month || !year) return "—";
    return `${month}-${year}`;
  };

  const chooseAxisFormatter = (rangeKey, data = []) => {
    const normalized = String(rangeKey || "").toLowerCase();
    if (normalized === "1m") return formatAxisShort;
    if (Array.isArray(data) && data.length > 1) {
      const first = parseDateLike(data[0]?.date ?? data[0]);
      const last = parseDateLike(data[data.length - 1]?.date ?? data[data.length - 1]);
      if (first && last) {
        const diffDays = Math.abs(last.getTime() - first.getTime()) / (1000 * 60 * 60 * 24);
        if (diffDays <= 45) return formatAxisShort;
      }
    }
    return formatAxisLong;
  };

  const formatByType = (type, value) => {
    if (!value || value === "—") return "—";
    switch (type) {
      case "axis-short":
        return formatAxisShort(value) || "—";
      case "axis-long":
        return formatAxisLong(value) || "—";
      case "month-year":
        return formatMonthYear(value);
      case "as-of":
      default:
        return formatAsOfDate(value);
    }
  };

  const applyDateFormatting = (root = document) => {
    if (!root?.querySelectorAll) return;
    root.querySelectorAll("[data-date-format]").forEach((el) => {
      const formatType = el.dataset.dateFormat;
      const rawValue = el.dataset.dateValue || el.textContent;
      el.textContent = formatByType(formatType, rawValue);
    });
  };

  window.SCDateFormat = {
    parseDateLike,
    formatAsOfDate,
    formatAxisShort,
    formatAxisLong,
    formatMonthYear,
    chooseAxisFormatter,
    applyDateFormatting,
  };
})();

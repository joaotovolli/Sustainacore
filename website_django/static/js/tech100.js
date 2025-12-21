(function () {
  const modal = document.getElementById("tech100-modal");
  const dialog = modal ? modal.querySelector(".modal__dialog") : null;
  const content = modal ? modal.querySelector("#tech100-modal-content") : null;
  const title = modal ? modal.querySelector("#tech100-modal-title") : null;
  const closeButton = modal ? modal.querySelector(".modal__close") : null;
  const detailsButtons = document.querySelectorAll(".tech100-details-button");
  const copyButtons = document.querySelectorAll("[data-copy-link]");
  let activeTrigger = null;

  copyButtons.forEach((button) => {
    button.addEventListener("click", async () => {
      const url = window.location.href;
      try {
        if (navigator.clipboard?.writeText) {
          await navigator.clipboard.writeText(url);
        } else {
          const temp = document.createElement("input");
          temp.value = url;
          document.body.appendChild(temp);
          temp.select();
          document.execCommand("copy");
          temp.remove();
        }
        const original = button.textContent;
        button.textContent = "Copied";
        setTimeout(() => {
          button.textContent = original;
        }, 1500);
      } catch (err) {
        console.warn("Unable to copy link", err);
      }
    });
  });

  if (!modal || !dialog || !content || !detailsButtons.length) {
    return;
  }

  function openModal(templateId, companyLabel) {
    const template = document.getElementById(templateId);
    if (!template) return;

    content.innerHTML = "";
    content.appendChild(template.content.cloneNode(true));

    if (title) {
      title.textContent = companyLabel || "Company details";
    }

    modal.hidden = false;
    window.requestAnimationFrame(() => {
      modal.classList.add("is-visible");
    });
    document.body.classList.add("has-modal-open");
    dialog.focus({ preventScroll: true });
  }

  function closeModal() {
    if (modal.hidden) return;
    modal.classList.remove("is-visible");
    modal.hidden = true;
    document.body.classList.remove("has-modal-open");
    if (activeTrigger) {
      activeTrigger.focus({ preventScroll: true });
    }
  }

  detailsButtons.forEach((button) => {
    button.addEventListener("click", () => {
      activeTrigger = button;
      openModal(button.dataset.template, button.dataset.company);
    });
  });

  closeButton?.addEventListener("click", closeModal);

  modal.addEventListener("click", (event) => {
    if (event.target === modal) {
      closeModal();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeModal();
    }
  });

})();

(async function () {
  const dateFormat = window.SCDateFormat || {};
  const rebalanceSelect = document.querySelector('select[name="port_date"]');
  const filterForm = rebalanceSelect?.closest("form");

  if (!rebalanceSelect) {
    return;
  }

  const formatMonthYear = (value) =>
    typeof dateFormat.formatMonthYear === "function"
      ? dateFormat.formatMonthYear(value)
      : value;

  function requestFilterSubmit() {
    if (!filterForm) return;
    const submitButton = filterForm.querySelector('button[type="submit"]');
    if (typeof filterForm.requestSubmit === "function") {
      filterForm.requestSubmit(submitButton || undefined);
    } else {
      filterForm.submit();
    }
  }

  async function loadAvailableRebalanceDates() {
    const params = new URLSearchParams({ limit: "200" });
    const apiBase =
      (window.SC_BACKEND_API_BASE || window.TECH100_API_BASE || "").toString().trim().replace(/\/$/, "");
    const url = `${apiBase}/api/tech100/rebalance_dates?${params.toString()}`;
    try {
      const response = await fetch(url);
      if (!response.ok) {
        console.warn(
          "TECH100 rebalance dates request failed",
          response.status,
          response.statusText
        );
        return [];
      }
      const payload = await response.json();
      const items = Array.isArray(payload?.items) ? payload.items : [];

      return items
        .filter((value) => typeof value === "string" && Boolean(value))
        .sort((a, b) => (a < b ? 1 : -1));
    } catch (error) {
      console.warn("Unable to load TECH100 rebalance dates", error);
      return [];
    }
  }

  async function setupRebalanceSelect() {
    const urlParams = new URLSearchParams(window.location.search);
    const paramPortDate = urlParams.get("port_date");
    const hasPortDateQuery = Boolean(paramPortDate);

    const apiDates = await loadAvailableRebalanceDates();

    const optionValues = apiDates.length
      ? apiDates
      : Array.from(rebalanceSelect.querySelectorAll("option"))
          .map((option) => option.value)
          .filter(Boolean);

    if (!optionValues.length) {
      return;
    }

    const sortedValues = [...new Set(optionValues)].sort((a, b) => (a < b ? 1 : -1));
    const latestValue = sortedValues[0];

    const fragment = document.createDocumentFragment();
    sortedValues.forEach((value) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = formatMonthYear(value);
      fragment.appendChild(option);
    });

    rebalanceSelect.innerHTML = "";
    rebalanceSelect.appendChild(fragment);

    const targetValue =
      hasPortDateQuery && sortedValues.includes(paramPortDate)
        ? paramPortDate
        : latestValue;

    rebalanceSelect.value = targetValue;

    if (!hasPortDateQuery && targetValue) {
      requestFilterSubmit();
    }
  }

  dateFormat.applyDateFormatting?.();
  setupRebalanceSelect();
})();

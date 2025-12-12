(function () {
  const modal = document.getElementById("tech100-modal");
  const dialog = modal ? modal.querySelector(".modal__dialog") : null;
  const content = modal ? modal.querySelector("#tech100-modal-content") : null;
  const title = modal ? modal.querySelector("#tech100-modal-title") : null;
  const closeButton = modal ? modal.querySelector(".modal__close") : null;
  const detailsButtons = document.querySelectorAll(".tech100-details-button");
  let activeTrigger = null;

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
  const rebalanceSelect = document.querySelector('select[name="port_date"]');
  const filterForm = rebalanceSelect?.closest("form");

  if (!rebalanceSelect) {
    return;
  }

  const monthLabels = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];

  function formatMMMYYYY(dateString) {
    const [year, month] = dateString.split("-");
    const monthIndex = Number.parseInt(month, 10) - 1;
    if (!monthLabels[monthIndex] || !year) {
      return dateString;
    }
    return `${monthLabels[monthIndex]}-${year}`;
  }

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
    const params = new URLSearchParams({ limit: "2000" });
    try {
      const response = await fetch(`/api/tech100?${params.toString()}`);
      if (!response.ok) {
        return [];
      }
      const payload = await response.json();
      const items = Array.isArray(payload)
        ? payload
        : Array.isArray(payload.items)
        ? payload.items
        : [];

      const distinct = new Set();
      items.forEach((item) => {
        const value = item && typeof item.port_date === "string" ? item.port_date : null;
        if (value) {
          distinct.add(value);
        }
      });

      return Array.from(distinct).sort((a, b) => (a < b ? 1 : -1));
    } catch (error) {
      console.error("Unable to load TECH100 rebalance dates", error);
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
      option.textContent = formatMMMYYYY(value);
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

  setupRebalanceSelect();
})();

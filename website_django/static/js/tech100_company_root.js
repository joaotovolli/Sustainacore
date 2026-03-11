(() => {
  const root = document.querySelector("[data-company-hub]");
  if (!root) return;

  const searchInput = root.querySelector("[data-company-search]");
  const sectorSelect = root.querySelector("[data-company-sector]");
  const clearButton = root.querySelector("[data-company-clear]");
  const countNode = root.querySelector("[data-company-results-count]");
  const emptyState = root.querySelector("[data-company-empty]");
  const items = Array.from(root.querySelectorAll("[data-company-item]"));

  if (!items.length) return;

  const renderCount = (count) => {
    if (!countNode) return;
    countNode.textContent = `${count} ${count === 1 ? "company" : "companies"}`;
  };

  const update = () => {
    const query = (searchInput?.value || "").trim().toLowerCase();
    const sector = (sectorSelect?.value || "").trim().toLowerCase();
    let visibleCount = 0;

    items.forEach((item) => {
      const text = (item.getAttribute("data-company-search-text") || "").toLowerCase();
      const itemSector = (item.getAttribute("data-company-sector") || "").toLowerCase();
      const matchesQuery = !query || text.includes(query);
      const matchesSector = !sector || itemSector === sector;
      const visible = matchesQuery && matchesSector;
      item.hidden = !visible;
      if (visible) visibleCount += 1;
    });

    renderCount(visibleCount);
    if (emptyState) emptyState.hidden = visibleCount !== 0;
  };

  searchInput?.addEventListener("input", update);
  sectorSelect?.addEventListener("change", update);
  clearButton?.addEventListener("click", () => {
    if (searchInput) searchInput.value = "";
    if (sectorSelect) sectorSelect.value = "";
    update();
    searchInput?.focus();
  });

  update();
})();

(() => {
  const toggle = document.querySelector("[data-nav-toggle]");
  const panel = document.querySelector("[data-nav-panel]");
  const topbar = document.querySelector(".topbar");
  if (!toggle || !panel || !topbar) return;

  const setPanelState = (isOpen) => {
    topbar.classList.toggle("topbar--open", isOpen);
    toggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
  };

  toggle.addEventListener("click", () => {
    const isOpen = topbar.classList.contains("topbar--open");
    setPanelState(!isOpen);
  });

  panel.addEventListener("click", (event) => {
    const link = event.target.closest("a");
    if (!link) return;
    if (window.matchMedia("(max-width: 768px)").matches) {
      setPanelState(false);
    }
  });

  document.addEventListener("click", (event) => {
    if (panel.contains(event.target) || toggle.contains(event.target)) return;
    setPanelState(false);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    setPanelState(false);
  });
})();

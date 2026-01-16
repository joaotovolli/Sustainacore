(() => {
  const toggle = document.querySelector('[data-nav-toggle]');
  const panel = document.querySelector('[data-nav-panel]');
  if (!toggle || !panel) return;

  const dropdowns = Array.from(panel.querySelectorAll('[data-nav-dropdown]'));

  const setPanelState = (isOpen) => {
    panel.classList.toggle('is-open', isOpen);
    toggle.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
  };

  const closeDropdowns = () => {
    dropdowns.forEach((dropdown) => {
      dropdown.classList.remove('is-open');
      const button = dropdown.querySelector('[data-nav-dropdown-toggle]');
      if (button) button.setAttribute('aria-expanded', 'false');
    });
  };

  toggle.addEventListener('click', () => {
    const isOpen = panel.classList.contains('is-open');
    setPanelState(!isOpen);
    if (isOpen) closeDropdowns();
  });

  dropdowns.forEach((dropdown) => {
    const button = dropdown.querySelector('[data-nav-dropdown-toggle]');
    if (!button) return;
    button.addEventListener('click', (event) => {
      event.preventDefault();
      const isOpen = dropdown.classList.contains('is-open');
      closeDropdowns();
      dropdown.classList.toggle('is-open', !isOpen);
      button.setAttribute('aria-expanded', !isOpen ? 'true' : 'false');
    });
  });

  panel.addEventListener('click', (event) => {
    const link = event.target.closest('a');
    if (!link) return;
    if (window.matchMedia('(max-width: 768px)').matches) {
      setPanelState(false);
      closeDropdowns();
    }
  });

  document.addEventListener('click', (event) => {
    if (panel.contains(event.target) || toggle.contains(event.target)) return;
    setPanelState(false);
    closeDropdowns();
  });

  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape') return;
    setPanelState(false);
    closeDropdowns();
  });
})();

(() => {
  const root = document.querySelector("[data-auth-menu]");
  if (!root) return;

  const button = root.querySelector(".auth-pill__button");
  const menu = root.querySelector(".auth-menu");
  if (!button || !menu) return;

  const closeMenu = () => {
    root.classList.remove("is-open");
    button.setAttribute("aria-expanded", "false");
  };

  const openMenu = () => {
    root.classList.add("is-open");
    button.setAttribute("aria-expanded", "true");
  };

  button.addEventListener("click", (event) => {
    event.stopPropagation();
    if (root.classList.contains("is-open")) {
      closeMenu();
    } else {
      openMenu();
    }
  });

  button.addEventListener("keydown", (event) => {
    if (event.key === " " || event.key === "Enter") {
      event.preventDefault();
      if (root.classList.contains("is-open")) {
        closeMenu();
      } else {
        openMenu();
        const firstItem = menu.querySelector(".auth-menu__item");
        if (firstItem) {
          firstItem.focus();
        }
      }
    }
  });

  document.addEventListener("click", (event) => {
    if (!root.contains(event.target)) {
      closeMenu();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeMenu();
      button.focus();
    }
  });
})();

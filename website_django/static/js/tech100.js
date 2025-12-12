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

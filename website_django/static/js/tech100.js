(function () {
  const tableButtons = Array.from(document.querySelectorAll('.tech100-toggle'));
  const tableDetails = Array.from(document.querySelectorAll('.tech100__details-row'));
  const mobileButtons = Array.from(document.querySelectorAll('.tech100-mobile-toggle'));
  const mobileDetails = Array.from(document.querySelectorAll('.tech100-card__details'));
  const loadMoreButtons = Array.from(document.querySelectorAll('.tech100-load-more__button'));

  const resetTableButtons = () => {
    tableButtons.forEach((btn) => {
      btn.setAttribute('aria-expanded', 'false');
      btn.textContent = 'View details';
    });
  };

  const closeTableDetails = () => {
    tableDetails.forEach((row) => {
      row.classList.remove('is-open');
      row.setAttribute('aria-hidden', 'true');
    });
    resetTableButtons();
  };

  const closeMobileDetails = () => {
    mobileDetails.forEach((panel) => {
      panel.classList.remove('is-open');
      panel.setAttribute('aria-hidden', 'true');
    });
    mobileButtons.forEach((btn) => {
      btn.setAttribute('aria-expanded', 'false');
      btn.textContent = 'View details';
    });
  };

  tableButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      const detailId = btn.dataset.detailId;
      const row = detailId ? document.getElementById(detailId) : null;
      const isOpen = row?.classList.contains('is-open');

      closeTableDetails();
      closeMobileDetails();

      if (!row) {
        return;
      }

      if (!isOpen) {
      if (!isOpen && row) {
        row.classList.add('is-open');
        row.classList.remove('tech100-hidden');
        row.setAttribute('aria-hidden', 'false');
        btn.setAttribute('aria-expanded', 'true');
        btn.textContent = 'Hide details';
        row.scrollIntoView({ behavior: 'smooth', block: 'start', inline: 'nearest' });
      }
    });
  });

  mobileButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      const detailId = btn.dataset.detailId;
      const panel = detailId ? document.getElementById(detailId) : null;
      const isOpen = panel?.classList.contains('is-open');

      closeTableDetails();
      closeMobileDetails();

      if (!panel) {
        return;
      }

      if (!isOpen) {
      if (!isOpen && panel) {
        panel.classList.add('is-open');
        panel.classList.remove('tech100-hidden');
        panel.setAttribute('aria-hidden', 'false');
        btn.setAttribute('aria-expanded', 'true');
        btn.textContent = 'Hide details';
        panel.scrollIntoView({ behavior: 'smooth', block: 'start', inline: 'nearest' });
      }
    });
  });

  const revealNextBatch = (pageSize) => {
    const hiddenRows = Array.from(document.querySelectorAll('.tech100-row.tech100-hidden[data-lazy="true"]'));
    const toReveal = hiddenRows.slice(0, pageSize);

    toReveal.forEach((row) => {
      row.classList.remove('tech100-hidden');
      const idx = row.dataset.rowIdx;
      if (idx !== undefined) {
        const detailRow = document.querySelector(`.tech100__details-row[data-row-idx="${idx}"]`);
        const mobileCard = document.querySelector(`.tech100-card[data-row-idx="${idx}"]`);
        detailRow?.classList.remove('tech100-hidden');
        mobileCard?.classList.remove('tech100-hidden');
      }
    });

    const remaining = document.querySelectorAll('.tech100-row.tech100-hidden[data-lazy="true"]').length;
    if (remaining === 0) {
      loadMoreButtons.forEach((btn) => btn.remove());
    }
  };

  loadMoreButtons.forEach((btn) => {
    const pageSize = Number.parseInt(btn.dataset.pageSize || '25', 10);
    btn.addEventListener('click', () => revealNextBatch(pageSize));
  });
})();

(function () {
  const form = document.querySelector('.filters');
  if (!form) return;
  const trackFilter = () => window.SCTelemetry?.track?.("filter_applied", { page: window.location.pathname });

  const inputs = form.querySelectorAll('select');
  inputs.forEach((input) => {
    input.addEventListener('change', () => {
      trackFilter();
      form.submit();
    });
  });
})();

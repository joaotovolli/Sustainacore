(function () {
  const form = document.querySelector('.filters');
  if (!form) return;

  const inputs = form.querySelectorAll('select');
  inputs.forEach((input) => {
    input.addEventListener('change', () => {
      form.submit();
    });
  });
})();

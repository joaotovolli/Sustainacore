(function () {
  const form = document.querySelector('[data-ask2-form]');
  const input = document.querySelector('[data-ask2-input]');
  const messages = document.querySelector('[data-ask2-messages]');
  const status = document.querySelector('[data-ask2-status]');
  const statusText = status ? status.querySelector('.status__text') : null;
  const errorBanner = document.querySelector('[data-ask2-error]');

  if (!form || !input || !messages) {
    return;
  }

  function setStatus(text, ok = true) {
    if (statusText) {
      statusText.textContent = text;
    }
    if (status) {
      status.classList.toggle('is-error', !ok);
    }
  }

  function toggleError(show, message) {
    if (!errorBanner) return;
    errorBanner.hidden = !show;
    if (message) {
      errorBanner.textContent = message;
    }
  }

  function addMessage(role, text, muted = false) {
    const bubble = document.createElement('div');
    bubble.className = `bubble bubble--${role}${muted ? ' bubble--muted' : ''}`;
    bubble.textContent = text;
    messages.appendChild(bubble);
    messages.scrollTop = messages.scrollHeight;
    return bubble;
  }

  async function sendMessage(text) {
    const pending = addMessage('assistant', 'SustainaCore is thinking…', true);
    toggleError(false);
    setStatus('Contacting backend…');

    try {
      const response = await fetch('/ask2/api/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: text })
      });
      const data = await response.json();

      if (!response.ok || data.error) {
        throw new Error(data.message || data.error || `Request failed (${response.status})`);
      }

      pending.remove();
      addMessage('assistant', data.reply || data.answer || data.content || 'No response received.');
      setStatus('Ready to chat');
    } catch (err) {
      if (pending && pending.remove) pending.remove();
      const message = err && err.message ? err.message : 'Unable to reach Ask2 right now.';
      addMessage('assistant', `Sorry, ${message}`);
      toggleError(true, message);
      setStatus('Backend unavailable', false);
    }
  }

  form.addEventListener('submit', function (event) {
    event.preventDefault();
    const text = (input.value || '').trim();
    if (!text) {
      input.focus();
      return;
    }
    addMessage('user', text);
    input.value = '';
    sendMessage(text);
  });

  input.addEventListener('keydown', function (event) {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      form.dispatchEvent(new Event('submit'));
    }
  });
})();

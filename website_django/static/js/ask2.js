(function () {
  const form = document.querySelector('[data-ask2-form]');
  const input = document.querySelector('[data-ask2-input]');
  const messagesEl = document.querySelector('[data-ask2-messages]');
  const status = document.querySelector('[data-ask2-status]');
  const statusText = document.querySelector('[data-ask2-status-text]');
  const thinking = document.querySelector('[data-ask2-thinking]');
  const errorBanner = document.querySelector('[data-ask2-error]');
  const sendButton = document.querySelector('[data-ask2-send]');

  if (!form || !input || !messagesEl) return;

  const messages = [];

  function getCSRFToken() {
    const metaToken = document.querySelector('meta[name="csrf-token"]');
    if (metaToken && metaToken.content) {
      return metaToken.content;
    }
    const match = document.cookie.match(/csrftoken=([^;]+)/);
    return match ? match[1] : null;
  }

  function setStatus(text, ok = true) {
    if (statusText) statusText.textContent = text;
    if (status) status.classList.toggle('is-error', !ok);
  }

  function toggleThinking(show) {
    if (thinking) thinking.hidden = !show;
    if (sendButton) sendButton.disabled = show;
  }

  function showError(message) {
    if (!errorBanner) return;
    errorBanner.hidden = false;
    errorBanner.textContent = message;
  }

  function hideError() {
    if (errorBanner) errorBanner.hidden = true;
  }

  function addBubble(role, text, { muted = false, store = true } = {}) {
    const bubble = document.createElement('div');
    bubble.className = `bubble bubble--${role}${muted ? ' bubble--muted' : ''}`;
    bubble.textContent = text;
    messagesEl.appendChild(bubble);
    messagesEl.scrollTop = messagesEl.scrollHeight;
    if (store) messages.push({ role, text });
    return bubble;
  }

  async function sendMessage(text) {
    hideError();
    toggleThinking(true);
    setStatus('Contacting backend…');

    const placeholder = addBubble('assistant', 'Assistant is thinking…', {
      muted: true,
      store: false,
    });

    try {
      const response = await fetch('/ask2/api/', {
        method: 'POST',
        credentials: 'same-origin',
        headers: {
          'Content-Type': 'application/json',
          ...(getCSRFToken() ? { 'X-CSRFToken': getCSRFToken() } : {}),
        },
        body: JSON.stringify({ message: text }),
      });

      let data;
      try {
        data = await response.json();
      } catch (_) {
        throw new Error('The assistant response could not be read.');
      }

      if (!response.ok || data.error) {
        const errMsg = data.message || data.error || `Request failed (${response.status})`;
        throw new Error(errMsg);
      }

      const reply = data.reply || data.answer || data.content || 'The assistant did not return a response.';
      placeholder.textContent = reply;
      placeholder.classList.remove('bubble--muted');
      messages.push({ role: 'assistant', text: reply });
      setStatus('Ready to chat');
    } catch (error) {
      const friendly = 'The assistant is temporarily unavailable. Please try again in a moment.';
      placeholder.textContent = friendly;
      placeholder.classList.remove('bubble--muted');
      messages.push({ role: 'assistant', text: friendly });
      showError(friendly);
      setStatus(error && error.message ? error.message : friendly, false);
    } finally {
      toggleThinking(false);
    }
  }

  form.addEventListener('submit', function (event) {
    event.preventDefault();
    const text = (input.value || '').trim();
    if (!text) {
      input.focus();
      return;
    }

    addBubble('user', text);
    input.value = '';
    sendMessage(text);
  });

  input.addEventListener('keydown', function (event) {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      form.dispatchEvent(new Event('submit', { cancelable: true, bubbles: true }));
    }
  });
})();

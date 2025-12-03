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

  function toggleError(show, message) {
    if (!errorBanner) return;
    errorBanner.hidden = !show;
    if (message) errorBanner.textContent = message;
  }

  function createBubble(role, text, muted = false) {
    const bubble = document.createElement('div');
    bubble.className = `bubble bubble--${role}${muted ? ' bubble--muted' : ''}`;
    bubble.textContent = text;
    return bubble;
  }

  function appendBubble(bubble) {
    messagesEl.appendChild(bubble);
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  async function sendMessage(text) {
    toggleError(false);
    toggleThinking(true);
    setStatus('Contacting backend…');

    const placeholder = createBubble('assistant', 'Assistant is thinking…', true);
    appendBubble(placeholder);

    try {
      const response = await fetch('/ask2/api/', {
        method: 'POST',
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

      placeholder.remove();
      const reply = data.reply || data.answer || data.content || 'The assistant did not return a response.';
      messages.push({ role: 'assistant', text: reply });
      appendBubble(createBubble('assistant', reply));
      setStatus('Ready to chat');
    } catch (error) {
      if (placeholder && placeholder.remove) placeholder.remove();
      const friendly = 'The assistant is temporarily unavailable. Please try again in a moment.';
      const detail = error && error.message ? error.message : friendly;
      messages.push({ role: 'assistant', text: friendly });
      appendBubble(createBubble('assistant', friendly));
      toggleError(true, friendly);
      setStatus(detail, false);
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

    messages.push({ role: 'user', text });
    appendBubble(createBubble('user', text));
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

(function () {
  document.addEventListener('DOMContentLoaded', () => {
    const messagesEl = document.querySelector('[data-ask2-messages]');
    const form = document.querySelector('[data-ask2-form]');
    const input = document.querySelector('[data-ask2-input]');
    const thinking = document.querySelector('[data-ask2-thinking]');
    const errorBanner = document.querySelector('[data-ask2-error]');
    const statusText = document.querySelector('[data-ask2-status-text]');
    const sendButton = document.querySelector('[data-ask2-send]');

    if (!messagesEl || !form || !input) return;

    const apiUrl = form.dataset.ask2Api || '/ask2/api/';

    function getCSRFToken() {
      const cookieMatch = document.cookie.match(/csrftoken=([^;]+)/);
      if (cookieMatch) return cookieMatch[1];
      const tokenInput = form.querySelector('input[name="csrfmiddlewaretoken"]');
      return tokenInput ? tokenInput.value : null;
    }

    function appendBubble(role, text, muted = false) {
      const bubble = document.createElement('div');
      bubble.className = `bubble bubble--${role}${muted ? ' bubble--muted' : ''}`;
      bubble.textContent = text;
      messagesEl.appendChild(bubble);
      messagesEl.scrollTop = messagesEl.scrollHeight;
      return bubble;
    }

    function setThinking(isThinking) {
      if (thinking) thinking.hidden = !isThinking;
      if (sendButton) sendButton.disabled = isThinking;
    }

    function setStatus(message, ok = true) {
      if (statusText) statusText.textContent = message;
      if (statusText?.parentElement) {
        statusText.parentElement.classList.toggle('is-error', !ok);
      }
    }

    function showError(message) {
      if (!errorBanner) return;
      errorBanner.hidden = false;
      errorBanner.textContent = message;
    }

    function hideError() {
      if (errorBanner) errorBanner.hidden = true;
    }

    async function sendMessage(text) {
      hideError();
      setThinking(true);
      setStatus('Assistant is thinking…');

      const placeholder = appendBubble('assistant', 'Assistant is thinking…', true);

      try {
        const response = await fetch(apiUrl, {
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
          const detail = data.message || data.error || `Request failed (${response.status})`;
          throw new Error(detail);
        }

        const reply = data.reply || data.answer || data.content || 'The assistant did not return a response.';
        placeholder.textContent = reply;
        placeholder.classList.remove('bubble--muted');
        setStatus('Ready to chat');
      } catch (error) {
        const friendly = 'The assistant is temporarily unavailable. Please try again shortly.';
        placeholder.textContent = friendly;
        placeholder.classList.remove('bubble--muted');
        showError(friendly);
        setStatus(error?.message || friendly, false);
      } finally {
        setThinking(false);
      }
    }

    form.addEventListener('submit', (event) => {
      event.preventDefault();
      const text = (input.value || '').trim();
      if (!text) {
        input.focus();
        return;
      }

      appendBubble('user', text);
      input.value = '';
      sendMessage(text);
    });

    input.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        form.requestSubmit();
      }
    });
  });
})();

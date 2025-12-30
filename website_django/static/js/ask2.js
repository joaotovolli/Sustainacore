(function () {
  document.addEventListener('DOMContentLoaded', () => {
    const messagesEl = document.querySelector('[data-ask2-messages]');
    const form = document.querySelector('[data-ask2-form]');
    const input = document.querySelector('[data-ask2-input]');
    const thinking = document.querySelector('[data-ask2-thinking]');
    const errorBanner = document.querySelector('[data-ask2-error]');
    const statusText = document.querySelector('[data-ask2-status-text]');
    const sendButton = document.querySelector('[data-ask2-send]');
    const examples = document.querySelectorAll('[data-ask2-example]');
    const jumpButton = document.querySelector('[data-ask2-jump]');

    if (!messagesEl || !form || !input) return;
    window.SCTelemetry?.track?.('ask2_opened', { page: window.location.pathname });

    const apiUrl = form.dataset.ask2Api || '/ask2/api/';

    function getCSRFToken() {
      const cookieMatch = document.cookie.match(/csrftoken=([^;]+)/);
      if (cookieMatch) return cookieMatch[1];
      const tokenInput = form.querySelector('input[name="csrfmiddlewaretoken"]');
      return tokenInput ? tokenInput.value : null;
    }

    function isNearBottom() {
      if (!messagesEl) return true;
      const distance = messagesEl.scrollHeight - messagesEl.scrollTop - messagesEl.clientHeight;
      return distance < 120;
    }

    function updateJumpVisibility() {
      if (!jumpButton) return;
      jumpButton.hidden = isNearBottom();
    }

    function scrollToBottom() {
      if (!messagesEl) return;
      messagesEl.scrollTop = messagesEl.scrollHeight;
      updateJumpVisibility();
    }

    function appendBubble(role, text, muted = false) {
      const shouldScroll = isNearBottom();
      const bubble = document.createElement('div');
      bubble.className = `bubble bubble--${role}${muted ? ' bubble--muted' : ''}`;
      bubble.textContent = text;
      messagesEl.appendChild(bubble);
      if (shouldScroll) {
        scrollToBottom();
      } else {
        updateJumpVisibility();
      }
      return bubble;
    }

    function stripInternalIds(text) {
      return text.replace(/\(ID:[^)]+\)/gi, '').trim();
    }

    function appendTextWithLinks(parent, text) {
      const urlRegex = /https?:\/\/[^\s]+/g;
      let lastIndex = 0;
      let match;

      while ((match = urlRegex.exec(text)) !== null) {
        if (match.index > lastIndex) {
          parent.appendChild(document.createTextNode(text.slice(lastIndex, match.index)));
        }

        let url = match[0];
        let trailing = '';
        while (/[).,;]+$/.test(url)) {
          trailing = url.slice(-1) + trailing;
          url = url.slice(0, -1);
        }

        const link = document.createElement('a');
        link.href = url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.textContent = url;
        parent.appendChild(link);

        if (trailing) {
          parent.appendChild(document.createTextNode(trailing));
        }
        lastIndex = match.index + match[0].length;
      }

      if (lastIndex < text.length) {
        parent.appendChild(document.createTextNode(text.slice(lastIndex)));
      }
    }

    function renderAsk2Message(rawText) {
      const fragment = document.createDocumentFragment();
      const cleaned = stripInternalIds(rawText || '').replace(/\r\n/g, '\n');
      const lines = cleaned.split('\n');
      let i = 0;

      function flushParagraph(paragraphLines) {
        if (!paragraphLines.length) return;
        const p = document.createElement('p');
        appendTextWithLinks(p, paragraphLines.join(' ').trim());
        fragment.appendChild(p);
        paragraphLines.length = 0;
      }

      while (i < lines.length) {
        const line = lines[i].trim();
        if (!line) {
          i += 1;
          continue;
        }

        const headingMatch = line.match(/^\*\*(.+?)\*\*$/);
        if (headingMatch) {
          const heading = document.createElement('div');
          heading.className = 'ask2-h';
          heading.textContent = headingMatch[1].trim();
          fragment.appendChild(heading);
          i += 1;
          continue;
        }

        if (/^-\s+/.test(line)) {
          const ul = document.createElement('ul');
          while (i < lines.length && /^-\s+/.test(lines[i].trim())) {
            const li = document.createElement('li');
            appendTextWithLinks(li, lines[i].trim().replace(/^-+\s+/, ''));
            ul.appendChild(li);
            i += 1;
          }
          fragment.appendChild(ul);
          continue;
        }

        if (/^\d+\.\s+/.test(line)) {
          const ol = document.createElement('ol');
          while (i < lines.length && /^\d+\.\s+/.test(lines[i].trim())) {
            const li = document.createElement('li');
            appendTextWithLinks(li, lines[i].trim().replace(/^\d+\.\s+/, ''));
            ol.appendChild(li);
            i += 1;
          }
          fragment.appendChild(ol);
          continue;
        }

        const paragraphLines = [];
        while (i < lines.length) {
          const current = lines[i].trim();
          if (
            !current
            || /^\*\*(.+?)\*\*$/.test(current)
            || /^-\s+/.test(current)
            || /^\d+\.\s+/.test(current)
          ) {
            break;
          }
          paragraphLines.push(current);
          i += 1;
        }
        flushParagraph(paragraphLines);
      }

      if (!fragment.childNodes.length) {
        const fallback = document.createElement('p');
        fallback.textContent = cleaned;
        fragment.appendChild(fallback);
      }

      return fragment;
    }

    let isSending = false;

    function setThinking(isThinking) {
      isSending = isThinking;
      if (thinking) thinking.hidden = !isThinking;
      if (sendButton) sendButton.disabled = isThinking;
      if (input) input.disabled = isThinking;
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
      if (isSending) return;
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
        placeholder.replaceChildren(renderAsk2Message(reply));
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
      if (isSending) return;
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

    messagesEl.addEventListener('scroll', () => {
      updateJumpVisibility();
    });

    if (jumpButton) {
      jumpButton.addEventListener('click', () => {
        scrollToBottom();
      });
    }

    examples.forEach((example) => {
      example.addEventListener('click', () => {
        const value = example.dataset.ask2Example || '';
        if (!value) return;
        input.value = value;
        input.focus();
        input.setSelectionRange(value.length, value.length);
      });
    });

    setThinking(false);
    updateJumpVisibility();
  });
})();

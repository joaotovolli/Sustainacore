(function () {
  const init = () => {
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

    const COLLAPSE_THRESHOLD = 1200;
    const COLLAPSE_CLASS = 'is-collapsed';

    function stripInternalIds(text) {
      return text.replace(/\(ID:[^)]+\)/gi, '').trim();
    }

    function sanitizeUrl(rawUrl) {
      if (!rawUrl) return null;
      try {
        const url = new URL(rawUrl, window.location.origin);
        if (url.protocol === 'http:' || url.protocol === 'https:') {
          return url.href;
        }
      } catch (_) {
        return null;
      }
      return null;
    }

    function normalizeSources(payload) {
      const candidates = payload.sources || payload.contexts || [];
      if (!Array.isArray(candidates)) return [];
      return candidates
        .map((source, index) => {
          if (!source || typeof source !== 'object') return null;
          const title = String(source.title || source.name || source.label || `Source ${index + 1}`).trim();
          const url = sanitizeUrl(source.url || source.link || source.href || '');
          const snippet = String(source.snippet || source.summary || '').trim();
          return { title, url, snippet, score: source.score };
        })
        .filter((source) => source && (source.title || source.url || source.snippet));
    }

    function appendInlineNodes(parent, text, sourceCount = 0) {
      const tokenPattern = /(\[[^\]]+\]\([^)]+\)|\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`|\[\d+\]|https?:\/\/[^\s]+)/g;
      let lastIndex = 0;
      let match;

      function appendText(value) {
        if (!value) return;
        parent.appendChild(document.createTextNode(value));
      }

      while ((match = tokenPattern.exec(text)) !== null) {
        if (match.index > lastIndex) {
          appendText(text.slice(lastIndex, match.index));
        }

        const token = match[0];
        if (token.startsWith('[') && token.includes('](')) {
          const parts = token.match(/^\[([^\]]+)\]\(([^)]+)\)$/);
          const label = parts ? parts[1].trim() : token;
          const url = parts ? sanitizeUrl(parts[2].trim()) : null;
          if (url) {
            const link = document.createElement('a');
            link.href = url;
            link.target = '_blank';
            link.rel = 'noopener noreferrer';
            link.textContent = label || url;
            parent.appendChild(link);
          } else {
            appendText(label);
          }
        } else if (token.startsWith('**') && token.endsWith('**')) {
          const strong = document.createElement('strong');
          strong.textContent = token.slice(2, -2);
          parent.appendChild(strong);
        } else if (token.startsWith('*') && token.endsWith('*') && token.length > 2) {
          const em = document.createElement('em');
          em.textContent = token.slice(1, -1);
          parent.appendChild(em);
        } else if (token.startsWith('`') && token.endsWith('`')) {
          const code = document.createElement('code');
          code.textContent = token.slice(1, -1);
          parent.appendChild(code);
        } else if (/^\[\d+\]$/.test(token)) {
          const index = Number(token.slice(1, -1));
          if (index && index <= sourceCount) {
            const anchor = document.createElement('a');
            anchor.href = `#ask2-source-${index}`;
            anchor.className = 'ask2-citation';
            anchor.textContent = token;
            parent.appendChild(anchor);
          } else {
            appendText(token);
          }
        } else if (token.startsWith('http')) {
          let url = token;
          let trailing = '';
          while (/[).,;]+$/.test(url)) {
            trailing = url.slice(-1) + trailing;
            url = url.slice(0, -1);
          }
          const safeUrl = sanitizeUrl(url);
          if (safeUrl) {
            const link = document.createElement('a');
            link.href = safeUrl;
            link.target = '_blank';
            link.rel = 'noopener noreferrer';
            link.textContent = url;
            parent.appendChild(link);
          } else {
            appendText(url);
          }
          appendText(trailing);
        } else {
          appendText(token);
        }

        lastIndex = match.index + token.length;
      }

      if (lastIndex < text.length) {
        appendText(text.slice(lastIndex));
      }
    }

    function renderAsk2Message(rawText, sourceCount = 0) {
      const fragment = document.createDocumentFragment();
      const cleaned = stripInternalIds(rawText || '').replace(/\r\n/g, '\n');
      const lines = cleaned.split('\n');
      let i = 0;

      function flushParagraph(paragraphLines) {
        if (!paragraphLines.length) return;
        const p = document.createElement('p');
        appendInlineNodes(p, paragraphLines.join(' ').trim(), sourceCount);
        fragment.appendChild(p);
        paragraphLines.length = 0;
      }

      while (i < lines.length) {
        const line = lines[i].trim();
        if (!line) {
          i += 1;
          continue;
        }

        if (line.startsWith('```')) {
          const codeLines = [];
          i += 1;
          while (i < lines.length && !lines[i].trim().startsWith('```')) {
            codeLines.push(lines[i]);
            i += 1;
          }
          if (i < lines.length) i += 1;
          const pre = document.createElement('pre');
          const code = document.createElement('code');
          code.textContent = codeLines.join('\n').trimEnd();
          pre.appendChild(code);
          fragment.appendChild(pre);
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

        const mdHeading = line.match(/^#{1,6}\s+(.+)$/);
        if (mdHeading) {
          const heading = document.createElement('div');
          heading.className = 'ask2-h';
          appendInlineNodes(heading, mdHeading[1].trim(), sourceCount);
          fragment.appendChild(heading);
          i += 1;
          continue;
        }

        if (/^[-*]\s+/.test(line)) {
          const ul = document.createElement('ul');
          while (i < lines.length && /^[-*]\s+/.test(lines[i].trim())) {
            const li = document.createElement('li');
            appendInlineNodes(li, lines[i].trim().replace(/^[-*]\s+/, ''), sourceCount);
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
            appendInlineNodes(li, lines[i].trim().replace(/^\d+\.\s+/, ''), sourceCount);
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
            || current.startsWith('```')
            || /^\*\*(.+?)\*\*$/.test(current)
            || /^#{1,6}\s+/.test(current)
            || /^[-*]\s+/.test(current)
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
        appendInlineNodes(fallback, cleaned, sourceCount);
        fragment.appendChild(fallback);
      }

      return fragment;
    }

    function buildSourcesList(sources) {
      if (!sources.length) return null;
      const wrapper = document.createElement('div');
      wrapper.className = 'ask2-sources';

      const heading = document.createElement('div');
      heading.className = 'ask2-sources__title';
      heading.textContent = 'Sources';
      wrapper.appendChild(heading);

      const list = document.createElement('ol');
      list.className = 'ask2-sources__list';
      wrapper.appendChild(list);
      sources.forEach((source, index) => {
        const li = document.createElement('li');
        li.id = `ask2-source-${index + 1}`;
        const title = document.createElement(source.url ? 'a' : 'span');
        title.className = 'ask2-source__title';
        title.textContent = source.title || `Source ${index + 1}`;
        if (source.url) {
          title.href = source.url;
          title.target = '_blank';
          title.rel = 'noopener noreferrer';
        }
        li.appendChild(title);
        if (source.snippet) {
          const snippet = document.createElement('div');
          snippet.className = 'ask2-source__snippet';
          snippet.textContent = source.snippet;
          li.appendChild(snippet);
        }
        list.appendChild(li);
      });

      return wrapper;
    }

    function renderAssistantBubble(bubble, replyText, sources) {
      const container = document.createElement('div');
      container.className = 'ask2-bubble';
      const content = document.createElement('div');
      content.className = 'ask2-bubble__content';
      content.appendChild(renderAsk2Message(replyText, sources.length));
      const fade = document.createElement('div');
      fade.className = 'ask2-bubble__fade';
      content.appendChild(fade);
      container.appendChild(content);

      let toggle = null;
      const shouldCollapse = replyText.length > COLLAPSE_THRESHOLD;
      if (shouldCollapse) {
        bubble.classList.add(COLLAPSE_CLASS);
        toggle = document.createElement('button');
        toggle.type = 'button';
        toggle.className = 'ask2-bubble__toggle';
        toggle.setAttribute('aria-expanded', 'false');
        toggle.textContent = 'Show more';
        toggle.addEventListener('click', () => {
          const isCollapsed = bubble.classList.toggle(COLLAPSE_CLASS);
          toggle.textContent = isCollapsed ? 'Show more' : 'Show less';
          toggle.setAttribute('aria-expanded', (!isCollapsed).toString());
        });
        container.appendChild(toggle);
      }

      const sourcesBlock = buildSourcesList(sources);
      if (sourcesBlock) {
        container.appendChild(sourcesBlock);
      }

      bubble.replaceChildren(container);
      if (!shouldCollapse) {
        bubble.classList.remove(COLLAPSE_CLASS);
      }
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
        const sources = normalizeSources(data);
        renderAssistantBubble(placeholder, reply, sources);
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
    document.documentElement.dataset.ask2Ready = 'true';

    if (typeof window !== 'undefined') {
      window.SCAsk2 = window.SCAsk2 || {};
      window.SCAsk2.renderMock = (payload, userMessage = '') => {
        if (userMessage) {
          appendBubble('user', userMessage);
        }
        const reply = payload?.reply || payload?.answer || payload?.content || '';
        const sources = normalizeSources(payload || {});
        const bubble = appendBubble('assistant', '');
        renderAssistantBubble(bubble, reply, sources);
        bubble.classList.remove('bubble--muted');
        scrollToBottom();
      };
    }
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

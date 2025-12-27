(() => {
  const modal = document.querySelector("[data-download-modal]");
  document.documentElement.dataset.downloadGate = "ready";
  if (!modal) {
    return;
  }

  const body = document.body;
  const hasSessionCookie = () => document.cookie.includes("sc_session=");
  const isLoggedIn = () => body?.dataset?.loggedIn === "true" || hasSessionCookie();
  const emailStep = modal.querySelector("[data-step='email']");
  const codeStep = modal.querySelector("[data-step='code']");
  const emailInput = modal.querySelector("[data-email-input]");
  const codeInput = modal.querySelector("[data-code-input]");
  const errorBox = modal.querySelector("[data-modal-error]");
  const closeButtons = modal.querySelectorAll("[data-modal-close]");
  const resendButton = modal.querySelector("[data-resend]");
  const sendButton = modal.querySelector("[data-send-code]");
  const verifyButton = modal.querySelector("[data-verify-code]");
  const emailDisplay = modal.querySelector("[data-email-display]");
  const focusableSelector = "button, [href], input, select, textarea, [tabindex]:not([tabindex='-1'])";

  const pendingKey = "sc_pending_download";
  const pendingNextKey = "sc_pending_next";

  const getCookie = (name) => {
    const match = document.cookie.match(new RegExp(`(^| )${name}=([^;]+)`));
    return match ? decodeURIComponent(match[2]) : "";
  };

  const sendEvent = async (eventType, metadata = {}) => {
    try {
      await fetch("/api/ux-event/", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        body: JSON.stringify({ event_type: eventType, metadata }),
      });
    } catch (err) {
      // Analytics must never block UX.
    }
  };

  const showError = (message) => {
    if (!errorBox) return;
    errorBox.textContent = message;
    errorBox.hidden = !message;
  };

  const trapFocus = (container, event) => {
    const focusable = Array.from(container.querySelectorAll(focusableSelector)).filter(
      (el) => !el.hasAttribute("disabled")
    );
    if (!focusable.length) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  };

  const openModal = () => {
    modal.setAttribute("aria-hidden", "false");
    modal.classList.add("modal--open");
    showEmailStep();
    setTimeout(() => emailInput?.focus(), 50);
  };

  const closeModal = () => {
    modal.setAttribute("aria-hidden", "true");
    modal.classList.remove("modal--open");
    showError("");
  };

  const showEmailStep = () => {
    emailStep.hidden = false;
    codeStep.hidden = true;
    showError("");
    codeInput.value = "";
  };

  const showCodeStep = (email) => {
    emailStep.hidden = true;
    codeStep.hidden = false;
    emailDisplay.textContent = email;
    showError("");
    setTimeout(() => codeInput?.focus(), 50);
  };

  const storePending = (downloadUrl) => {
    sessionStorage.setItem(pendingKey, downloadUrl);
    sessionStorage.setItem(pendingNextKey, window.location.pathname + window.location.search);
  };

  const getPending = () => sessionStorage.getItem(pendingKey);

  const clearPending = () => {
    sessionStorage.removeItem(pendingKey);
    sessionStorage.removeItem(pendingNextKey);
  };

  const triggerPendingDownload = () => {
    const pending = getPending();
    if (pending && isLoggedIn()) {
      clearPending();
      window.location.href = pending;
    }
  };

  const handleDownloadClick = (event, link, href) => {
    if (!href) return;
    sendEvent("download_click", { download: href, page: window.location.pathname });
    event.preventDefault();
    event.stopPropagation();
    if (isLoggedIn()) {
      window.location.assign(href);
      return;
    }
    storePending(href);
    sendEvent("download_blocked", { download: href, page: window.location.pathname });
    openModal();
  };

  const getDownloadTarget = (element) => {
    if (!element) return null;
    const container = element.closest?.("[data-download-url]");
    if (container?.getAttribute) {
      const value = container.getAttribute("data-download-url");
      if (value) return value;
    }
    if (element.closest) {
      const anchor = element.closest("a");
      if (anchor?.getAttribute) {
        const href = anchor.getAttribute("href") || "";
        if (href) return href;
      }
    }
    if (element.hasAttribute?.("data-download-url")) {
      return element.getAttribute("data-download-url");
    }
    if (element.getAttribute) {
      const href = element.getAttribute("href") || "";
      if (href) return href;
    }
    return null;
  };

  const isDownloadLink = (element, href) => {
    if (!element) return false;
    const value = href || "";
    return (
      value.includes("/export/") ||
      value.includes("format=csv") ||
      value.endsWith(".csv") ||
      element.hasAttribute?.("data-download") ||
      element.hasAttribute?.("data-download-url")
    );
  };

  document.addEventListener(
    "click",
    (event) => {
      const target = event.target.closest("a, button, [data-download-url], [data-download]");
      if (!target) return;
      const href = getDownloadTarget(target) || "";
      if (isDownloadLink(target, href)) {
        handleDownloadClick(event, target, href);
      }
    },
    true
  );

  closeButtons.forEach((button) => {
    button.addEventListener("click", () => closeModal());
  });

  modal.addEventListener("click", (event) => {
    if (event.target === modal) {
      closeModal();
    }
  });

  modal.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeModal();
      return;
    }
    if (event.key === "Tab") {
      trapFocus(modal, event);
    }
  });

  sendButton?.addEventListener("click", async (event) => {
    event.preventDefault();
    const email = (emailInput.value || "").trim().toLowerCase();
    if (!email) {
      showError("Please enter a valid email.");
      return;
    }
    showError("");
    sendButton.disabled = true;
    try {
      const resp = await fetch("/auth/request-code/", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        body: JSON.stringify({ email }),
      });
      if (!resp.ok) {
        showError("We could not send a code right now. Please try again.");
        sendButton.disabled = false;
        return;
      }
      showCodeStep(email);
    } catch (err) {
      showError("We could not send a code right now. Please try again.");
    } finally {
      sendButton.disabled = false;
    }
  });

  resendButton?.addEventListener("click", async (event) => {
    event.preventDefault();
    const email = (emailInput.value || "").trim().toLowerCase();
    if (!email) {
      showError("Please enter a valid email.");
      return;
    }
    showError("");
    try {
      await fetch("/auth/request-code/", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        body: JSON.stringify({ email }),
      });
    } catch (err) {
      showError("We could not resend the code. Please try again.");
    }
  });

  verifyButton?.addEventListener("click", async (event) => {
    event.preventDefault();
    const email = (emailInput.value || "").trim().toLowerCase();
    const code = (codeInput.value || "").trim();
    if (!email || code.length < 6) {
      showError("Enter the 6-digit code we emailed you.");
      return;
    }
    showError("");
    verifyButton.disabled = true;
    try {
      const resp = await fetch("/auth/verify-code/", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        body: JSON.stringify({ email, code }),
      });
      if (!resp.ok) {
        showError("Invalid or expired code. Please try again.");
        verifyButton.disabled = false;
        return;
      }
      body.dataset.loggedIn = "true";
      closeModal();
      triggerPendingDownload();
    } catch (err) {
      showError("We could not verify the code right now. Please try again.");
    } finally {
      verifyButton.disabled = false;
    }
  });

  const params = new URLSearchParams(window.location.search);
  if (params.get("download_login") === "1") {
    const downloadUrl = params.get("download_url");
    if (downloadUrl) {
      storePending(downloadUrl);
    }
    if (!isLoggedIn()) {
      openModal();
    }
  }

  if (getPending() && !isLoggedIn()) {
    openModal();
  } else {
    triggerPendingDownload();
  }
})();

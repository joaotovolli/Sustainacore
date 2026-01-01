(() => {
  const modal = document.getElementById("download-auth-modal");
  document.documentElement.dataset.downloadGate = "ready";
  if (!modal) {
    return;
  }

  const body = document.body;
  const hasSessionCookie = () => document.cookie.includes("sc_session=");
  const isLoggedIn = () => body?.dataset?.loggedIn === "true" || hasSessionCookie();
  const debugEnabled = new URLSearchParams(window.location.search).get("dg_debug") === "1";
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
  const termsCheckbox = modal.querySelector("[data-terms-accept]");
  const focusableSelector = "button, [href], input, select, textarea, [tabindex]:not([tabindex='-1'])";

  const pendingKey = "sc_pending_download";
  const pendingNextKey = "sc_pending_next";
  const intentKey = "sc_post_auth_intent";
  const intentNextKey = "sc_post_auth_next";
  const detailsIntentKey = "sc_post_auth_details";
  const unlockMetaKey = "sc_unlock_meta";
  const unlockSuccessKey = "sc_unlock_success";

  const getCookie = (name) => {
    const match = document.cookie.match(new RegExp(`(^| )${name}=([^;]+)`));
    return match ? decodeURIComponent(match[2]) : "";
  };

  const sendEvent = (eventType, metadata = {}) => {
    const tracker = window.SCTelemetry;
    if (!tracker || typeof tracker.track !== "function") {
      return;
    }
    tracker.track(eventType, metadata);
  };

  const showDebugToast = (message) => {
    if (!debugEnabled) return;
    const existing = document.getElementById("dg-debug-toast");
    if (existing) existing.remove();
    const toast = document.createElement("div");
    toast.id = "dg-debug-toast";
    toast.textContent = message;
    toast.style.position = "fixed";
    toast.style.bottom = "24px";
    toast.style.right = "24px";
    toast.style.zIndex = "9999";
    toast.style.padding = "10px 14px";
    toast.style.background = "rgba(15, 23, 42, 0.92)";
    toast.style.color = "#fff";
    toast.style.borderRadius = "10px";
    toast.style.fontSize = "12px";
    toast.style.boxShadow = "0 10px 30px rgba(15,23,42,0.35)";
    document.body.appendChild(toast);
    setTimeout(() => {
      toast.remove();
    }, 2500);
  };

  const showError = (message) => {
    if (!errorBox) return;
    errorBox.textContent = message;
    errorBox.hidden = !message;
  };

  const hasAcceptedTerms = () => !termsCheckbox || termsCheckbox.checked;

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
    modal.hidden = false;
    modal.setAttribute("aria-hidden", "false");
    modal.classList.add("modal--open", "is-visible");
    document.body.classList.add("has-download-modal");
    showEmailStep();
    setTimeout(() => emailInput?.focus(), 50);
    return modal.classList.contains("modal--open");
  };

  const closeModal = (options = {}) => {
    const shouldClearPending = options.clearPending !== false;
    modal.setAttribute("aria-hidden", "true");
    modal.classList.remove("modal--open", "is-visible");
    document.body.classList.remove("has-download-modal");
    modal.hidden = true;
    showError("");
    if (shouldClearPending) {
      clearPending();
      clearPostAuthIntent();
      sessionStorage.removeItem(unlockMetaKey);
      sessionStorage.removeItem(unlockSuccessKey);
    }
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

  const setPostAuthIntent = (intentType, nextUrl) => {
    sessionStorage.setItem(intentKey, intentType);
    sessionStorage.setItem(intentNextKey, nextUrl || window.location.href);
  };

  const getPostAuthIntent = () => ({
    type: sessionStorage.getItem(intentKey),
    nextUrl: sessionStorage.getItem(intentNextKey),
  });

  const clearPostAuthIntent = () => {
    sessionStorage.removeItem(intentKey);
    sessionStorage.removeItem(intentNextKey);
  };

  const setDetailsIntent = (payload) => {
    try {
      sessionStorage.setItem(detailsIntentKey, JSON.stringify(payload || {}));
    } catch (err) {
      // ignore storage errors
    }
  };

  const popDetailsIntent = () => {
    const raw = sessionStorage.getItem(detailsIntentKey);
    sessionStorage.removeItem(detailsIntentKey);
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch (err) {
      return null;
    }
  };

  const storeUnlockMeta = (metadata) => {
    try {
      sessionStorage.setItem(unlockMetaKey, JSON.stringify(metadata || {}));
    } catch (err) {
      // Ignore storage errors.
    }
  };

  const popUnlockMeta = () => {
    const raw = sessionStorage.getItem(unlockMetaKey);
    sessionStorage.removeItem(unlockMetaKey);
    if (!raw) return {};
    try {
      return JSON.parse(raw);
    } catch (err) {
      return {};
    }
  };

  const handlePostAuthSuccess = () => {
    const pending = getPending();
    if (pending && isLoggedIn()) {
      clearPending();
      window.location.assign(pending);
      return;
    }
    const intent = getPostAuthIntent();
    if (intent.type === "unlock_table") {
      sessionStorage.setItem(unlockSuccessKey, "1");
      clearPostAuthIntent();
      if (intent.nextUrl) {
        window.location.assign(intent.nextUrl);
      } else {
        window.location.reload();
      }
    }
    if (intent.type === "details_modal" && isLoggedIn()) {
      clearPostAuthIntent();
      const detailsPayload = popDetailsIntent();
      if (detailsPayload) {
        window.dispatchEvent(new CustomEvent("sc:open-tech100-details", { detail: detailsPayload }));
      }
    }
  };

  const handleDownloadClick = (event, link, href) => {
    if (!href) return;
    sendEvent("download_click", {
      resource: href,
      page: window.location.pathname,
      gated: !isLoggedIn(),
    });
    event.preventDefault();
    event.stopPropagation();
    clearPostAuthIntent();
    if (isLoggedIn()) {
      showDebugToast(`Download detected: ${href} | logged_in=true | action=nav`);
      window.location.assign(href);
      return;
    }
    showDebugToast(`Download detected: ${href} | logged_in=false | action=modal`);
    storePending(href);
    sendEvent("download_blocked", { download: href, page: window.location.pathname });
    const opened = openModal();
    if (!opened) {
      window.location.assign(href);
    }
  };

  const gateDetails = (payload) => {
    if (isLoggedIn()) {
      return false;
    }
    clearPostAuthIntent();
    setPostAuthIntent("details_modal", window.location.href);
    setDetailsIntent(payload);
    openModal();
    return true;
  };

  const isDownloadUrl = (href) => {
    if (!href) return false;
    return href.includes("/export/") || href.includes("format=csv") || href.endsWith(".csv");
  };

  const getDownloadTarget = (element) => {
    if (!element) return null;
    const anchor = element.closest?.("a");
    if (anchor?.getAttribute) {
      const href = anchor.getAttribute("href") || "";
      if (isDownloadUrl(href)) {
        return href;
      }
    }
    const container = element.closest?.("[data-download-url]");
    if (container?.getAttribute) {
      const value = container.getAttribute("data-download-url");
      if (value) return value;
    }
    if (element.hasAttribute?.("data-download-url")) {
      return element.getAttribute("data-download-url");
    }
    return null;
  };

  document.addEventListener("click", (event) => {
    const trigger = event.target.closest("[data-open-download-modal]");
    if (!trigger) return;
    event.preventDefault();
    const intent = trigger.getAttribute("data-intent") || "";
    const nextUrl = trigger.getAttribute("data-next-url") || window.location.href;
    if (intent === "unlock_table") {
      setPostAuthIntent("unlock_table", nextUrl);
      const metadata = {
        page: trigger.getAttribute("data-page") || window.location.pathname,
        port_date: trigger.getAttribute("data-port-date") || "",
        sector: trigger.getAttribute("data-sector") || "",
        q: trigger.getAttribute("data-q") || "",
        preview_limit: trigger.getAttribute("data-preview-limit") || "",
      };
      storeUnlockMeta(metadata);
      sendEvent("table_unlock_click", metadata);
    }
    openModal();
  });

  document.addEventListener(
    "click",
    (event) => {
      const target = event.target.closest("a, button, [data-download-url], [data-download]");
      if (!target) return;
      const href = getDownloadTarget(target) || "";
      if (!href) {
        return;
      }
      handleDownloadClick(event, target, href);
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

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && modal.classList.contains("modal--open")) {
      closeModal();
    }
  });

  sendButton?.addEventListener("click", async (event) => {
    event.preventDefault();
    const email = (emailInput.value || "").trim().toLowerCase();
    if (!email) {
      showError("Please enter a valid email.");
      return;
    }
    if (!hasAcceptedTerms()) {
      showError("Please agree to the Terms and Privacy Policy to receive a code.");
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
        body: JSON.stringify({ email, terms_accepted: true }),
      });
      let data = null;
      try {
        data = await resp.json();
      } catch (err) {
        data = null;
      }
      if (!resp.ok || data?.ok === false) {
        let message = "We could not send a code right now. Please try again.";
        if (data?.error === "terms_required") {
          message = "Please agree to the Terms and Privacy Policy to receive a code.";
        } else if (data?.error === "terms_acceptance_failed") {
          message = "We could not record your acceptance. Please try again.";
        } else if (data?.error === "rate_limited") {
          message = "Too many requests. Please wait a bit and try again.";
        } else if (data?.message) {
          message = data.message;
        }
        showError(message);
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
    if (!hasAcceptedTerms()) {
      showError("Please agree to the Terms and Privacy Policy to receive a code.");
      return;
    }
    showError("");
    try {
      const resp = await fetch("/auth/request-code/", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        body: JSON.stringify({ email, terms_accepted: true }),
      });
      let data = null;
      try {
        data = await resp.json();
      } catch (err) {
        data = null;
      }
      if (!resp.ok || data?.ok === false) {
        let message = "We could not resend the code. Please try again.";
        if (data?.error === "terms_required") {
          message = "Please agree to the Terms and Privacy Policy to receive a code.";
        } else if (data?.error === "terms_acceptance_failed") {
          message = "We could not record your acceptance. Please try again.";
        } else if (data?.error === "rate_limited") {
          message = "Too many requests. Please wait a bit and try again.";
        } else if (data?.message) {
          message = data.message;
        }
        showError(message);
      }
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
        let message = "Invalid or expired code. Please try again.";
        try {
          const data = await resp.json();
          if (data?.error === "terms_required") {
            message = "Please accept the Terms and Privacy Policy before signing in.";
          }
        } catch (err) {
          // ignore JSON errors
        }
        showError(message);
        verifyButton.disabled = false;
        return;
      }
      body.dataset.loggedIn = "true";
      closeModal({ clearPending: false });
      handlePostAuthSuccess();
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

  const unlockIntent = getPostAuthIntent();
  if (getPending() && !isLoggedIn()) {
    openModal();
  } else if (unlockIntent.type === "unlock_table" && !isLoggedIn()) {
    openModal();
  } else {
    handlePostAuthSuccess();
  }

  if (isLoggedIn() && sessionStorage.getItem(unlockSuccessKey) === "1") {
    sessionStorage.removeItem(unlockSuccessKey);
    const metadata = popUnlockMeta();
    sendEvent("table_unlock_success", {
      page: metadata.page || window.location.pathname,
      port_date: metadata.port_date || "",
      sector: metadata.sector || "",
      q: metadata.q || "",
      preview_limit: metadata.preview_limit || "",
    });
  }

  window.SCDownloadGate = {
    isLoggedIn,
    gateDetails,
  };
})();

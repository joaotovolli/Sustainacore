(() => {
  const CONSENT_COOKIE = "sc_consent";
  const POLICY_VERSION = (window.SC_TELEMETRY_POLICY_VERSION || "").toString();
  const allowedEvents = new Set([
    "filter_applied",
    "search_submitted",
    "download_click",
    "ask2_opened",
    "tab_changed",
  ]);

  const banner = document.querySelector("[data-consent-banner]");
  const modal = document.querySelector("[data-consent-modal]");
  const analyticsToggle = modal?.querySelector("[data-consent-analytics]");
  const functionalToggle = modal?.querySelector("[data-consent-functional]");
  const closeButton = modal?.querySelector("[data-consent-close]");
  const saveButton = modal?.querySelector("[data-consent-save]");
  const acceptAllButton = modal?.querySelector("[data-consent-accept-all]");
  const rejectAllButton = modal?.querySelector("[data-consent-reject-all]");

  const getCookie = (name) => {
    const match = document.cookie.match(new RegExp(`(^| )${name}=([^;]+)`));
    return match ? decodeURIComponent(match[2]) : "";
  };

  const setCookie = (name, value, maxAgeSeconds) => {
    const secureFlag = window.location.protocol === "https:" ? ";Secure" : "";
    document.cookie = `${name}=${encodeURIComponent(value)};Max-Age=${maxAgeSeconds};Path=/;SameSite=Lax${secureFlag}`;
  };

  const parseConsent = () => {
    const raw = getCookie(CONSENT_COOKIE);
    if (!raw) return null;
    try {
      const payload = JSON.parse(raw);
      if (!payload || typeof payload !== "object") return null;
      if (payload.policy_version !== POLICY_VERSION) return null;
      return {
        analytics: Boolean(payload.analytics),
        functional: Boolean(payload.functional),
        policy_version: payload.policy_version,
        source: payload.source || "banner",
      };
    } catch (err) {
      return null;
    }
  };

  const setBodyBannerState = (visible) => {
    document.body.classList.toggle("has-consent-banner", visible);
  };

  const showBanner = () => {
    if (!banner) return;
    banner.hidden = false;
    setBodyBannerState(true);
  };

  const hideBanner = () => {
    if (!banner) return;
    banner.hidden = true;
    setBodyBannerState(false);
  };

  const openModal = () => {
    if (!modal) return;
    modal.hidden = false;
    modal.setAttribute("aria-hidden", "false");
    modal.classList.add("is-visible");
    document.body.classList.add("has-modal-open");
  };

  const closeModal = () => {
    if (!modal) return;
    modal.classList.remove("is-visible");
    modal.setAttribute("aria-hidden", "true");
    modal.hidden = true;
    document.body.classList.remove("has-modal-open");
  };

  const applyToggleState = (consent) => {
    if (!consent) {
      if (analyticsToggle) analyticsToggle.checked = false;
      if (functionalToggle) functionalToggle.checked = false;
      return;
    }
    if (analyticsToggle) analyticsToggle.checked = Boolean(consent.analytics);
    if (functionalToggle) functionalToggle.checked = Boolean(consent.functional);
  };

  const saveConsent = async ({ analytics, functional, source }) => {
    const payload = {
      analytics: Boolean(analytics),
      functional: Boolean(functional),
      source: source || "banner",
    };
    const cookieValue = JSON.stringify({
      analytics: payload.analytics,
      functional: payload.functional,
      policy_version: POLICY_VERSION,
      source: payload.source,
    });
    setCookie(CONSENT_COOKIE, cookieValue, 180 * 24 * 60 * 60);
    try {
      await fetch("/telemetry/consent/", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        body: JSON.stringify(payload),
      });
    } catch (err) {
      // Consent persistence should not block UX.
    }
    window.SCTelemetry?.refresh?.();
    hideBanner();
    closeModal();
  };

  const refreshConsentState = () => {
    const consent = parseConsent();
    if (!consent) {
      showBanner();
      applyToggleState(null);
      return consent;
    }
    hideBanner();
    applyToggleState(consent);
    return consent;
  };

  const trackEvent = async (eventName, metadata = {}) => {
    if (!allowedEvents.has(eventName)) return;
    const consent = parseConsent();
    if (!consent || !consent.analytics) return;
    try {
      await fetch("/telemetry/event/", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        body: JSON.stringify({ event_name: eventName, metadata }),
      });
    } catch (err) {
      // Telemetry must never block UX.
    }
  };

  window.SCTelemetry = {
    refresh: refreshConsentState,
    track: trackEvent,
    openPreferences: () => {
      const consent = parseConsent();
      applyToggleState(consent);
      openModal();
    },
  };

  document.addEventListener("click", (event) => {
    const acceptButton = event.target.closest("[data-consent-accept]");
    if (acceptButton) {
      saveConsent({ analytics: true, functional: false, source: "banner" });
      return;
    }
    const rejectButton = event.target.closest("[data-consent-reject]");
    if (rejectButton) {
      saveConsent({ analytics: false, functional: false, source: "banner" });
      return;
    }
    const manageButton = event.target.closest("[data-consent-manage]");
    if (manageButton) {
      openModal();
      return;
    }
    const openButton = event.target.closest("[data-consent-open]");
    if (openButton) {
      event.preventDefault();
      openModal();
    }
  });

  closeButton?.addEventListener("click", closeModal);

  saveButton?.addEventListener("click", () => {
    saveConsent({
      analytics: Boolean(analyticsToggle?.checked),
      functional: Boolean(functionalToggle?.checked),
      source: "settings",
    });
  });

  acceptAllButton?.addEventListener("click", () => {
    saveConsent({ analytics: true, functional: true, source: "settings" });
  });

  rejectAllButton?.addEventListener("click", () => {
    saveConsent({ analytics: false, functional: false, source: "settings" });
  });

  refreshConsentState();
})();

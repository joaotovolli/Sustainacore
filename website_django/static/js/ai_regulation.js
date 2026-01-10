const NAME_ALIASES = new Map([
  ['unitedstatesofamerica', 'unitedstates'],
  ['russianfederation', 'russia'],
  ['czechia', 'czechrepublic'],
  ['korearepublicof', 'southkorea'],
  ['koreademocraticpeoplesrepublicof', 'northkorea'],
  ['iranislamicrepublicof', 'iran'],
  ['tanzaniaunitedrepublicof', 'tanzania'],
  ['venezuelabolivarianrepublicof', 'venezuela'],
  ['boliviaplurinationalstateof', 'bolivia'],
  ['laopeoplesdemocraticrepublic', 'laos'],
  ['syrianarabrepublic', 'syria'],
  ['moldovarepublicof', 'moldova'],
  ['macedoniatheformeryugoslavrepublicof', 'northmacedonia'],
  ['vietnam', 'vietnam'],
  ['bruneidarussalam', 'brunei']
]);

const GEOJSON_URL = 'https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json';

const state = {
  asOf: null,
  heatmapCache: new Map(),
  jurisdictionCache: new Map(),
  instrumentsCache: new Map(),
  timelineCache: new Map(),
  geoData: null,
  heatmapIndex: new Map(),
  heatmapMax: 1,
  globe: null,
  flatMap: null,
  hover: null,
  mapMode: '3d'
};

const formatNumber = (value) => (Number.isFinite(value) ? value.toLocaleString() : '—');

const escapeHtml = (value) =>
  String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');

const normalizeName = (name) => {
  const normalized = String(name || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
  return NAME_ALIASES.get(normalized) || normalized;
};

const resolveJurisdiction = (feature) => {
  if (!feature?.properties?.name) return null;
  const key = normalizeName(feature.properties.name);
  return state.heatmapIndex.get(key) || null;
};

const getStorageItem = (key) => {
  try {
    const value = window.sessionStorage.getItem(key);
    return value ? JSON.parse(value) : null;
  } catch (error) {
    return null;
  }
};

const setStorageItem = (key, value) => {
  try {
    window.sessionStorage.setItem(key, JSON.stringify(value));
  } catch (error) {
    // no-op
  }
};

const fetchJson = async (url) => {
  const response = await fetch(url, {
    headers: {
      'X-Requested-With': 'XMLHttpRequest'
    }
  });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
};

const getHeatmap = async (endpoint, asOf) => {
  const cacheKey = `aiRegHeatmap:${asOf}`;
  if (state.heatmapCache.has(cacheKey)) {
    return state.heatmapCache.get(cacheKey);
  }
  const cached = getStorageItem(cacheKey);
  if (cached) {
    state.heatmapCache.set(cacheKey, cached);
    return cached;
  }
  const data = await fetchJson(`${endpoint}?as_of=${encodeURIComponent(asOf)}`);
  state.heatmapCache.set(cacheKey, data);
  setStorageItem(cacheKey, data);
  return data;
};

const getJurisdictionBundle = async (endpoint, iso2, asOf) => {
  const cacheKey = `aiRegJurisdiction:${iso2}:${asOf}`;
  if (state.jurisdictionCache.has(cacheKey)) {
    return state.jurisdictionCache.get(cacheKey);
  }
  const data = await fetchJson(`${endpoint}/${iso2}/?as_of=${encodeURIComponent(asOf)}`);
  state.jurisdictionCache.set(cacheKey, data);
  return data;
};

const getJurisdictionInstruments = async (endpoint, iso2, asOf) => {
  const cacheKey = `aiRegInstruments:${iso2}:${asOf}`;
  if (state.instrumentsCache.has(cacheKey)) {
    return state.instrumentsCache.get(cacheKey);
  }
  const data = await fetchJson(`${endpoint}/${iso2}/instruments?as_of=${encodeURIComponent(asOf)}`);
  state.instrumentsCache.set(cacheKey, data);
  return data;
};

const getJurisdictionTimeline = async (endpoint, iso2, asOf) => {
  const cacheKey = `aiRegTimeline:${iso2}:${asOf}`;
  if (state.timelineCache.has(cacheKey)) {
    return state.timelineCache.get(cacheKey);
  }
  const data = await fetchJson(`${endpoint}/${iso2}/timeline?as_of=${encodeURIComponent(asOf)}`);
  state.timelineCache.set(cacheKey, data);
  return data;
};

const isWebGLAvailable = () => {
  try {
    const canvas = document.createElement('canvas');
    return !!(
      window.WebGLRenderingContext &&
      (canvas.getContext('webgl') || canvas.getContext('experimental-webgl'))
    );
  } catch (error) {
    return false;
  }
};

const colorForCount = (count, max) => {
  const safeCount = Number.isFinite(count) ? count : 0;
  const ratio = max > 0 ? Math.min(safeCount / max, 1) : 0;
  const low = [56, 189, 248];
  const high = [37, 99, 235];
  const r = Math.round(low[0] + (high[0] - low[0]) * ratio);
  const g = Math.round(low[1] + (high[1] - low[1]) * ratio);
  const b = Math.round(low[2] + (high[2] - low[2]) * ratio);
  const alpha = 0.2 + ratio * 0.7;
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
};

const getInstrumentCount = (data) => {
  if (!data) return 0;
  return data.instrument_count ?? data.instruments_count ?? 0;
};

const initGlobe = async (container, tooltip) => {
  if (!window.Globe) {
    return null;
  }
  const globe = window.Globe()(container)
    .backgroundColor('rgba(0,0,0,0)')
    .showAtmosphere(true)
    .atmosphereColor('rgba(56, 189, 248, 0.35)')
    .atmosphereAltitude(0.25)
    .polygonStrokeColor(() => 'rgba(255,255,255,0.35)')
    .polygonAltitude((feature) => {
      const data = resolveJurisdiction(feature);
      const count = getInstrumentCount(data);
      if (!count) return 0.01;
      return 0.01 + count / Math.max(state.heatmapMax, 1) * 0.2;
    })
    .polygonCapColor((feature) => {
      const data = resolveJurisdiction(feature);
      return colorForCount(getInstrumentCount(data), state.heatmapMax);
    })
    .onPolygonHover((feature) => {
      state.hover = feature;
      if (!feature) {
        tooltip.hidden = true;
        return;
      }
      const data = resolveJurisdiction(feature);
      if (!data) {
        tooltip.hidden = true;
        return;
      }
      tooltip.innerHTML = `<strong>${escapeHtml(data.name)}</strong><br>${formatNumber(getInstrumentCount(data))} instruments`;
      tooltip.hidden = false;
    })
    .onPolygonClick((feature) => {
      const data = resolveJurisdiction(feature);
      if (data?.iso2) {
        loadJurisdiction(data.iso2, data.name);
      }
    });

  container.addEventListener('mousemove', (event) => {
    if (tooltip.hidden) return;
    const bounds = container.getBoundingClientRect();
    tooltip.style.left = `${event.clientX - bounds.left}px`;
    tooltip.style.top = `${event.clientY - bounds.top}px`;
  });

  return globe;
};

const initFallbackMap = (container, tooltip, geoData) => {
  if (!window.d3) return null;
  const width = 900;
  const height = 500;
  const svg = window.d3
    .select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const projection = window.d3.geoNaturalEarth1().fitSize([width, height], geoData);
  const path = window.d3.geoPath(projection);

  const paths = svg
    .append('g')
    .selectAll('path')
    .data(geoData.features)
    .enter()
    .append('path')
    .attr('d', path)
    .attr('fill', (feature) => {
      const data = resolveJurisdiction(feature);
      return colorForCount(getInstrumentCount(data), state.heatmapMax);
    })
    .attr('stroke', 'rgba(255,255,255,0.5)')
    .attr('stroke-width', 0.5)
    .on('mousemove', (event, feature) => {
      const data = resolveJurisdiction(feature);
      if (!data) {
        tooltip.hidden = true;
        return;
      }
      tooltip.innerHTML = `<strong>${escapeHtml(data.name)}</strong><br>${formatNumber(getInstrumentCount(data))} instruments`;
      tooltip.hidden = false;
      const bounds = container.getBoundingClientRect();
      tooltip.style.left = `${event.clientX - bounds.left}px`;
      tooltip.style.top = `${event.clientY - bounds.top}px`;
    })
    .on('mouseleave', () => {
      tooltip.hidden = true;
    })
    .on('click', (event, feature) => {
      const data = resolveJurisdiction(feature);
      if (data?.iso2) {
        loadJurisdiction(data.iso2, data.name);
      }
    });

  return {
    svg,
    path,
    projection,
    paths
  };
};

const setHeatmapIndex = (jurisdictions) => {
  state.heatmapIndex.clear();
  let max = 0;
  jurisdictions.forEach((item) => {
    const key = normalizeName(item.name);
    state.heatmapIndex.set(key, item);
    max = Math.max(max, getInstrumentCount(item));
  });
  state.heatmapMax = max || 1;
};

const updateMapColors = (jurisdictions) => {
  setHeatmapIndex(jurisdictions);
  if (state.globe) {
    state.globe
      .polygonAltitude((feature) => {
        const data = resolveJurisdiction(feature);
        const count = getInstrumentCount(data);
        if (!count) return 0.01;
        return 0.01 + count / state.heatmapMax * 0.2;
      })
      .polygonCapColor((feature) => {
        const data = resolveJurisdiction(feature);
        return colorForCount(getInstrumentCount(data), state.heatmapMax);
      });
  }
  if (state.flatMap?.paths) {
    state.flatMap.paths.attr('fill', (feature) => {
      const data = resolveJurisdiction(feature);
      return colorForCount(getInstrumentCount(data), state.heatmapMax);
    });
  }
};

const renderSummary = (jurisdictions, summaryJurisdictions, summaryInstruments) => {
  const totalJurisdictions = jurisdictions.length;
  const totalInstruments = jurisdictions.reduce(
    (acc, item) => acc + getInstrumentCount(item),
    0
  );
  summaryJurisdictions.textContent = formatNumber(totalJurisdictions);
  summaryInstruments.textContent = formatNumber(totalInstruments);
};

const renderDrilldown = (panel, data) => {
  const instruments = data.instruments || [];
  const sources = data.sources || [];
  const milestones = data.timeline || [];
  const jurisdiction = data.jurisdiction || {};

  const instrumentsList = instruments
    .map(
      (item) => `
        <li>
          <strong>${escapeHtml(item.title_english || item.title_official || 'Untitled')}</strong>
          <div class="muted">${escapeHtml(item.instrument_type || 'Unknown type')} · ${escapeHtml(item.status || 'Unknown status')}</div>
        </li>
      `
    )
    .join('');

  const milestonesList = milestones
    .map(
      (item) => `
        <li>
          <strong>${escapeHtml(item.milestone_type || 'Milestone')}</strong>
          <div class="muted">${escapeHtml(item.milestone_date || '--')}</div>
        </li>
      `
    )
    .join('');

  const sourcesList = sources
    .map(
      (item) => `
        <li>
          <a href="${escapeHtml(item.url || '#')}" target="_blank" rel="noreferrer">${escapeHtml(item.title || 'Source')}</a>
        </li>
      `
    )
    .join('');

  const dataQualityFlag = jurisdiction.data_quality?.flag
    ? '<span class="ai-reg__pill">Data quality flag</span>'
    : '';

  panel.innerHTML = `
    <div>
      <h4 class="h5">${escapeHtml(jurisdiction.name || 'Jurisdiction')}</h4>
      <div class="muted">ISO: ${escapeHtml(jurisdiction.iso2 || '--')}</div>
      ${dataQualityFlag}
    </div>
    <div class="ai-reg__stat-grid">
      <div class="ai-reg__stat">
        <span>Obligations</span>
        <strong>${formatNumber(jurisdiction.obligations_count)}</strong>
      </div>
      <div class="ai-reg__stat">
        <span>Instruments</span>
        <strong>${formatNumber(instruments.length)}</strong>
      </div>
    </div>
    <div class="ai-reg__panel-section">
      <h5 class="h6">Instruments</h5>
      ${instrumentsList ? `<ul class="ai-reg__list">${instrumentsList}</ul>` : '<div class="muted">No instruments listed.</div>'}
    </div>
    <div class="ai-reg__panel-section">
      <h5 class="h6">Upcoming milestones</h5>
      ${milestonesList ? `<ul class="ai-reg__list">${milestonesList}</ul>` : '<div class="muted">No milestones available.</div>'}
    </div>
    <div class="ai-reg__panel-section">
      <h5 class="h6">Sources</h5>
      ${sourcesList ? `<ul class="ai-reg__list">${sourcesList}</ul>` : '<div class="muted">No sources listed.</div>'}
    </div>
  `;
};

const loadJurisdiction = async (iso2, nameOverride) => {
  const root = document.querySelector('[data-ai-reg-root]');
  if (!root) return;
  const panelBody = root.querySelector('[data-drilldown-body]');
  if (!panelBody) return;
  panelBody.innerHTML = `<div class="muted">Loading ${escapeHtml(nameOverride || iso2)}…</div>`;
  try {
    const endpoint = root.dataset.jurisdictionEndpoint;
    const [bundle, instruments, timeline] = await Promise.all([
      getJurisdictionBundle(endpoint, iso2, state.asOf),
      getJurisdictionInstruments(endpoint, iso2, state.asOf),
      getJurisdictionTimeline(endpoint, iso2, state.asOf)
    ]);
    renderDrilldown(panelBody, {
      jurisdiction: bundle.jurisdiction,
      sources: bundle.sources,
      instruments: instruments.instruments || instruments,
      timeline: timeline.milestones || timeline
    });
  } catch (error) {
    panelBody.innerHTML = '<div class="muted">Unable to load jurisdiction details. Please try again.</div>';
  }
};

const loadGeoData = async () => {
  if (state.geoData) return state.geoData;
  const data = await fetchJson(GEOJSON_URL);
  state.geoData = data;
  return data;
};

const refreshHeatmap = async (root, asOf) => {
  const loading = root.querySelector('[data-map-loading]');
  const summaryJurisdictions = root.querySelector('[data-ai-reg-summary-jurisdictions]');
  const summaryInstruments = root.querySelector('[data-ai-reg-summary-instruments]');
  const heatmapEndpoint = root.dataset.heatmapEndpoint;
  loading.hidden = false;
  try {
    if (!asOf) {
      summaryJurisdictions.textContent = '—';
      summaryInstruments.textContent = '—';
      return;
    }
    const data = await getHeatmap(heatmapEndpoint, asOf);
    const jurisdictions = data.jurisdictions || [];
    renderSummary(jurisdictions, summaryJurisdictions, summaryInstruments);
    updateMapColors(jurisdictions);
  } catch (error) {
    summaryJurisdictions.textContent = '—';
    summaryInstruments.textContent = '—';
  } finally {
    loading.hidden = true;
  }
};

const setup = async () => {
  const root = document.querySelector('[data-ai-reg-root]');
  if (!root) return;

  const asOfSelect = root.querySelector('#ai-reg-as-of');
  const mapContainer = root.querySelector('[data-map-container]');
  const globeContainer = root.querySelector('[data-globe]');
  const fallbackContainer = root.querySelector('[data-fallback]');
  const tooltip = root.querySelector('[data-tooltip]');

  state.asOf = asOfSelect?.value || null;

  const geoData = await loadGeoData();

  const canUseWebGL = isWebGLAvailable();
  if (canUseWebGL) {
    state.globe = await initGlobe(globeContainer, tooltip);
    state.mapMode = '3d';
  }

  if (!state.globe) {
    state.mapMode = '2d';
    fallbackContainer.hidden = false;
    state.flatMap = initFallbackMap(fallbackContainer, tooltip, geoData);
  }

  if (state.globe) {
    state.globe
      .polygonsData(geoData.features)
      .polygonSideColor(() => 'rgba(15, 23, 42, 0.2)')
      .polygonsTransitionDuration(400);
    state.globe.controls().autoRotate = true;
    state.globe.controls().autoRotateSpeed = 0.6;
  }

  await refreshHeatmap(root, state.asOf);

  asOfSelect?.addEventListener('change', async (event) => {
    state.asOf = event.target.value;
    await refreshHeatmap(root, state.asOf);
  });

  mapContainer.addEventListener('mouseleave', () => {
    tooltip.hidden = true;
  });
};

document.addEventListener('DOMContentLoaded', () => {
  setup().catch(() => {
    // no-op
  });
});

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

const GEOJSON_REMOTE_FALLBACK =
  'https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json';
const ISO2_PROPERTY_CANDIDATES = ['ISO_A2', 'iso_a2', 'ISO2', 'iso2', 'ISO_2'];
const DEFAULT_POV = { lat: 20, lng: 0, altitude: 2.1 };
const FOCUS_POV = { altitude: 1.65 };

const state = {
  asOf: null,
  heatmapCache: new Map(),
  jurisdictionCache: new Map(),
  instrumentsCache: new Map(),
  timelineCache: new Map(),
  geoData: null,
  geojsonUrl: null,
  globeTextureUrl: null,
  featureByIso: new Map(),
  heatmapIndex: new Map(),
  heatmapByIso: new Map(),
  heatmapMax: 1,
  globe: null,
  flatMap: null,
  hover: null,
  mapMode: '3d',
  mapErrorEl: null,
  defaultPov: DEFAULT_POV,
  globeSize: { width: 0, height: 0 }
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

const logIssue = (message, error) => {
  if (error) {
    console.error(`AI regulation map: ${message}`, error);
  } else {
    console.warn(`AI regulation map: ${message}`);
  }
};

const getIso2FromFeature = (feature) => {
  if (!feature?.properties) return null;
  for (const key of ISO2_PROPERTY_CANDIDATES) {
    const value = feature.properties[key];
    if (!value) continue;
    const normalized = String(value).trim().toUpperCase();
    if (!normalized || normalized === '-99') continue;
    return normalized;
  }
  return null;
};

const getFeatureName = (feature) =>
  feature?.properties?.name ||
  feature?.properties?.NAME ||
  feature?.properties?.ADMIN ||
  '';

const collectCoordinates = (geometry) => {
  if (!geometry) return [];
  if (geometry.type === 'Polygon') {
    return geometry.coordinates.flat();
  }
  if (geometry.type === 'MultiPolygon') {
    return geometry.coordinates.flat(2);
  }
  return [];
};

const getFeatureCentroid = (feature) => {
  const coords = collectCoordinates(feature?.geometry);
  if (!coords.length) return null;
  let sumLon = 0;
  let sumLat = 0;
  let count = 0;
  coords.forEach((point) => {
    const [lon, lat] = point;
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) return;
    sumLon += lon;
    sumLat += lat;
    count += 1;
  });
  if (!count) return null;
  return { lng: sumLon / count, lat: sumLat / count };
};

const resolveJurisdiction = (feature) => {
  const iso2 = getIso2FromFeature(feature);
  if (iso2 && state.heatmapByIso.has(iso2)) {
    return state.heatmapByIso.get(iso2) || null;
  }
  const featureName = getFeatureName(feature);
  if (!featureName) return null;
  const key = normalizeName(featureName);
  return state.heatmapIndex.get(key) || null;
};

const buildFeatureIndex = (geoData) => {
  state.featureByIso.clear();
  if (!geoData?.features) return;
  geoData.features.forEach((feature) => {
    const iso2 = getIso2FromFeature(feature);
    if (!iso2) return;
    state.featureByIso.set(iso2, feature);
  });
};

const setMapError = (message) => {
  if (!state.mapErrorEl) return;
  state.mapErrorEl.textContent = message;
  state.mapErrorEl.hidden = false;
};

const clearMapError = () => {
  if (!state.mapErrorEl) return;
  state.mapErrorEl.hidden = true;
  state.mapErrorEl.textContent = '';
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

const hydrateAsOfSelect = async (selectEl) => {
  if (!selectEl) return null;
  if (selectEl.options.length > 1) {
    return selectEl.value || null;
  }
  const endpoint = selectEl.dataset.endpoint;
  if (!endpoint) return selectEl.value || null;
  try {
    const payload = await fetchJson(endpoint);
    const dates = payload.as_of_dates || [];
    if (!dates.length) return null;
    selectEl.innerHTML = "";
    dates.forEach((date) => {
      const option = document.createElement("option");
      option.value = date;
      option.textContent = date;
      selectEl.appendChild(option);
    });
    selectEl.value = dates[0];
    return dates[0];
  } catch (error) {
    logIssue("Unable to fetch as-of dates.", error);
    return selectEl.value || null;
  }
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

const initGlobe = async (container, tooltip, textureUrl) => {
  if (!window.Globe || !window.THREE) {
    logIssue('Globe library unavailable; switching to 2D fallback.');
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
      const iso2 = data?.iso2 || getIso2FromFeature(feature);
      const name = data?.name || getFeatureName(feature);
      if (iso2) {
        loadJurisdiction(iso2, name);
      }
    });

  if (textureUrl) {
    globe.globeImageUrl(textureUrl);
  } else {
    globe.globeMaterial().color = new window.THREE.Color('#3b82f6');
  }

  container.addEventListener('mousemove', (event) => {
    if (tooltip.hidden) return;
    const bounds = container.getBoundingClientRect();
    tooltip.style.left = `${event.clientX - bounds.left}px`;
    tooltip.style.top = `${event.clientY - bounds.top}px`;
  });

  const controls = globe.controls();
  if (controls) {
    controls.enableZoom = true;
    controls.zoomSpeed = 0.7;
    controls.enablePan = false;
    controls.minDistance = 140;
    controls.maxDistance = 520;
  }

  return globe;
};

const buildSvgPath = (geometry, project) => {
  if (!geometry) return '';
  const drawRing = (ring) =>
    ring
      .map((point, idx) => {
        const [x, y] = project(point);
        return `${idx === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
      })
      .join(' ') + ' Z';

  if (geometry.type === 'Polygon') {
    return geometry.coordinates.map(drawRing).join(' ');
  }
  if (geometry.type === 'MultiPolygon') {
    return geometry.coordinates.map((poly) => poly.map(drawRing).join(' ')).join(' ');
  }
  return '';
};

const initFallbackMap = (container, tooltip, geoData) => {
  if (!container || !geoData) return null;
  const width = 1000;
  const height = 520;
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');
  svg.setAttribute('width', '100%');
  svg.setAttribute('height', '100%');
  container.appendChild(svg);

  const project = ([lon, lat]) => [
    ((Number(lon) + 180) / 360) * width,
    ((90 - Number(lat)) / 180) * height
  ];

  const paths = [];
  geoData.features.forEach((feature) => {
    const pathDef = buildSvgPath(feature.geometry, project);
    if (!pathDef) return;
    const data = resolveJurisdiction(feature);
    const fallbackIso2 = data?.iso2 || getIso2FromFeature(feature);
    const fallbackName = data?.name || getFeatureName(feature);
    const pathEl = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    pathEl.setAttribute('d', pathDef);
    pathEl.setAttribute('fill', colorForCount(getInstrumentCount(data), state.heatmapMax));
    pathEl.setAttribute('stroke', 'rgba(255,255,255,0.5)');
    pathEl.setAttribute('stroke-width', '0.5');
    if (fallbackIso2) {
      pathEl.setAttribute('data-iso2', fallbackIso2);
      pathEl.setAttribute('data-name', fallbackName || '');
    }
    pathEl.addEventListener('mousemove', (event) => {
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
    });
    pathEl.addEventListener('mouseleave', () => {
      tooltip.hidden = true;
    });
    pathEl.addEventListener('click', () => {
      const data = resolveJurisdiction(feature);
      const iso2 = data?.iso2 || getIso2FromFeature(feature);
      const name = data?.name || getFeatureName(feature);
      if (iso2) {
        loadJurisdiction(iso2, name);
      }
    });
    svg.appendChild(pathEl);
    paths.push({ element: pathEl, feature });
  });

  const viewBoxState = { x: 0, y: 0, width, height };
  const minScale = 1;
  const maxScale = 4.5;

  const applyViewBox = () => {
    svg.setAttribute(
      'viewBox',
      `${viewBoxState.x.toFixed(2)} ${viewBoxState.y.toFixed(2)} ${viewBoxState.width.toFixed(2)} ${viewBoxState.height.toFixed(2)}`
    );
  };

  const clampViewBox = () => {
    const maxX = width - viewBoxState.width;
    const maxY = height - viewBoxState.height;
    viewBoxState.x = Math.max(0, Math.min(viewBoxState.x, maxX));
    viewBoxState.y = Math.max(0, Math.min(viewBoxState.y, maxY));
  };

  const resetView = () => {
    viewBoxState.x = 0;
    viewBoxState.y = 0;
    viewBoxState.width = width;
    viewBoxState.height = height;
    applyViewBox();
  };

  const focusOnFeature = (feature) => {
    const coords = collectCoordinates(feature?.geometry);
    if (!coords.length) return;
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    coords.forEach(([lon, lat]) => {
      const [x, y] = project([lon, lat]);
      if (!Number.isFinite(x) || !Number.isFinite(y)) return;
      minX = Math.min(minX, x);
      maxX = Math.max(maxX, x);
      minY = Math.min(minY, y);
      maxY = Math.max(maxY, y);
    });
    if (!Number.isFinite(minX) || !Number.isFinite(maxX)) return;
    const padding = 24;
    const targetWidth = Math.max((maxX - minX) + padding * 2, width / maxScale);
    const targetHeight = Math.max((maxY - minY) + padding * 2, height / maxScale);
    viewBoxState.width = Math.min(width, targetWidth);
    viewBoxState.height = Math.min(height, targetHeight);
    viewBoxState.x = minX - (viewBoxState.width - (maxX - minX)) / 2;
    viewBoxState.y = minY - (viewBoxState.height - (maxY - minY)) / 2;
    clampViewBox();
    applyViewBox();
  };

  const zoomBy = (factor) => {
    const scale = width / viewBoxState.width;
    const nextScale = Math.min(maxScale, Math.max(minScale, scale * factor));
    const nextWidth = width / nextScale;
    const nextHeight = height / nextScale;
    const centerX = viewBoxState.x + viewBoxState.width / 2;
    const centerY = viewBoxState.y + viewBoxState.height / 2;
    viewBoxState.x = centerX - nextWidth / 2;
    viewBoxState.y = centerY - nextHeight / 2;
    viewBoxState.width = nextWidth;
    viewBoxState.height = nextHeight;
    clampViewBox();
    applyViewBox();
  };

  svg.addEventListener(
    'wheel',
    (event) => {
      event.preventDefault();
      const rect = svg.getBoundingClientRect();
      const pointerX = (event.clientX - rect.left) / rect.width;
      const pointerY = (event.clientY - rect.top) / rect.height;
      const scale = width / viewBoxState.width;
      const zoomFactor = Math.exp(-event.deltaY * 0.001);
      const nextScale = Math.min(maxScale, Math.max(minScale, scale * zoomFactor));
      const nextWidth = width / nextScale;
      const nextHeight = height / nextScale;
      const anchorX = viewBoxState.x + pointerX * viewBoxState.width;
      const anchorY = viewBoxState.y + pointerY * viewBoxState.height;
      viewBoxState.x = anchorX - pointerX * nextWidth;
      viewBoxState.y = anchorY - pointerY * nextHeight;
      viewBoxState.width = nextWidth;
      viewBoxState.height = nextHeight;
      clampViewBox();
      applyViewBox();
    },
    { passive: false }
  );

  let dragStart = null;

  svg.addEventListener('pointerdown', (event) => {
    if (event.button !== 0) return;
    const rect = svg.getBoundingClientRect();
    dragStart = {
      x: event.clientX,
      y: event.clientY,
      viewBoxX: viewBoxState.x,
      viewBoxY: viewBoxState.y,
      width: viewBoxState.width,
      height: viewBoxState.height,
      rectWidth: rect.width,
      rectHeight: rect.height
    };
    svg.setPointerCapture(event.pointerId);
    svg.style.cursor = 'grabbing';
  });

  svg.addEventListener('pointermove', (event) => {
    if (!dragStart) return;
    const dx = event.clientX - dragStart.x;
    const dy = event.clientY - dragStart.y;
    const offsetX = (dx / dragStart.rectWidth) * dragStart.width;
    const offsetY = (dy / dragStart.rectHeight) * dragStart.height;
    viewBoxState.x = dragStart.viewBoxX - offsetX;
    viewBoxState.y = dragStart.viewBoxY - offsetY;
    clampViewBox();
    applyViewBox();
    event.preventDefault();
  });

  const endDrag = (event) => {
    if (!dragStart) return;
    dragStart = null;
    svg.releasePointerCapture(event.pointerId);
    svg.style.cursor = 'grab';
  };

  svg.addEventListener('pointerup', endDrag);
  svg.addEventListener('pointerleave', endDrag);
  svg.style.cursor = 'grab';

  return { svg, paths, resetView, focusOnFeature, zoomBy };
};

const setHeatmapIndex = (jurisdictions) => {
  state.heatmapIndex.clear();
  state.heatmapByIso.clear();
  let max = 0;
  jurisdictions.forEach((item) => {
    if (item.iso2) {
      state.heatmapByIso.set(String(item.iso2).toUpperCase(), item);
    }
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
    state.flatMap.paths.forEach(({ element, feature }) => {
      const data = resolveJurisdiction(feature);
      element.setAttribute('fill', colorForCount(getInstrumentCount(data), state.heatmapMax));
    });
  }
};

const renderSummary = (jurisdictions, summaryJurisdictions, summaryInstruments) => {
  if (!summaryJurisdictions || !summaryInstruments) return;
  const totalJurisdictions = jurisdictions.length;
  const totalInstruments = jurisdictions.reduce(
    (acc, item) => acc + getInstrumentCount(item),
    0
  );
  summaryJurisdictions.textContent = formatNumber(totalJurisdictions);
  summaryInstruments.textContent = formatNumber(totalInstruments);
};

const renderCountryOptions = (select, jurisdictions) => {
  if (!select) return;
  if (!Array.isArray(jurisdictions) || jurisdictions.length === 0) {
    select.innerHTML = '<option value="">No countries available</option>';
    select.disabled = true;
    return;
  }
  const sorted = [...jurisdictions].sort((a, b) => {
    const aCount = getInstrumentCount(a);
    const bCount = getInstrumentCount(b);
    if (bCount !== aCount) return bCount - aCount;
    return String(a.name || '').localeCompare(String(b.name || ''));
  });
  const options = sorted
    .filter((item) => item.iso2)
    .map((item) => {
      const count = getInstrumentCount(item);
      return `<option value="${escapeHtml(item.iso2)}" data-count="${count}">${escapeHtml(item.name)}</option>`;
    });
  select.innerHTML = `<option value="">Select a country</option>${options.join('')}`;
  select.disabled = false;
};

const updateCountrySelect = (select, iso2) => {
  if (!select || !iso2) return;
  select.value = iso2;
};

const focusOnIso = (iso2) => {
  if (!iso2) return;
  const feature = state.featureByIso.get(String(iso2).toUpperCase());
  if (!feature) return;
  const centroid = getFeatureCentroid(feature);
  if (state.globe && centroid) {
    state.globe.pointOfView({ ...centroid, altitude: FOCUS_POV.altitude }, 700);
  }
  if (state.flatMap?.focusOnFeature) {
    state.flatMap.focusOnFeature(feature);
  }
};

const renderDrilldown = (panel, data) => {
  const instruments = data.instruments || [];
  const sources = data.sources || [];
  const milestones = data.timeline || [];
  const jurisdiction = data.jurisdiction || {};
  const root = document.querySelector('[data-ai-reg-root]');
  const externalLists = root?.querySelector('[data-ai-reg-lists]');
  const listLimit = externalLists ? 5 : null;

  const instrumentsList = instruments
    .slice(0, listLimit || instruments.length)
    .map(
      (item) => `
        <li>
          <strong>${escapeHtml(item.title_english || item.title_official || 'Untitled')}</strong>
          <div class="muted">${escapeHtml(item.instrument_type || 'Unknown type')} · ${escapeHtml(item.status || 'Unknown status')}</div>
        </li>
      `
    )
    .join('');

  const instrumentsFullList = instruments
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
    .slice(0, listLimit || sources.length)
    .map(
      (item) => `
        <li>
          <a href="${escapeHtml(item.url || '#')}" target="_blank" rel="noreferrer">${escapeHtml(item.title || 'Source')}</a>
        </li>
      `
    )
    .join('');

  const sourcesFullList = sources
    .map(
      (item) => `
        <li>
          <a href="${escapeHtml(item.url || '#')}" target="_blank" rel="noreferrer">${escapeHtml(item.title || 'Source')}</a>
        </li>
      `
    )
    .join('');

  if (externalLists) {
    const hasMore =
      instruments.length > (listLimit || 0) || sources.length > (listLimit || 0);
    externalLists.innerHTML = `
      <div class="card card--lined ai-reg__list-card" data-list-block ${hasMore ? '' : 'data-expanded="true"'}>
        <div class="card__header">
          <div>
            <h4 class="h6">Instruments &amp; sources</h4>
            <p class="muted">Key instruments paired with reference sources for the selected jurisdiction.</p>
          </div>
        </div>
        <div class="ai-reg__combined-section">
          <h5 class="h6">Instruments</h5>
          ${instrumentsList ? `<ul class="ai-reg__list">${instrumentsList}</ul>` : '<div class="muted">No instruments listed.</div>'}
        </div>
        <div class="ai-reg__combined-section">
          <h5 class="h6">Sources</h5>
          ${sourcesList ? `<ul class="ai-reg__list">${sourcesList}</ul>` : '<div class="muted">No sources listed.</div>'}
        </div>
        ${hasMore ? `<div class="ai-reg__list-full" data-list-full hidden>
          <div class="ai-reg__combined-section">
            <h5 class="h6">Instruments (all)</h5>
            ${instrumentsFullList ? `<ul class="ai-reg__list">${instrumentsFullList}</ul>` : '<div class="muted">No instruments listed.</div>'}
          </div>
          <div class="ai-reg__combined-section">
            <h5 class="h6">Sources (all)</h5>
            ${sourcesFullList ? `<ul class="ai-reg__list">${sourcesFullList}</ul>` : '<div class="muted">No sources listed.</div>'}
          </div>
        </div>
        <button class="btn btn--ghost btn--sm ai-reg__list-toggle" type="button" data-list-toggle>Show more</button>` : ''}
      </div>
    `;
  }

  const dataQualityFlag = jurisdiction.data_quality?.flag
    ? '<span class="ai-reg__pill">Data quality flag</span>'
    : '';

  panel.innerHTML = `
    <div class="ai-reg__panel-header">
      <h4 class="h5">${escapeHtml(jurisdiction.name || 'Jurisdiction')}</h4>
      <div class="muted">ISO: ${escapeHtml(jurisdiction.iso2 || '--')}</div>
      ${dataQualityFlag}
    </div>
    <div class="ai-reg__panel-metrics">
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
      <h5 class="h6">Upcoming milestones</h5>
      ${milestonesList ? `<ul class="ai-reg__list">${milestonesList}</ul>` : '<div class="muted">No milestones available.</div>'}
    </div>
    ${externalLists ? '' : `
    <div class="ai-reg__panel-columns">
      <div class="ai-reg__panel-section">
        <h5 class="h6">Instruments</h5>
        ${instrumentsList ? `<ul class="ai-reg__list">${instrumentsList}</ul>` : '<div class="muted">No instruments listed.</div>'}
      </div>
      ${externalLists ? '' : `
      <div class="ai-reg__panel-section">
        <h5 class="h6">Sources</h5>
        ${sourcesList ? `<ul class="ai-reg__list">${sourcesList}</ul>` : '<div class="muted">No sources listed.</div>'}
      </div>`}
    </div>`}
  `;
};

const loadJurisdiction = async (iso2, nameOverride) => {
  const root = document.querySelector('[data-ai-reg-root]');
  if (!root) return;
  const panelBody = root.querySelector('[data-drilldown-body]');
  const countrySelect = root.querySelector('[data-country-select]');
  if (!panelBody) return;
  updateCountrySelect(countrySelect, iso2);
  focusOnIso(iso2);
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

const loadGeoData = async (localUrl) => {
  if (state.geoData) return state.geoData;
  const candidates = [localUrl, GEOJSON_REMOTE_FALLBACK].filter(Boolean);
  let lastError = null;
  for (const url of candidates) {
    try {
      const data = await fetchJson(url);
      if (!data || !Array.isArray(data.features)) {
        throw new Error('GeoJSON missing features array');
      }
      state.geoData = data;
      return data;
    } catch (error) {
      lastError = error;
      logIssue(`GeoJSON load failed for ${url}.`, error);
    }
  }
  throw lastError || new Error('GeoJSON load failed');
};

const refreshHeatmap = async (root, asOf) => {
  const loading = root.querySelector('[data-map-loading]');
  const summaryJurisdictions = root.querySelector('[data-ai-reg-summary-jurisdictions]');
  const summaryInstruments = root.querySelector('[data-ai-reg-summary-instruments]');
  const countrySelect = root.querySelector('[data-country-select]');
  const heatmapEndpoint = root.dataset.heatmapEndpoint;
  if (loading) loading.hidden = false;
  try {
    if (!asOf) {
      if (summaryJurisdictions) summaryJurisdictions.textContent = '—';
      if (summaryInstruments) summaryInstruments.textContent = '—';
      if (countrySelect) {
        countrySelect.innerHTML = '<option value="">No snapshots available</option>';
        countrySelect.disabled = true;
      }
      return;
    }
    const data = await getHeatmap(heatmapEndpoint, asOf);
    const jurisdictions = data.jurisdictions || [];
    if (state.geoData) {
      clearMapError();
    }
    renderSummary(jurisdictions, summaryJurisdictions, summaryInstruments);
    renderCountryOptions(countrySelect, jurisdictions);
    updateMapColors(jurisdictions);
  } catch (error) {
    logIssue('Heatmap fetch failed.', error);
    setMapError('Heatmap failed to load. Please refresh and try again.');
    if (summaryJurisdictions) summaryJurisdictions.textContent = '—';
    if (summaryInstruments) summaryInstruments.textContent = '—';
    if (countrySelect) {
      countrySelect.innerHTML = '<option value="">Unable to load countries</option>';
      countrySelect.disabled = true;
    }
  } finally {
    if (loading) loading.hidden = true;
  }
};

const setup = async () => {
  const root = document.querySelector('[data-ai-reg-root]');
  if (!root) return;

  const asOfSelect = root.querySelector('#ai-reg-as-of');
  const countrySelect = root.querySelector('[data-country-select]');
  const mapContainer = root.querySelector('[data-map-container]');
  const globeContainer = root.querySelector('[data-globe]');
  const fallbackContainer = root.querySelector('[data-fallback]');
  const tooltip = root.querySelector('[data-tooltip]');
  const resetButton = root.querySelector('[data-reset-view]');
  const zoomInButton = root.querySelector('[data-zoom-in]');
  const zoomOutButton = root.querySelector('[data-zoom-out]');
  state.mapErrorEl = root.querySelector('[data-map-error]');
  clearMapError();

  state.asOf = await hydrateAsOfSelect(asOfSelect);
  state.globeTextureUrl = root.dataset.globeTextureUrl || null;

  let geoData = null;
  try {
    geoData = await loadGeoData(root.dataset.geojsonUrl);
    buildFeatureIndex(geoData);
  } catch (error) {
    setMapError('Map data failed to load. Please refresh and try again.');
  }

  const force2d = window.__AI_REG_FORCE_2D === true;
  const canUseWebGL = isWebGLAvailable();
  if (!force2d && canUseWebGL && geoData) {
    try {
      state.globe = await initGlobe(globeContainer, tooltip, state.globeTextureUrl);
      state.mapMode = '3d';
    } catch (error) {
      logIssue('Globe initialization failed; falling back to 2D.', error);
      state.globe = null;
    }
  }

  if (!state.globe) {
    state.mapMode = '2d';
    if (fallbackContainer) {
      fallbackContainer.hidden = false;
    }
    if (geoData) {
      state.flatMap = initFallbackMap(fallbackContainer, tooltip, geoData);
    }
  }

  let resizeRaf = null;
  const resizeGlobe = () => {
    if (!state.globe || !mapContainer) return;
    const bounds = mapContainer.getBoundingClientRect();
    const width = Math.round(bounds.width);
    const height = Math.round(bounds.height);
    if (!width || !height) return;
    const last = state.globeSize || { width: 0, height: 0 };
    if (Math.abs(width - last.width) < 2 && Math.abs(height - last.height) < 2) {
      return;
    }
    state.globeSize = { width, height };
    state.globe.width(width).height(height);
  };

  const scheduleResize = () => {
    if (resizeRaf) return;
    resizeRaf = window.requestAnimationFrame(() => {
      resizeRaf = null;
      resizeGlobe();
    });
  };

  if (state.globe && geoData) {
    scheduleResize();
    state.globe
      .polygonsData(geoData.features)
      .polygonSideColor(() => 'rgba(15, 23, 42, 0.2)')
      .polygonsTransitionDuration(400);
    state.globe.pointOfView(state.defaultPov, 0);
    const controls = state.globe.controls();
    if (controls) {
      controls.autoRotate = false;
    }
  }

  await refreshHeatmap(root, state.asOf);

  const defaultIso = root.dataset.defaultIso;
  if (defaultIso) {
    const iso2 = defaultIso.trim().toUpperCase();
    const jurisdiction = state.heatmapByIso.get(iso2);
    loadJurisdiction(iso2, jurisdiction?.name || iso2);
  }

  asOfSelect?.addEventListener('change', async (event) => {
    state.asOf = event.target.value;
    await refreshHeatmap(root, state.asOf);
  });

  countrySelect?.addEventListener('change', (event) => {
    const iso2 = event.target.value;
    if (!iso2) return;
    const jurisdiction = state.heatmapByIso.get(String(iso2).toUpperCase());
    loadJurisdiction(iso2, jurisdiction?.name || iso2);
  });

  if (mapContainer) {
    mapContainer.addEventListener('mouseleave', () => {
      tooltip.hidden = true;
    });
  }

  if (mapContainer && state.globe) {
    const observer = new ResizeObserver(() => {
      scheduleResize();
    });
    observer.observe(mapContainer);
  }

  resetButton?.addEventListener('click', () => {
    if (state.globe) {
      state.globe.pointOfView(state.defaultPov, 600);
    }
    if (state.flatMap?.resetView) {
      state.flatMap.resetView();
    }
  });

  const adjustZoom = (direction) => {
    if (state.globe) {
      const controls = state.globe.controls();
      if (controls?.dollyIn && controls?.dollyOut) {
        const factor = direction === 'in' ? 1.12 : 0.9;
        if (direction === 'in') {
          controls.dollyIn(factor);
        } else {
          controls.dollyOut(factor);
        }
        controls.update();
      }
    }
    if (state.flatMap?.zoomBy) {
      state.flatMap.zoomBy(direction === 'in' ? 1.15 : 0.9);
    }
  };

  zoomInButton?.addEventListener('click', () => adjustZoom('in'));
  zoomOutButton?.addEventListener('click', () => adjustZoom('out'));

  root.addEventListener('click', (event) => {
    const toggle = event.target.closest('[data-list-toggle]');
    if (!toggle) return;
    const block = toggle.closest('[data-list-block]');
    if (!block) return;
    const full = block.querySelector('[data-list-full]');
    if (!full) return;
    const expanded = block.getAttribute('data-expanded') === 'true';
    block.setAttribute('data-expanded', expanded ? 'false' : 'true');
    full.hidden = expanded;
    toggle.textContent = expanded ? 'Show more' : 'Show less';
  });
};

document.addEventListener('DOMContentLoaded', () => {
  setup().catch((error) => {
    logIssue('Map initialization failed.', error);
    setMapError('Map failed to initialize. Please refresh and try again.');
  });
});

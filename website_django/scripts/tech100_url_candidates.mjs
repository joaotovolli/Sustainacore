const normalizePath = (value) => {
  if (!value) return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
};

const dedupe = (items) => {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    if (!item || seen.has(item)) continue;
    seen.add(item);
    out.push(item);
  }
  return out;
};

const buildTech100Candidates = ({ override, discovered = [] } = {}) => {
  const normalizedOverride = normalizePath(override);
  const normalizedDiscovered = discovered.map(normalizePath).filter(Boolean);
  return dedupe([
    normalizedOverride,
    "/tech100/index/",
    "/tech100/",
    "/tech100",
    ...normalizedDiscovered,
  ]);
};

export { buildTech100Candidates };

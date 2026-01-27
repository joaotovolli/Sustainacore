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

const buildAiRegCandidates = ({ override, discovered = [] } = {}) => {
  const normalizedOverride = normalizePath(override);
  const normalizedDiscovered = discovered.map(normalizePath).filter(Boolean);
  return dedupe([
    normalizedOverride,
    "/ai-regulation/",
    "/ai-regulation",
    ...normalizedDiscovered,
  ]);
};

export { buildAiRegCandidates };

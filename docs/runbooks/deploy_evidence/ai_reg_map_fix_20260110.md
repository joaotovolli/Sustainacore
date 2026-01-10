# AI regulation map stability evidence

## Root cause
- The 2D fallback depended on `window.d3` and a remote GeoJSON fetch; when WebGL was unavailable and the external script chain failed, the fallback never rendered (only the placeholder label showed).
- The loading overlay could remain visible because `.ai-reg__loading` set `display: flex` without a `[hidden]` override, so toggling `hidden` did not always hide it.

## Production endpoint checks (curl)

### /ai-regulation/data/as-of-dates
```
$(cat /tmp/ai_reg_as_of_prod.json)
```

### /ai-regulation/data/heatmap (snippet)
```
$(head -c 300 /tmp/ai_reg_heatmap_prod.json)
```

## Production headers

### /ai-regulation/ (CSP check)
```
$(cat /tmp/ai_reg_headers_prod.txt)
```

### /static/js/ai_regulation.js
```
$(cat /tmp/ai_reg_js_headers_prod.txt)
```

### /static/css/ai_reg.css
```
$(cat /tmp/ai_reg_css_headers_prod.txt)
```

### Remote GeoJSON (old dependency)
```
$(cat /tmp/ai_reg_geojson_remote_headers.txt)
```

## Local checks

### data-geojson-url in template
```
$(cat /tmp/ai_reg_geo_attr_grep.txt)
```

### Local runserver note
Local runserver returned 500s because the sqlite telemetry tables are not present in this environment:
```
$(head -n 6 /tmp/ai_reg_runserver.log)
```

## Expected behavior after deploy
- The loading overlay clears on success or failure.
- Summary numbers populate from the heatmap response.
- 3D globe renders when WebGL + Globe/Three are available; otherwise the 2D SVG map renders from the local GeoJSON.

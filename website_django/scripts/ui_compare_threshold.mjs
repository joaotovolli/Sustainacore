import fs from "node:fs";
import path from "node:path";

const reportPath = process.env.UI_COMPARE_REPORT || path.resolve("artifacts", "ui", "report", "ui_compare_report.json");
const maxPixels = Number(process.env.DIFF_MAX_PIXELS || "100");

if (!fs.existsSync(reportPath)) {
  console.error(`[ui-compare-threshold] report not found: ${reportPath}`);
  process.exit(1);
}

const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
const mismatchPixels = report?.diff?.mismatchPixels ?? null;

if (mismatchPixels === null) {
  console.error("[ui-compare-threshold] report missing diff.mismatchPixels");
  process.exit(1);
}

if (mismatchPixels > maxPixels) {
  console.error(`[ui-compare-threshold] mismatch ${mismatchPixels} exceeds ${maxPixels}`);
  process.exit(1);
}

console.log(`[ui-compare-threshold] mismatch ${mismatchPixels} within ${maxPixels}`);

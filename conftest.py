import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
DJANGO_ROOT = REPO_ROOT / "website_django"
if str(DJANGO_ROOT) not in sys.path:
    sys.path.insert(0, str(DJANGO_ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
os.environ.setdefault("DJANGO_SECRET_KEY", "test-secret")
os.environ.setdefault(
    "DJANGO_ALLOWED_HOSTS",
    "testserver,localhost,sustainacore.org,www.sustainacore.org",
)
os.environ.setdefault("GEMINI_FIRST_ENABLED", "0")
os.environ.setdefault("ASK2_SKIP_CAPABILITY_SNAPSHOT", "1")

import django  # noqa: E402
from django.test.utils import setup_test_environment  # noqa: E402

django.setup()
setup_test_environment()

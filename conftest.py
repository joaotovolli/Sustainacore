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
    "testserver,localhost,sustainacore.org,www.sustainacore.org,preview.sustainacore.org",
)
os.environ.setdefault("GEMINI_FIRST_ENABLED", "0")
os.environ.setdefault("ASK2_SKIP_CAPABILITY_SNAPSHOT", "1")
os.environ.setdefault("AUTH_TOKEN_SIGNING_KEY", "test-signing-key")

import atexit  # noqa: E402

import django  # noqa: E402
from django.test.utils import setup_databases, setup_test_environment, teardown_databases  # noqa: E402

django.setup()
setup_test_environment()
_db_config = setup_databases(verbosity=0, interactive=False)
atexit.register(lambda: teardown_databases(_db_config, verbosity=0))

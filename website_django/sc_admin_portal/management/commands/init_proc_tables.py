from django.core.management.base import BaseCommand

from sc_admin_portal.oracle_proc import ensure_proc_tables_exist


class Command(BaseCommand):
    help = "Create PROC_ Gemini job/approval tables in Oracle if missing."

    def handle(self, *args, **options):
        ensure_proc_tables_exist()
        self.stdout.write(self.style.SUCCESS("PROC_ tables are ready."))

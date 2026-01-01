from django.core.management.base import BaseCommand

from sc_admin_portal.models import SocialDraftPost


class Command(BaseCommand):
    help = "Delete all SocialDraftPost rows from the Django database."

    def handle(self, *args, **options):
        deleted, _ = SocialDraftPost.objects.all().delete()
        self.stdout.write(self.style.SUCCESS(f"Deleted {deleted} SocialDraftPost rows."))

from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="SocialDraftPost",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("title", models.CharField(blank=True, max_length=200)),
                ("body_text", models.TextField()),
                ("hashtags", models.TextField(blank=True)),
                ("platform", models.CharField(default="linkedin_page", max_length=64)),
                (
                    "status",
                    models.CharField(
                        choices=[("DRAFT", "Draft"), ("APPROVED", "Approved"), ("REJECTED", "Rejected")],
                        default="DRAFT",
                        max_length=16,
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("approved_at", models.DateTimeField(blank=True, null=True)),
                ("published_url", models.URLField(blank=True, null=True)),
            ],
            options={"ordering": ["-created_at"]},
        )
    ]

from celery import shared_task


@shared_task()
def task_get_single_provider_onboarding_aggregate_data(provider_id):
    import json

    from apps.providers.models import Provider
    from libs.logging import LoggingAdapterBuilder

    task_logger = LoggingAdapterBuilder().set_prefix("[ONBOARDING-AGGREGATE]").build()

    try:
        provider = Provider.objects.get(id=provider_id)
        percent_complete = provider.get_percent_complete()
        missing_sections = provider.get_missing_sections()
        info = {
            "percent_complete": percent_complete,
            "missing_sections": missing_sections,
        }
        task_logger.info(f"Provider data {provider.id}: {json.dumps(info)}\n")
    except Exception:
        task_logger.error(f"Provider {provider_id} failed")


@shared_task()
def task_get_provider_onboarding_aggregate_data():
    from apps.core.helpers import chunked_queryset
    from apps.providers.models import Provider, ProviderChecklist
    from libs.logging import LoggingAdapterBuilder

    task_logger = LoggingAdapterBuilder().set_prefix("[ONBOARDING-AGGREGATE]").build()

    CHUNK_SIZE = 300

    provider_ids = (
        ProviderChecklist.objects.prefetch_related("provider__user__org_memberships")
        .filter(
            deleted__isnull=True,
            provider__user__org_memberships__is_active=True,
            provider__user__org_memberships__organization__is_demo_account=False,
            unique_key__startswith="pe-intake-",
        )
        .values_list("provider_id", flat=True)
        .distinct()
    )
    providers = Provider.objects.filter(id__in=provider_ids)
    total = providers.count()
    current = 0

    for chunk in chunked_queryset(providers, CHUNK_SIZE):
        for provider in chunk:
            current += 1
            task_get_single_provider_onboarding_aggregate_data.delay(provider.id)
            task_logger.info(f"Progress: {current} / {total}")

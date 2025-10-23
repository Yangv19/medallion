import logging

from celery import shared_task

from libs.gen.completion_pb2 import DataRequirement
from libs.gen.provider_completion_pb2 import ProviderDataRequirement

logger = logging.getLogger(__name__)


@shared_task()
def task_compute_checklist_mismatch():
    from apps.checklists.models import Checklist
    from apps.core.helpers import chunked_queryset
    from apps.providers.models import ProviderChecklist

    logger.info("[COMPUTE CHECKLIST MISMATCH] Starting")
    checklists = Checklist.objects.prefetch_related(
        "providers__user__org_memberships"
    ).filter(
        unique_key__startswith="pe-intake-",
        deleted__isnull=True,
        providers__user__org_memberships__is_active=True,
    )
    processed, total = 1, checklists.count()
    match, mismatch = 0, 0
    for chunk in chunked_queryset(checklists, 500):
        for cl in chunk:
            logger.info(
                f"[COMPUTE CHECKLIST MISMATCH] Processing checklist {cl.id}: {processed}/{total}"
            )
            provider_checklist = ProviderChecklist.objects.get(unique_key=cl.unique_key)
            if (
                cl.percent_complete
                != provider_checklist.primitive().latest_report.percent_complete
            ):
                mismatch += 1
            else:
                match += 1
            processed += 1
    logger.info(f"[COMPUTE CHECKLIST MISMATCH] Match: {match}, Mismatch: {mismatch}")


@shared_task()
def task_compute_checklist_mismatch_avoid_has_mismatching_related_object():
    from apps.checklists.models import Checklist
    from apps.core.helpers import chunked_queryset
    from apps.providers.models import ProviderChecklist

    logger.info("[COMPUTE CHECKLIST MISMATCH] Starting related object check")
    checklists = Checklist.objects.prefetch_related(
        "providers__user__org_memberships"
    ).filter(
        unique_key__startswith="pe-intake-",
        deleted__isnull=True,
        providers__user__org_memberships__is_active=True,
    )
    processed, total = 1, checklists.count()
    match, mismatch = 0, 0
    for chunk in chunked_queryset(checklists, 500):
        for cl in chunk:
            logger.info(
                f"[COMPUTE CHECKLIST MISMATCH FILTER] Processing checklist {cl.id}: {processed}/{total}"
            )
            provider_checklist = ProviderChecklist.objects.get(unique_key=cl.unique_key)
            processed_requirements = (
                provider_checklist.primitive().latest_report.processed_requirements
            )
            if any(req.processing_status == 5 for req in processed_requirements):
                continue
            if (
                cl.percent_complete
                != provider_checklist.primitive().latest_report.percent_complete
            ):
                mismatch += 1
            else:
                match += 1
            processed += 1
    logger.info(
        f"[COMPUTE CHECKLIST MISMATCH FILTER] Match: {match}, Mismatch: {mismatch}"
    )


@shared_task()
def task_compute_checklist_mismatch_percent():
    from apps.checklists.models import Checklist
    from apps.core.helpers import chunked_queryset
    from apps.providers.models import ProviderChecklist

    logger.info("[COMPUTE CHECKLIST MISMATCH PERCENT] Starting")
    checklists = Checklist.objects.prefetch_related(
        "providers__user__org_memberships"
    ).filter(
        unique_key__startswith="pe-intake-",
        deleted__isnull=True,
        providers__user__org_memberships__is_active=True,
    )
    processed, total = 1, checklists.count()
    map = {
        range(3): [],
        range(3, 5): [],
        range(5, 10): [],
        range(10, 20): [],
        range(20, 30): [],
        range(30, 40): [],
        range(40, 50): [],
        range(50, 60): [],
        range(60, 70): [],
        range(70, 80): [],
        range(80, 90): [],
        range(90, 101): [],
    }
    for chunk in chunked_queryset(checklists, 500):
        for cl in chunk:
            logger.info(
                f"[COMPUTE CHECKLIST MISMATCH PERCENT] Processing checklist {cl.id}: {processed}/{total}"
            )
            provider_checklist = ProviderChecklist.objects.get(unique_key=cl.unique_key)
            processed_requirements = (
                provider_checklist.primitive().latest_report.processed_requirements
            )
            if any(req.processing_status == 5 for req in processed_requirements):
                continue
            if (
                cl.percent_complete
                != provider_checklist.primitive().latest_report.percent_complete
            ):
                diff = abs(
                    cl.percent_complete
                    - provider_checklist.primitive().latest_report.percent_complete
                )
                for k, v in map.items():
                    if diff in k:
                        map[k].append(cl.id)
                        break
            processed += 1
    for k, v in map.items():
        # k is a range, so we need to convert it to a string
        k = f"{k.start}-{k.stop}"
        logger.info(f"[COMPUTE CHECKLIST MISMATCH PERCENT] {k}: {v}")
        logger.info(f"[COMPUTE CHECKLIST MISMATCH PERCENT] {k}: {len(v)}")


def get_requirement_kind_v2(requirement: DataRequirement) -> str:
    if requirement.WhichOneof("requirement") == "any_of":
        if requirement.name == "Citizenship and Documentation":
            return "citizenship_and_documentation"
        elif (
            requirement.any_of.requirements[0].provider_requirement.WhichOneof(
                "requirement"
            )
            == "military_history"
        ):
            return "military_history_assignment"
        elif (
            requirement.any_of.requirements[0].provider_requirement.WhichOneof(
                "requirement"
            )
            == "us_graduated"
        ):
            return "md_us_graduated_or_foreign_certificate"
        elif (
            requirement.any_of.requirements[0].provider_requirement.WhichOneof(
                "requirement"
            )
            == "license"
        ):
            return "license"
        elif (
            requirement.any_of.requirements[0].provider_requirement.WhichOneof(
                "requirement"
            )
            == "exam"
        ):
            return "exam"
        elif (
            requirement.any_of.requirements[0].provider_requirement.WhichOneof(
                "requirement"
            )
            == "liability_insurance"
        ):
            return "group liability_insurance"
        else:
            return f"document {requirement.any_of.requirements[0].provider_requirement.document.constraints.matching_kind}"
    else:
        if requirement.WhichOneof("requirement") == "provider_requirement":
            kind = requirement.provider_requirement.WhichOneof("requirement")
            if kind == "standalone_review":
                name = requirement.name
                if name == "Review Provider Name Matches":
                    return "standalone_provider_name"
                elif name == "Verify Current Employer Using Medallion":
                    return "standalone_current_employer"
                else:
                    return "standalone_board_certification"
            elif kind == "medical_program":
                return f"medical_program {requirement.name}"
            elif kind == "document":
                return f"document {requirement.provider_requirement.document.constraints.matching_kind}"
            elif kind == "practice_start_date":
                return f"practice {requirement.provider_requirement.practice_start_date.constraints.matching_practice_id.value} practice_start_date"
            return kind
        elif requirement.WhichOneof("requirement") == "practice_requirement":
            requirement_kind = requirement.practice_requirement.WhichOneof(
                "requirement"
            )
            return (
                f"practice {requirement.principal.practice_id.value} {requirement_kind}"
            )
        else:
            requirement_kind = requirement.group_requirement.WhichOneof("requirement")
            return f"group {requirement_kind}"


def get_requirement_kind_v1(requirement: ProviderDataRequirement) -> str:
    if requirement.WhichOneof("requirement") == "any_of":
        if requirement.name == "Us Citizen":
            return "citizenship_and_documentation"
        elif (
            requirement.any_of.requirements[0].WhichOneof("requirement")
            == "military_history"
        ):
            return "military_history_assignment"
        elif (
            requirement.any_of.requirements[0].WhichOneof("requirement")
            == "us_graduated"
        ):
            return "md_us_graduated_or_foreign_certificate"
        elif requirement.any_of.requirements[0].WhichOneof("requirement") == "license":
            return "license"
        elif requirement.any_of.requirements[0].WhichOneof("requirement") == "exam":
            return "exam"
        else:
            return f"document {requirement.any_of.requirements[0].document.constraints.matching_kind}"
    elif requirement.WhichOneof("requirement") == "standalone_verification":
        name = requirement.name
        if name == "Verify Provider Name Matches":
            return "standalone_provider_name"
        elif name == "Verify Current Employer Using Medallion":
            return "standalone_current_employer"
        else:
            return "standalone_board_certification"
    elif requirement.WhichOneof("requirement") == "medical_program":
        return f"medical_program {requirement.name}"
    elif requirement.WhichOneof("requirement") == "document":
        return f"document {requirement.document.constraints.matching_kind}"
    elif requirement.WhichOneof("requirement") == "related_practice_requirement":
        requirement_kind = requirement.related_practice_requirement.WhichOneof(
            "requirement"
        )
        if requirement_kind == "start_date":
            requirement_kind = "practice_start_date"
        return f"practice {requirement.related_practice_requirement.relation_constraints.practice_id.value} {requirement_kind}"
    elif requirement.WhichOneof("requirement") == "related_group_requirement":
        requirement_kind = requirement.related_group_requirement.WhichOneof(
            "requirement"
        )
        return f"group {requirement_kind}"
    else:
        return requirement.WhichOneof("requirement")


def requirement_statuses_equal(
    req_v1: ProviderDataRequirement, req_v2: DataRequirement
) -> bool:
    if req_v1.processing_status == 3 and req_v2.processing_status == 6:
        return True
    return req_v1.processing_status == req_v2.processing_status


@shared_task()
def task_compute_checklist_mismatch_requirement_kind():
    from collections import defaultdict

    from apps.checklists.models import Checklist
    from apps.core.helpers import chunked_raw_queryset
    from apps.providers.models import ProviderChecklist

    checklists = Checklist.objects.prefetch_related(
        "providers__user__org_memberships"
    ).filter(
        unique_key__startswith="pe-intake-",
        deleted__isnull=True,
        providers__user__org_memberships__is_active=True,
    )
    processed, total = 1, checklists.count()
    map = defaultdict(list)
    for chunk in chunked_raw_queryset(checklists, 500):
        for cl in chunk:
            logger.info(
                f"[COMPUTE CHECKLIST MISMATCH REQUIREMENT KIND] Processing checklist {cl.id}: {processed}/{total}"
            )
            provider_checklist = ProviderChecklist.objects.get(unique_key=cl.unique_key)
            processed_requirements_v1 = (
                provider_checklist.primitive().latest_report.processed_requirements
            )
            if any(req.processing_status == 5 for req in processed_requirements_v1):
                continue
            processed_requirements_v2 = (
                cl.primitive().latest_report.processed_requirements
            )
            if (
                cl.percent_complete
                != provider_checklist.primitive().latest_report.percent_complete
            ):
                for req_v2 in processed_requirements_v2:
                    req_v2_kind = get_requirement_kind_v2(req_v2)
                    for req_v1 in processed_requirements_v1:
                        if req_v2_kind == get_requirement_kind_v1(
                            req_v1
                        ) and not requirement_statuses_equal(req_v1, req_v2):
                            map[req_v2_kind].append(cl.id)
                            break
            processed += 1
    for k, v in map.items():
        logger.info(f"[COMPUTE CHECKLIST MISMATCH REQUIREMENT KIND] {k}: {v}")
        logger.info(f"[COMPUTE CHECKLIST MISMATCH REQUIREMENT KIND] {k}: {len(v)}")


@shared_task()
def task_get_single_provider_onboarding_aggregate_data(provider_id):
    import json

    from apps.providers.models import Provider

    try:
        provider = Provider.objects.get(id=provider_id)
        percent_complete = provider.get_percent_complete()
        missing_sections = provider.get_missing_sections()
        info = {
            "percent_complete": percent_complete,
            "missing_sections": missing_sections,
        }
        logger.info(
            f"[ONBOARDING-AGGREGATE] Provider data {provider.id}: {json.dumps(info)}\n"
        )
    except Exception:
        logger.error(f"[ONBOARDING-AGGREGATE] Provider {provider_id} failed")


@shared_task()
def task_get_provider_onboarding_aggregate_data():
    from apps.checklists.models import Checklist
    from apps.core.helpers import chunked_queryset
    from apps.providers.models import Provider

    CHUNK_SIZE = 300

    provider_ids = (
        Checklist.objects.prefetch_related("providers__user__org_memberships")
        .filter(
            deleted__isnull=True,
            providers__user__org_memberships__is_active=True,
        )
        .values_list("providers__id", flat=True)
        .distinct()
    )
    providers = Provider.objects.filter(id__in=provider_ids)
    total = providers.count()
    current = 0

    for chunk in chunked_queryset(providers, CHUNK_SIZE):
        for provider in chunk:
            current += 1
            task_get_single_provider_onboarding_aggregate_data.delay(provider.id)
            logger.info(f"[ONBOARDING-AGGREGATE] Progress: {current} / {total}")

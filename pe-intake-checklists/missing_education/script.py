from apps.providers.models import Provider
from collections import defaultdict

# List of provider IDs with missing education history
ids = ['ff331bdd-448c-4b23-a414-a63c9a6ec46f', 'fed9574d-9539-46cd-9f94-edbd9d3a45a5',
                'fe18d828-3c54-4ccb-92a5-0f3999487b14', 'ff5dc753-8426-42da-a274-c5e5253e9e01',
                'fdb84187-b199-4a13-b974-a8250c691571', 'ffc6ff4e-98b7-4372-a7ef-32482a72e0ca',
                'ffdae9f3-5fc4-46b2-b37c-b7e6aee2d361', 'ffe09c09-92da-4453-b56d-9eebc6986ef4',
                'ff76cf29-29b2-43e4-8a8d-e49085896bdd', 'ffd0b837-64a1-4827-896d-bbacbc5c8a7d',
                'fffec8c6-a5d9-40cd-bd71-4afdb9cc1e7e', 'ff0ade41-93cb-4b79-9470-1383c734680b',
                'ff377be8-9b7d-4d10-af87-bbd84a63461b', 'ff287942-190f-45e0-8f3b-a1907ca7fa63',
                'fe99fc73-7371-4b15-97bc-238d24a6344d', 'fe6f85b9-de3b-43f6-a8e8-c1cbcfe44865',
                'fe73d51e-7964-4e29-a27d-222f273fb994', 'fe03bede-b63a-4011-a979-ce3f67c085f2',
                'fdcd699e-196e-4ffc-b2a1-68db2d43a8a8', 'fe1f934d-795d-41c2-bc32-1b641400f2be',
                'fe2fd80e-957a-4bcb-933e-8597e9f0e081', 'fdd52345-111c-40c3-a53b-754740fe07e5']

# Fetch providers
providers = Provider.objects.filter(id__in=ids)

# Initialize counters
org_counts = defaultdict(int)
profession_counts = defaultdict(int)

# Aggregate data
for provider in providers:
    if provider.user.org and not provider.user.org.is_demo_account:
        org_counts[provider.user.org.name] += 1
        profession_counts[provider.profession] += 1

# Print results
print("\n=== Organizations ===")
for org_name, count in sorted(org_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"{org_name}: {count} providers")

print("\n=== Professions ===")
for profession, count in sorted(profession_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"{profession}: {count} providers")

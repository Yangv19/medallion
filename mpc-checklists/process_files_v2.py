#!/usr/bin/env python3
import csv
import re
import json
from collections import defaultdict

# File paths
before_file = "before_turning_on_ff_deduped.csv"
after_file = "after_deduped.csv"

# Message is in the 4th column (index 3)
MESSAGE_COL_INDEX = 3


def extract_provider_id_and_json(message):
    """Extract the provider ID and JSON data from the message."""
    # Extract provider ID
    id_match = re.search(r'Provider data ([0-9a-f-]+):', message)
    if not id_match:
        return None, None

    provider_id = id_match.group(1)

    # Extract JSON data
    json_match = re.search(r'({.*})', message)
    if not json_match:
        return provider_id, None

    json_str = json_match.group(1)
    try:
        json_data = json.loads(json_str)
        return provider_id, json_data
    except json.JSONDecodeError:
        return provider_id, None


def build_message_map(file_path):
    """Build a map of provider IDs to JSON data from a CSV file."""
    message_map = {}

    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Skip header

        for row in reader:
            message = row[MESSAGE_COL_INDEX]
            provider_id, json_data = extract_provider_id_and_json(message)

            if provider_id and json_data:
                message_map[provider_id] = json_data

    return message_map


def analyze_percentage_distribution(message_map, label):
    """Analyze the distribution of percent_complete values in the message map."""
    # Initialize counters for each range
    ranges = {
        "0-9%": 0,
        "10-19%": 0,
        "20-29%": 0,
        "30-39%": 0,
        "40-49%": 0,
        "50-59%": 0,
        "60-69%": 0,
        "70-79%": 0,
        "80-89%": 0,
        "90-99%": 0,
        "100%": 0
    }

    # Count providers in each range
    for provider_id, data in message_map.items():
        percent = data.get("percent_complete", 0)

        if percent == 100:
            ranges["100%"] += 1
        elif 0 <= percent < 10:
            ranges["0-9%"] += 1
        elif 10 <= percent < 20:
            ranges["10-19%"] += 1
        elif 20 <= percent < 30:
            ranges["20-29%"] += 1
        elif 30 <= percent < 40:
            ranges["30-39%"] += 1
        elif 40 <= percent < 50:
            ranges["40-49%"] += 1
        elif 50 <= percent < 60:
            ranges["50-59%"] += 1
        elif 60 <= percent < 70:
            ranges["60-69%"] += 1
        elif 70 <= percent < 80:
            ranges["70-79%"] += 1
        elif 80 <= percent < 90:
            ranges["80-89%"] += 1
        elif 90 <= percent < 100:
            ranges["90-99%"] += 1

    # Calculate total
    total = sum(ranges.values())

    # Print the distribution
    print(f"\nPercentage Distribution for {label} (Total: {total} providers):")
    print("-" * 50)
    print(f"{'Range':<10} {'Count':<10} {'Percentage':<10}")
    print("-" * 50)

    for range_label, count in ranges.items():
        percentage = (count / total) * 100 if total > 0 else 0
        print(f"{range_label:<10} {count:<10} {percentage:.2f}%")


# Build maps for both files
print(f"Building map for 'before' file: {before_file}")
before_map = build_message_map(before_file)
print(f"Found {len(before_map)} provider entries in 'before' file")

print(f"\nBuilding map for 'after' file: {after_file}")
after_map = build_message_map(after_file)
print(f"Found {len(after_map)} provider entries in 'after' file")

# Analyze percentage distributions
analyze_percentage_distribution(before_map, "Before")
analyze_percentage_distribution(after_map, "After")

# Find common provider IDs
common_ids = set(before_map.keys()) & set(after_map.keys())
print(f"\nFound {len(common_ids)} provider IDs common to both files")

# Initialize counters for missing sections analysis
fields_in_before_not_after = defaultdict(list)  # Provider ID -> [(section, field), ...]
new_missing_fields = defaultdict(list)  # (section, field) -> [provider_ids]


def flatten_fields(fields):
    """Flatten nested lists in fields."""
    result = []

    if not fields:
        return result

    if not isinstance(fields, list):
        return [str(fields)]

    for field in fields:
        if isinstance(field, list):
            result.extend(flatten_fields(field))
        else:
            result.append(str(field))

    return result


# Analyze missing sections for each provider
for provider_id in common_ids:
    before_data = before_map[provider_id]
    after_data = after_map[provider_id]

    # Get missing sections from both maps
    before_missing = before_data.get("missing_sections", {})
    after_missing = after_data.get("missing_sections", {})

    # Check for fields in before but not in after (shouldn't happen)
    for section, fields in before_missing.items():
        # Flatten any nested lists in fields
        flat_before_fields = flatten_fields(fields)

        if not flat_before_fields:
            continue

        # If section doesn't exist in after, all fields are missing
        if section not in after_missing:
            for field in flat_before_fields:
                # Store provider ID for this field
                fields_in_before_not_after[f"{section}:{field}"].append(provider_id)
        else:
            # Flatten any nested lists in after fields
            flat_after_fields = flatten_fields(after_missing.get(section, []))

            # Check each field
            for field in flat_before_fields:
                if field not in flat_after_fields:
                    # Store provider ID for this field
                    fields_in_before_not_after[f"{section}:{field}"].append(provider_id)

    # Check for fields in after but not in before (new missing fields)
    for section, fields in after_missing.items():
        # Flatten any nested lists in fields
        flat_after_fields = flatten_fields(fields)

        if not flat_after_fields:
            continue

        # If section doesn't exist in before, all fields are new
        if section not in before_missing:
            for field in flat_after_fields:
                # Store provider ID for this field
                new_missing_fields[f"{section}:{field}"].append(provider_id)
        else:
            # Flatten any nested lists in before fields
            flat_before_fields = flatten_fields(before_missing.get(section, []))

            # Check each field
            for field in flat_after_fields:
                if field not in flat_before_fields:
                    # Store provider ID for this field
                    new_missing_fields[f"{section}:{field}"].append(provider_id)

# Write old missing fields to CSV
if fields_in_before_not_after:
    print("\nWriting old missing fields in 'before' but not in 'after' to old_missing_fields.csv:")
    with open("old_missing_fields.csv", 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Section', 'Field', 'Count', 'Provider IDs'])

        # Sort by count (descending)
        for key, provider_ids in sorted(fields_in_before_not_after.items(), key=lambda x: len(x[1]), reverse=True):
            section, field = key.split(":", 1)  # Split at first colon
            count = len(provider_ids)
            # Join provider IDs with pipe separator for easy parsing
            provider_ids_str = '|'.join(provider_ids)
            writer.writerow([section, field, count, provider_ids_str])
else:
    print("\nNo fields were found in 'before' but not in 'after'")

# Write new missing fields to CSV
if new_missing_fields:
    print(f"\nWriting new missing fields in 'after' that weren't in 'before' to new_missing_fields.csv")
    with open("new_missing_fields.csv", 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Section', 'Field', 'Count', 'Provider IDs'])

        # Sort by count (descending)
        for key, provider_ids in sorted(new_missing_fields.items(), key=lambda x: len(x[1]), reverse=True):
            section, field = key.split(":", 1)  # Split at first colon
            count = len(provider_ids)
            # Join provider IDs with pipe separator for easy parsing
            provider_ids_str = '|'.join(provider_ids)
            writer.writerow([section, field, count, provider_ids_str])

    print(f"Found {len(new_missing_fields)} new missing fields in 'after'")
else:
    print("\nNo new missing fields were found in 'after'")

print("\nAnalysis complete!")

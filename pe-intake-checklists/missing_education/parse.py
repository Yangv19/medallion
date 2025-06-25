import json
import csv
import re

# Regular expression to extract provider ID and missing sections data
pattern = r'\[ONBOARDING-AGGREGATE\] Provider data ([a-f0-9-]+): (.*)'

# The specific string we're looking for
target_string = "Missing Required Education History: Institution for Professional Degree"

# List to store matching provider IDs
matching_ids = []

# Read the file and process each line
with open('sheet.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        if not row:
            continue

        # Extract the log entry
        log_entry = row[0]
        match = re.search(pattern, log_entry)

        if match:
            provider_id = match.group(1)
            data_str = match.group(2)

            try:
                # Parse the JSON data
                data = json.loads(data_str)
                missing_sections = data.get("missing_sections", {})

                # Check if education section contains our target string
                education_issues = missing_sections.get("education", [])
                if target_string in education_issues:
                    matching_ids.append(provider_id)
            except json.JSONDecodeError:
                print(f"Failed to parse JSON for provider {provider_id}")

# Print results as a Python list
print(matching_ids)
print(len(matching_ids))

#!/usr/bin/env python3
import csv

# Input and output file paths
input_file = "after.csv"
output_file = "after_deduped.csv"

# Message is in the 4th column (index 3)
MESSAGE_COL_INDEX = 3

# Read the CSV file
with open(input_file, 'r', newline='') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)  # Save the header
    rows = list(reader)    # Read all rows

print(f"Read {len(rows)} rows from {input_file}")
print(f"Using column {header[MESSAGE_COL_INDEX]} as the message column")

# Global deduplication - remove all duplicates regardless of position
seen_messages = set()
deduped_rows = []

for row in rows:
    message = row[MESSAGE_COL_INDEX]
    if message not in seen_messages:
        seen_messages.add(message)
        deduped_rows.append(row)

print(f"Removed {len(rows) - len(deduped_rows)} duplicate rows (global deduplication)")
print(f"Remaining rows: {len(deduped_rows)}")

# Write the deduplicated data to the output file
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(header)  # Write the header
    writer.writerows(deduped_rows)

print(f"Deduplicated data written to {output_file}")

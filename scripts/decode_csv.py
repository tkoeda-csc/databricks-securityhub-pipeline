#!/usr/bin/env python3
"""
Decode Base64-encoded CSV from bronze_to_s3 pipeline
Usage: python decode_csv.py <file.csv.gz>
"""

import base64
import csv
import gzip
import json
import sys


def decode_csv(filename):
    """Decode Base64-encoded standards_summary from CSV."""

    with gzip.open(filename, "rt") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, 1):
            print(f"\n{'=' * 80}")
            print(f"ROW {i}")
            print(f"{'=' * 80}")

            # Display regular fields
            print(f"Company ID:          {row['company_id']}")
            print(f"Account ID:          {row['account_id']}")
            print(f"Region:              {row['region_id']}")
            print(f"Control Pass Score:  {row['control_pass_score']}%")
            print(f"Total Rules:         {row['total_rules']}")
            print(f"Total Passed:        {row['total_passed']}")
            print(f"Processed Time:      {row['cf_processed_time']}")

            # Decode Base64 standards_summary
            encoded = row["standards_summary"]
            try:
                decoded_bytes = base64.b64decode(encoded)
                decoded_str = decoded_bytes.decode("utf-8")
                standards = json.loads(decoded_str)

                print(f"\nStandards Summary:")
                print(json.dumps(standards, indent=2))

            except Exception as e:
                print(f"Error decoding: {e}")

            # Optional: stop after first few rows
            if i >= 3:
                print(f"\n... (showing first 3 rows only)")
                break


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python decode_csv.py <file.csv.gz>")
        sys.exit(1)

    decode_csv(sys.argv[1])

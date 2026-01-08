#!/bin/bash
# Quick CSV Base64 decoder
# Usage: ./decode_csv_quick.sh <file.csv.gz>

if [ $# -eq 0 ]; then
    echo "Usage: $0 <file.csv.gz>"
    exit 1
fi

gzip -dc "$1" | python3 -c "
import csv
import sys
import base64
import json

reader = csv.DictReader(sys.stdin)
for i, row in enumerate(reader, 1):
    print(f'\n{'='*80}')
    print(f'ROW {i}')
    print(f'{'='*80}')
    print(f'Company:  {row[\"company_id\"]}')
    print(f'Account:  {row[\"account_id\"]}')
    print(f'Region:   {row[\"region_id\"]}')
    print(f'Score:    {row[\"control_pass_score\"]}%')
    print(f'\nStandards Summary (decoded):')
    
    try:
        decoded = base64.b64decode(row['standards_summary']).decode('utf-8')
        data = json.loads(decoded)
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f'Error: {e}')
    
    if i >= 3:
        print(f'\n... (showing first 3 rows)')
        break
"

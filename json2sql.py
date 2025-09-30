# Usage:
#
# Create file.sqlite with column names "c0", "c1", ...
#   python json2sql.py --json-body file.json
#
# Create file.sqlite with column names from header.json
#   python json2sql.py --json-body file.json --json-head header.json
#
# Creates OUTFILE sqlite file.
#    python json2sql.py ... --out OUTFILE
#
# Example:
#   python json2sql.py --json-body demo/demo.body.json --json-head demo/demo.head.json
import tableui
import argparse

parser = argparse.ArgumentParser(description="Convert JSON to SQLite database.")
parser.add_argument('--json-body', required=True, help='Path to JSON file containing table body data.')
parser.add_argument('--json-head', help='Path to JSON file containing table header (column names).')
parser.add_argument('--out', help='Output SQLite file name (default is json-body file base name with extension sqlite).')

args = parser.parse_args()

tableui.json2sql(args.json_body, args.json_head, args.out)
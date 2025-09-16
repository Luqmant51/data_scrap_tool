import os
import pandas as pd

# Source file (merged CSV to clean)
source_file = r"USA/merge_unique/AZ/AZ_merged_unique.csv"

# Destination folder
dest_folder = r"USA/merge_unique_null/AZ"
os.makedirs(dest_folder, exist_ok=True)

# Output file paths
merged_file = os.path.join(dest_folder, "AZ_merged_unique.csv")
report_file = os.path.join(dest_folder, "AZ_merge_report_unique.csv")

# Read the merged CSV
if not os.path.isfile(source_file):
    print(f"Source file not found: {source_file}")
    exit()

df = pd.read_csv(source_file)

# Function to check if Phone(s) column has a real number
def has_phone(x):
    if pd.isna(x):
        return False
    val = str(x).strip().upper()
    return val != "" and val != "N/A"

# Count before cleaning
row_count = len(df)
with_phone = df['Phone(s)'].apply(has_phone).sum()
without_phone = row_count - with_phone

# Drop rows where any field is null/blank
df = df.dropna(how="any")
df = df[~df.apply(lambda row: row.astype(str).str.strip().eq("").any(), axis=1)]

# Remove duplicate rows
df = df.drop_duplicates()
unique_rows = len(df)

# Count phone numbers after cleaning
total_with_phone = df['Phone(s)'].apply(has_phone).sum()
total_without_phone = unique_rows - total_with_phone

# Save cleaned CSV
df.to_csv(merged_file, index=False, encoding="utf-8")

# Save report CSV
report_data = [
    {"file_name": os.path.basename(source_file), "rows": row_count, "with_phone": with_phone, "without_phone": without_phone},
    {"file_name": "TOTAL", "rows": unique_rows, "with_phone": total_with_phone, "without_phone": total_without_phone}
]
report_df = pd.DataFrame(report_data)
report_df.to_csv(report_file, index=False, encoding="utf-8")

# Print summary
print(f"✅ Cleaned CSV saved at: {merged_file}")
print(f"✅ Report saved at: {report_file}")

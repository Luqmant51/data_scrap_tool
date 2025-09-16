import os
import pandas as pd

# Source folder containing all CSVs
source_folder = r"USA/AZ"

# Destination folder for merged CSV and report
dest_folder = r'USA\merge\AZ'
os.makedirs(dest_folder, exist_ok=True)

# Output file paths
merged_file = os.path.join(dest_folder, "AZ_merged.csv")
report_file = os.path.join(dest_folder, "AZ_merge_report.csv")

# List all CSV files in source folder
csv_files = [f for f in os.listdir(source_folder) if f.endswith(".csv")]

if not csv_files:
    print("No CSV files found in the source folder!")
    exit()

# Function to check if Phone(s) column has a real number
def has_phone(x):
    if pd.isna(x):
        return False
    val = str(x).strip().upper()
    return val != "" and val != "N/A"

# Prepare data for report
report_data = []

# Read and merge CSVs
df_list = []
for file in csv_files:
    file_path = os.path.join(source_folder, file)
    df = pd.read_csv(file_path)
    row_count = len(df)
    # Count rows with and without phone numbers
    with_phone = df['Phone(s)'].apply(has_phone).sum()
    without_phone = row_count - with_phone
    report_data.append({
        "file_name": file,
        "rows": row_count,
        "with_phone": with_phone,
        "without_phone": without_phone
    })
    df_list.append(df)

# Merge all CSVs
merged_df = pd.concat(df_list, ignore_index=True)

# Overall totals
total_rows = len(merged_df)
total_with_phone = merged_df['Phone(s)'].apply(has_phone).sum()
total_without_phone = total_rows - total_with_phone

# Save merged CSV
merged_df.to_csv(merged_file, index=False, encoding="utf-8")

# Add totals to report
report_data.append({
    "file_name": "TOTAL",
    "rows": total_rows,
    "with_phone": total_with_phone,
    "without_phone": total_without_phone
})

# Save report CSV
report_df = pd.DataFrame(report_data)
report_df.to_csv(report_file, index=False, encoding="utf-8")

print(f"✅ Merged CSV saved at: {merged_file}")
print(f"✅ Merge report saved at: {report_file}")

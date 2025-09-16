import os
import pandas as pd

# Main folder path
folder_path = r"C:\Users\LUQMAN\Desktop\New folder (3)"

# Collect all CSV file paths (including subfolders)
csv_files = []
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith(".csv"):
            csv_files.append(os.path.join(root, file))

print(f"📂 Found {len(csv_files)} CSV files.")

# Merge all CSVs
all_data = []
for file in csv_files:
    try:
        df = pd.read_csv(file, dtype=str)  # read as string to avoid type issues
        df["__source_file"] = os.path.basename(file)  # keep track of source file
        all_data.append(df)
    except Exception as e:
        print(f"❌ Error reading {file}: {e}")

if not all_data:
    print("⚠️ No CSV data found.")
    exit()

merged_df = pd.concat(all_data, ignore_index=True)

# ✅ Check duplicates by Business Name + Phone + Address
key_cols = ["Business Name", "Phone", "Address"]

# Ensure required columns exist
if not all(col in merged_df.columns for col in key_cols):
    print(f"❌ One or more required columns are missing. Found columns: {merged_df.columns.tolist()}")
    exit()

# Find duplicates
duplicates = merged_df[merged_df.duplicated(subset=key_cols, keep=False)]

# Save duplicates separately
if not duplicates.empty:
    dup_file = os.path.join(folder_path, "duplicates_by_BusinessName_Phone_Address.csv")
    duplicates.to_csv(dup_file, index=False, encoding="utf-8")
    print(f"⚠️ Found {len(duplicates)} duplicate rows. Saved to {dup_file}")
else:
    print("🎉 No duplicate rows found initially.")

# ✅ Remove duplicates (keep first occurrence)
cleaned_df = merged_df.drop_duplicates(subset=key_cols, keep="first")

# Save cleaned merged CSV
cleaned_file = os.path.join(folder_path, "all_data_merged_clean_new.csv")
cleaned_df.to_csv(cleaned_file, index=False, encoding="utf-8")
print(f"✅ Cleaned merged CSV saved to {cleaned_file} with {len(cleaned_df)} rows.")

# ✅ Re-check for duplicates to confirm
final_duplicates = cleaned_df[cleaned_df.duplicated(subset=key_cols, keep=False)]

if final_duplicates.empty:
    print("🎉 Final check passed: No duplicates remain in cleaned file.")
else:
    print(f"⚠️ Still found {len(final_duplicates)} duplicates after cleaning.")

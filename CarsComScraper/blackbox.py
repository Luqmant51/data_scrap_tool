# app.py
import io
from typing import List, Optional

import numpy as np
import pandas as pd
import streamlit as st


# -----------------------------
# Helpers
# -----------------------------

def profile_df(df: pd.DataFrame) -> dict:
    """Quick profile for display."""
    return {
        "rows": df.shape[0],
        "cols": df.shape[1],
        "memory_mb": round(df.memory_usage(deep=True).sum() / (1024 ** 2), 2),
        "missing_top": df.isna().sum().sort_values(ascending=False).head(10),
        "dtype_counts": df.dtypes.astype(str).value_counts().to_dict(),
    }

def to_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def to_datetime(df: pd.DataFrame, cols: List[str], fmt: Optional[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], format=fmt if fmt else None, errors="coerce")
    return df

def remove_outliers_z(df: pd.DataFrame, cols: List[str], z: float) -> pd.DataFrame:
    """Drop rows where any selected numeric col has |z| > threshold."""
    cols = [c for c in cols if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]
    if not cols:
        return df
    df = df.copy()
    mask = pd.Series(True, index=df.index)
    for c in cols:
        col = df[c]
        mu, sigma = col.mean(skipna=True), col.std(skipna=True)
        if sigma and not np.isnan(sigma) and sigma != 0:
            zscores = (col - mu) / sigma
            mask &= zscores.abs().le(z) | col.isna()
    return df[mask]

def scale_cols(df: pd.DataFrame, cols: List[str], method: str) -> pd.DataFrame:
    cols = [c for c in cols if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]
    if not cols:
        return df
    df = df.copy()
    if method == "zscore":
        for c in cols:
            mu, sigma = df[c].mean(skipna=True), df[c].std(skipna=True)
            if sigma and not np.isnan(sigma) and sigma != 0:
                df[c] = (df[c] - mu) / sigma
    elif method == "minmax":
        for c in cols:
            mn, mx = df[c].min(skipna=True), df[c].max(skipna=True)
            rng = mx - mn
            if rng and not np.isnan(rng) and rng != 0:
                df[c] = (df[c] - mn) / rng
    return df

def rename_columns(df: pd.DataFrame, to_lower: bool, strip_spaces: bool, unders: bool) -> pd.DataFrame:
    df = df.copy()
    new_cols = []
    for c in df.columns:
        nc = c
        if strip_spaces:
            nc = nc.strip()
        if to_lower:
            nc = nc.lower()
        if unders:
            nc = nc.replace(" ", "_")
        new_cols.append(nc)
    df.columns = new_cols
    return df


# -----------------------------
# UI
# -----------------------------

st.set_page_config(page_title="CSV Data Treatment", layout="wide")
st.title("CSV Data Treatment App (pandas + numpy)")
st.caption("Upload a CSV, choose operations, preview, and download the cleaned data.")

with st.sidebar:
    st.header("1) Upload CSV")
    uploaded = st.file_uploader("Choose CSV", type=["csv"])
    st.subheader("CSV options")
    delimiter = st.text_input("Delimiter", value=",")
    encoding = st.text_input("Encoding", value="utf-8")
    header_row = st.number_input("Header row (-1 = no header)", min_value=-1, value=0, step=1)

if uploaded is None:
    st.info("Upload a CSV file to get started.")
    st.stop()

# Read CSV with error handling
try:
    df_raw = pd.read_csv(
        uploaded,
        sep=delimiter or ",",
        encoding=encoding or None,
        header=None if header_row == -1 else header_row,
    )
except Exception as e:
    st.error(f"Failed to read CSV: {e}")
    st.stop()

# Profile + preview
prof = profile_df(df_raw)
c1, c2, c3 = st.columns(3)
c1.metric("Rows", f"{prof['rows']:,}")
c2.metric("Columns", f"{prof['cols']:,}")
c3.metric("Memory (MB)", prof["memory_mb"])
st.write("Dtype counts:", prof["dtype_counts"])
with st.expander("Top missing (by column)"):
    st.write(prof["missing_top"])
st.subheader("Source preview (first 200 rows)")
st.dataframe(df_raw.head(200), use_container_width=True)

# -----------------------------
# Operations
# -----------------------------
st.header("2) Cleaning and Transformations")

# Remove duplicates
st.subheader("Remove Duplicates")
rm_dupes = st.checkbox("Enable duplicate removal", value=False)
dupe_subset = st.multiselect("Columns to check (empty = all)", options=list(df_raw.columns))
keep_opt = st.selectbox("Keep which duplicate?", ["first", "last", "drop all"], index=0)

# Missing values
st.subheader("Handle Missing Values")
miss_strategy = st.selectbox("Strategy", ["none", "drop", "mean", "median", "mode", "constant"])
miss_cols = st.multiselect("Columns (empty = all)", options=list(df_raw.columns))
drop_how = st.selectbox("Drop rows if missing in selected columns", ["any", "all"], index=0) if miss_strategy == "drop" else "any"
fill_value = st.text_input("Fill value (for 'constant')", value="") if miss_strategy == "constant" else None

# Convert types
st.subheader("Convert Types")
num_cols_convert = st.multiselect("Convert to numeric (coerce invalid -> NaN)", options=list(df_raw.columns))
dt_cols_convert = st.multiselect("Convert to datetime (coerce invalid -> NaT)", options=list(df_raw.columns))
dt_format = st.text_input("Datetime format (optional, e.g., %Y-%m-%d)", value="")

# Outliers
st.subheader("Outliers")
out_enable = st.checkbox("Remove outliers via Z-score", value=False)
out_cols = st.multiselect("Numeric columns for outlier check", options=[c for c in df_raw.columns if pd.api.types.is_numeric_dtype(df_raw[c])]) if out_enable else []
z_thresh = st.slider("Z threshold", min_value=1.0, max_value=5.0, value=3.0, step=0.1) if out_enable else 3.0

# Scaling
st.subheader("Scaling")
scale_enable = st.checkbox("Scale numeric columns", value=False)
scale_method = st.selectbox("Method", ["zscore", "minmax"]) if scale_enable else "zscore"
scale_cols_sel = st.multiselect("Columns to scale", options=[c for c in df_raw.columns if pd.api.types.is_numeric_dtype(df_raw[c])]) if scale_enable else []

# Filter (advanced)
st.subheader("Filter Rows (advanced)")
filter_query = st.text_input('Pandas query string (e.g., colA > 10 and colB == "A")', value="")

# Rename columns
st.subheader("Rename Columns (clean-up)")
ren_enable = st.checkbox("Enable rename cleanup", value=True)
to_lower = st.checkbox("To lower case", value=True, disabled=not ren_enable)
strip_spaces = st.checkbox("Strip spaces", value=True, disabled=not ren_enable)
unders = st.checkbox("Spaces -> underscores", value=True, disabled=not ren_enable)

apply_btn = st.button("Apply operations")

# -----------------------------
# Apply pipeline
# -----------------------------
df_out = df_raw.copy()
changes = []

if apply_btn:
    # 1) Remove duplicates
    if rm_dupes:
        keep = False if keep_opt == "drop all" else keep_opt
        before = len(df_out)
        subset = None if len(dupe_subset) == 0 else dupe_subset
        df_out = df_out.drop_duplicates(subset=subset, keep=keep)
        changes.append(f"Removed duplicates: {before - len(df_out)} rows dropped")

    # 2) Missing values
    if miss_strategy != "none":
        cols = miss_cols or list(df_out.columns)
        if miss_strategy == "drop":
            before = len(df_out)
            df_out = df_out.dropna(subset=cols, how=drop_how)
            changes.append(f"Dropped rows with missing ({drop_how}) in {len(cols)} cols: {before - len(df_out)} rows")
        elif miss_strategy in {"mean", "median"}:
            numc = [c for c in cols if pd.api.types.is_numeric_dtype(df_out[c])]
            if numc:
                fill_map = {c: (df_out[c].mean() if miss_strategy == "mean" else df_out[c].median()) for c in numc}
                df_out[numc] = df_out[numc].fillna(fill_map)
                changes.append(f"Filled NaN in {len(numc)} numeric cols with {miss_strategy}")
        elif miss_strategy == "mode":
            for c in cols:
                mode_val = df_out[c].mode(dropna=True)
                if not mode_val.empty:
                    df_out[c] = df_out[c].fillna(mode_val.iloc[0])
            changes.append(f"Filled NaN with column mode for {len(cols)} cols")
        elif miss_strategy == "constant":
            df_out[cols] = df_out[cols].fillna(fill_value)
            changes.append(f"Filled NaN with constant value in {len(cols)} cols")

    # 3) Conversions
    if num_cols_convert:
        df_out = to_numeric(df_out, num_cols_convert)
        changes.append(f"Converted {len(num_cols_convert)} cols to numeric (coerce)")
    if dt_cols_convert:
        df_out = to_datetime(df_out, dt_cols_convert, fmt=dt_format or None)
        changes.append(f"Converted {len(dt_cols_convert)} cols to datetime")

    # 4) Outliers
    if out_enable and out_cols:
        before = len(df_out)
        df_out = remove_outliers_z(df_out, out_cols, z=z_thresh)
        changes.append(f"Outlier removal: {before - len(df_out)} rows dropped (z>{z_thresh})")

    # 5) Scaling
    if scale_enable and scale_cols_sel:
        df_out = scale_cols(df_out, scale_cols_sel, scale_method)
        changes.append(f"Scaled {len(scale_cols_sel)} cols with {scale_method}")

    # 6) Filter
    if filter_query.strip():
        try:
            before = len(df_out)
            df_out = df_out.query(filter_query)
            changes.append(f"Filter applied: {before - len(df_out)} rows removed")
        except Exception as e:
            st.warning(f"Query error: {e}")

    # 7) Rename columns
    if ren_enable:
        df_out = rename_columns(df_out, to_lower=to_lower, strip_spaces=strip_spaces, unders=unders)
        changes.append("Column names cleaned")

    # Results
    st.success("Operations applied.")
    c1, c2 = st.columns(2)
    c1.metric("Output rows", f"{len(df_out):,}")
    c2.metric("Output cols", f"{df_out.shape[1]:,}")

    if changes:
        st.write("Summary of changes:")
        for ch in changes:
            st.write("- " + ch)

    st.subheader("Result preview (first 200 rows)")
    st.dataframe(df_out.head(200), use_container_width=True)

    # Download
    buffer = io.StringIO()
    df_out.to_csv(buffer, index=False)
    st.download_button(
        "Download cleaned CSV",
        data=buffer.getvalue(),
        file_name="cleaned.csv",
        mime="text/csv",
    )
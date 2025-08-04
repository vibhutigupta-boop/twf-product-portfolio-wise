import streamlit as st
import pandas as pd
from io import BytesIO
import os

st.title("📊 Amazon Data Merger")

# Step 1: Upload Sales Files
st.header("Step 1: Upload Amazon Files (Excel or CSV)")
sales_files = st.file_uploader("Upload multiple reports", type=["xlsx", "xls", "csv"], accept_multiple_files=True)

# Step 2: Upload Mapping File
st.header("Step 2: Upload ASIN Mapping File")
map_file = st.file_uploader("Upload ASIN → Product Name + Portfolio mapping", type=["xlsx", "csv"])

# Placeholder for dataframes
dfs = {}

if sales_files and map_file:
    for file in sales_files:
        filename = file.name
        ext = os.path.splitext(filename)[1].lower()

        try:
            if ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file)
            else:
                df = pd.read_csv(file)

            if 'ASIN' not in df.columns:
                st.warning(f"❌ '{filename}' missing 'ASIN' column. Skipping.")
                continue

            # Clean and convert all numeric columns
            removed_cols = []
            for col in df.columns:
                if col == "ASIN":
                    continue
                try:
                    df[col] = pd.to_numeric(df[col], errors='raise')
                except:
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace(r'[^\d\.\-]', '', regex=True)
                    )
                    try:
                        df[col] = pd.to_numeric(df[col], errors='raise')
                    except:
                        removed_cols.append(col)

            if removed_cols:
                
                df = df.drop(columns=removed_cols)

            shortname = os.path.splitext(filename)[0][:6]
            df_name = f"df_{shortname}"

            # Rename columns based on filename pattern
            suffix = None
            if "Traffi" in df_name or "Sales_" in df_name:
                suffix = " VC"
            elif "Busine" in df_name or "IN_Sea" in df_name:
                suffix = " SC"

            if suffix:
                df = df.rename(columns={col: col + suffix for col in df.columns if col != "ASIN"})

            grouped = df.groupby("ASIN", as_index=False).sum(numeric_only=True)
            dfs[df_name] = grouped
            
        except Exception as e:
            st.error(f"❌ Error reading '{filename}': {e}")

    # Merge all cleaned dataframes
    from functools import reduce
    merged_df = reduce(lambda left, right: pd.merge(left, right, on="ASIN", how="outer"), dfs.values())

    # Read mapping file
    ext = os.path.splitext(map_file.name)[1].lower()
    if ext in ['.xlsx', '.xls']:
        asin_map = pd.read_excel(map_file)
    else:
        asin_map = pd.read_csv(map_file)

    if 'ASIN' not in asin_map.columns or 'Product name' not in asin_map.columns or 'Portfolio' not in asin_map.columns:
        st.error("Mapping file must contain columns: ASIN, Product name, Portfolio")
    else:
        # Merge product and portfolio info
        merged = pd.merge(merged_df, asin_map, on="ASIN", how="inner")
        grouped = merged.groupby("Product name", as_index=False).sum(numeric_only=True)
        portfolio_map = asin_map.drop_duplicates(subset="Product name")[["Product name", "Portfolio"]]
        grouped = pd.merge(grouped, portfolio_map, on="Product name", how="left")

        st.header("📋 Final Grouped Output")
        st.dataframe(grouped.head(20))

        # Step 3: Download Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            grouped.to_excel(writer, index=False, sheet_name='Grouped Summary')

        st.download_button(
            label="⬇️ Download Excel Summary",
            data=output.getvalue(),
            file_name="Product_Summary.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

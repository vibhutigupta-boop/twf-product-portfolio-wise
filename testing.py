import streamlit as st
import pandas as pd
from io import BytesIO
import os
from functools import reduce

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
    try:
        # Read ASIN mapping file
        map_ext = os.path.splitext(map_file.name)[1].lower()
        if map_ext in ['.xlsx', '.xls']:
            asin_map = pd.read_excel(map_file)
        else:
            asin_map = pd.read_csv(map_file)

        if not set(['ASIN', 'Product name', 'Portfolio']).issubset(asin_map.columns):
            st.error("Mapping file must contain columns: ASIN, Product name, Portfolio")
        else:
            # Process all uploaded sales files
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

            if dfs:
                merged_df = reduce(lambda left, right: pd.merge(left, right, on="ASIN", how="outer"), dfs.values())
                merged = pd.merge(merged_df, asin_map, on="ASIN", how="inner")
                grouped = merged.groupby(["Product name", "Portfolio"], as_index=False).sum(numeric_only=True)

                # Select only the required columns
                final_cols = [
                    "Sessions - Total SC", "Ordered Product Sales SC", "Total Order Items SC",
                    "Impressions: Impressions SC", "Clicks: Clicks SC", "Cart Adds: Cart Adds SC",
                    "Ordered Revenue VC", "Ordered Units VC", "Featured Offer Page Views VC"
                ]
                required_columns = final_cols + ["Product name", "Portfolio"]
                missing_cols = [col for col in required_columns if col not in grouped.columns]

                if missing_cols:
                    st.error(f"Missing columns in data for calculations: {missing_cols}")
                else:
                    grouped = grouped[required_columns]

                    # Add derived columns
                    grouped["Total Sessions"] = grouped["Sessions - Total SC"] + grouped["Featured Offer Page Views VC"]
                    grouped["Total Product Sales"] = grouped["Ordered Product Sales SC"] + grouped["Ordered Revenue VC"]
                    grouped["Total Units"] = grouped["Ordered Units VC"] + grouped["Total Order Items SC"]

                    grouped["VC Impressions"] = grouped["Featured Offer Page Views VC"] * (
                        grouped["Impressions: Impressions SC"] / grouped["Sessions - Total SC"].replace(0, 1)
                    )
                    grouped["VC Clicks"] = grouped["Featured Offer Page Views VC"] * (
                        grouped["Clicks: Clicks SC"] / grouped["Sessions - Total SC"].replace(0, 1)
                    )
                    grouped["VC Add to Carts"] = grouped["Featured Offer Page Views VC"] * (
                        grouped["Cart Adds: Cart Adds SC"] / grouped["Sessions - Total SC"].replace(0, 1)
                    )

                    grouped["Total Impressions"] = grouped["VC Impressions"] + grouped["Impressions: Impressions SC"]
                    grouped["Total Clicks"] = grouped["VC Clicks"] + grouped["Clicks: Clicks SC"]
                    grouped["Total Add to Carts"] = grouped["VC Add to Carts"] + grouped["Cart Adds: Cart Adds SC"]

                    # Reorder columns if needed
                    display_cols = final_cols + [
                        "Product name", "Portfolio", "Total Sessions", "Total Product Sales", "Total Units",
                        "VC Impressions", "VC Clicks", "VC Add to Carts",
                        "Total Impressions", "Total Clicks", "Total Add to Carts"
                    ]
                    grouped = grouped[display_cols]

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
            else:
                st.warning("No valid dataframes were processed from uploaded files.")

    except Exception as e:
        st.error(f"❌ Error processing mapping file or merging: {e}")

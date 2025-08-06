# import streamlit as st
# import pandas as pd
# from io import BytesIO
# import os
# from functools import reduce

# st.title("📊 Amazon Data Merger")

# # Step 1: Upload Sales Files
# st.header("Step 1: Upload Amazon Files (Excel or CSV)")
# sales_files = st.file_uploader("Upload multiple reports", type=["xlsx", "xls", "csv"], accept_multiple_files=True)

# #Step 2: Upload ASIN Mapping File
# map_file = r"C:\Users\HP\Yandex.Disk\Amazon automation\Automated\asin_ref_map.csv"

# # Placeholder for dataframes
# dfs = {}

# if sales_files and map_file:
#     try:
#         # Read ASIN mapping file
#         asin_map = pd.read_csv(map_file)

#         if not set(['ASIN', 'Product name', 'Portfolio']).issubset(asin_map.columns):
#             st.error("Mapping file must contain columns: ASIN, Product name, Portfolio")
#         else:
#             # Process all uploaded sales files
#             for file in sales_files:
#                 filename = file.name
#                 ext = os.path.splitext(filename)[1].lower()

#                 try:
#                     if ext in ['.xlsx', '.xls']:
#                         df = pd.read_excel(file)
#                     else:
#                         df = pd.read_csv(file)

#                     if 'ASIN' not in df.columns:
#                         st.warning(f"❌ '{filename}' missing 'ASIN' column. Skipping.")
#                         continue

#                     # Clean and convert all numeric columns
#                     removed_cols = []
#                     for col in df.columns:
#                         if col == "ASIN":
#                             continue
#                         try:
#                             df[col] = pd.to_numeric(df[col], errors='raise')
#                         except:
#                             df[col] = (
#                                 df[col]
#                                 .astype(str)
#                                 .str.replace(r'[^\d\.\-]', '', regex=True)
#                             )
#                             try:
#                                 df[col] = pd.to_numeric(df[col], errors='raise')
#                             except:
#                                 removed_cols.append(col)

#                     if removed_cols:
#                         df = df.drop(columns=removed_cols)

#                     shortname = os.path.splitext(filename)[0][:6]
#                     df_name = f"df_{shortname}"

#                     # Rename columns based on filename pattern
#                     suffix = None
#                     if "Traffi" in df_name or "Sales_" in df_name:
#                         suffix = " VC"
#                     elif "Busine" in df_name or "IN_Sea" in df_name:
#                         suffix = " SC"

#                     if suffix:
#                         df = df.rename(columns={col: col + suffix for col in df.columns if col != "ASIN"})

#                     grouped = df.groupby("ASIN", as_index=False).sum(numeric_only=True)
#                     dfs[df_name] = grouped

#                 except Exception as e:
#                     st.error(f"❌ Error reading '{filename}': {e}")

#             if dfs:
#                 merged_df = reduce(lambda left, right: pd.merge(left, right, on="ASIN", how="outer"), dfs.values())
#                 merged = pd.merge(merged_df, asin_map, on="ASIN", how="inner")
#                 grouped = merged.groupby(["Product name", "Portfolio"], as_index=False).sum(numeric_only=True)

#                 # Select only the required columns
#                 final_cols = [
#                     "Sessions - Total SC", "Ordered Product Sales SC", "Total Order Items SC",
#                     "Impressions: Impressions SC", "Clicks: Clicks SC", "Cart Adds: Cart Adds SC",
#                     "Ordered Revenue VC", "Ordered Units VC", "Featured Offer Page Views VC"
#                 ]
#                 required_columns = final_cols + ["Product name", "Portfolio"]
#                 missing_cols = [col for col in required_columns if col not in grouped.columns]

#                 if missing_cols:
#                     st.error(f"Missing columns in data for calculations: {missing_cols}")
#                 else:
#                     grouped = grouped[required_columns]

#                     # Add derived columns
#                     grouped["Total Sessions"] = grouped["Sessions - Total SC"] + grouped["Featured Offer Page Views VC"]
#                     grouped["Total Product Sales"] = grouped["Ordered Product Sales SC"] + grouped["Ordered Revenue VC"]
#                     grouped["Total Units"] = grouped["Ordered Units VC"] + grouped["Total Order Items SC"]

#                     grouped["VC Impressions"] = grouped["Featured Offer Page Views VC"] * (
#                         grouped["Impressions: Impressions SC"] / grouped["Sessions - Total SC"].replace(0, 1)
#                     )
#                     grouped["VC Clicks"] = grouped["Featured Offer Page Views VC"] * (
#                         grouped["Clicks: Clicks SC"] / grouped["Sessions - Total SC"].replace(0, 1)
#                     )
#                     grouped["VC Add to Carts"] = grouped["Featured Offer Page Views VC"] * (
#                         grouped["Cart Adds: Cart Adds SC"] / grouped["Sessions - Total SC"].replace(0, 1)
#                     )

#                     grouped["Total Impressions"] = grouped["VC Impressions"] + grouped["Impressions: Impressions SC"]
#                     grouped["Total Clicks"] = grouped["VC Clicks"] + grouped["Clicks: Clicks SC"]
#                     grouped["Total Add to Carts"] = grouped["VC Add to Carts"] + grouped["Cart Adds: Cart Adds SC"]

#                     # Reorder columns if needed
#                     display_cols = final_cols + [
#                         "Product name", "Portfolio", "Total Sessions", "Total Product Sales", "Total Units",
#                         "VC Impressions", "VC Clicks", "VC Add to Carts",
#                         "Total Impressions", "Total Clicks", "Total Add to Carts"
#                     ]
#                     grouped = grouped[display_cols]

#                     st.header("📋 Final Grouped Output")
#                     st.dataframe(grouped.head(20))

#                     # Step 3: Download Excel
#                     output = BytesIO()
#                     with pd.ExcelWriter(output, engine='openpyxl') as writer:
#                         grouped.to_excel(writer, index=False, sheet_name='Grouped Summary')

#                     st.download_button(
#                         label="⬇️ Download Excel Summary",
#                         data=output.getvalue(),
#                         file_name="Product_Summary.xlsx",
#                         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#                     )
#             else:
#                 st.warning("No valid dataframes were processed from uploaded files.")

#     except Exception as e:
#         st.error(f"❌ Error processing mapping file or merging: {e}")

import streamlit as st
import pandas as pd
from io import BytesIO
import os
from functools import reduce
from pathlib import Path

st.title("📊 Amazon Data Merger + Ads Metrics")

# -------------------------------------------------------------
# 🔧 CONFIG
# -------------------------------------------------------------
MAP_FILE      = r"C:\Users\HP\Yandex.Disk\Amazon automation\Automated\asin_ref_map.csv"
CAMPAIGN_MAP  = r"C:\Users\HP\Yandex.Disk\Amazon automation\Automated\campaign_product_lookup.csv"
AD_NUM_COLS   = ["Impressions", "Clicks", "Spend",
                 "14 Day Total Orders (#)", "14 Day Total Sales"]
campaign_lkp = pd.read_csv(CAMPAIGN_MAP)

if "pattern" not in campaign_lkp.columns or "product_name" not in campaign_lkp.columns:
    st.error(
        f"'{CAMPAIGN_MAP}' must contain at least columns "
        "'pattern' and 'product_name'.\n"
        f"Current columns: {list(campaign_lkp.columns)}"
    )
    st.stop()

# -------------------------------------------------------------
# STEP 1 – Upload Sales Files
# -------------------------------------------------------------
st.header("Step 1: Upload Amazon Files (Excel or CSV) - ASIN match")
sales_files = st.file_uploader("Upload multiple sales reports",
                               type=["xlsx", "xls", "csv"],
                               accept_multiple_files=True)

# -------------------------------------------------------------
# STEP 2 – Upload Ads Data
# -------------------------------------------------------------
st.header("Step 2: Upload Ads Data (Sponsored Products / Brands) - Campaign match")
ads_files = st.file_uploader("Upload one or more ads reports",
                             type=["xlsx", "xls", "csv"],
                             accept_multiple_files=True,
                             key="ads")

# -------------------------------------------------------------
# LOAD BACK-END MAPPINGS
# -------------------------------------------------------------
try:
    asin_map      = pd.read_csv(MAP_FILE)
    campaign_lkp  = pd.read_csv(CAMPAIGN_MAP)   # columns: pattern, product_name
except FileNotFoundError as e:
    st.error(f"❌ Mapping file missing: {e}")
    st.stop()

required_asin_cols = {"ASIN", "Product name", "Portfolio"}
if not required_asin_cols.issubset(asin_map.columns):
    st.error(f"ASIN mapping must have {required_asin_cols}")
    st.stop()

# Tiny helper: match campaign name → product
def match_product(camp_name: str) -> str | None:
    low = camp_name.lower()
    hit = campaign_lkp[campaign_lkp["pattern"].apply(lambda p: p in low)]
    return hit["product_name"].iloc[0] if not hit.empty else None

# -------------------------------------------------------------
# RUN WHEN BOTH INPUTS ARE PRESENT
# -------------------------------------------------------------
if sales_files and ads_files:
    ### --------------------------------------------------------
    ### PART A – Sales processing  (unchanged from your script)
    ### --------------------------------------------------------
    dfs = {}   # same as before
    for file in sales_files:
        filename = file.name
        ext = os.path.splitext(filename)[1].lower()
        try:
            df = pd.read_excel(file) if ext in [".xlsx", ".xls"] else pd.read_csv(file)

            if "ASIN" not in df.columns:
                st.warning(f"'{filename}' missing ASIN – skipped"); continue

            # numeric cleanup (unchanged) …
            removed_cols = []
            for c in df.columns:
                if c == "ASIN": continue
                try:
                    df[c] = pd.to_numeric(df[c], errors="raise")
                except:
                    df[c] = (df[c].astype(str)
                                   .str.replace(r"[^\d\.\-]", "", regex=True))
                    try:
                        df[c] = pd.to_numeric(df[c], errors="raise")
                    except: removed_cols.append(c)
            if removed_cols: df = df.drop(columns=removed_cols)

            # rename metrics by suffix (unchanged) …
            short = Path(filename).stem[:6]
            suffix = None
            if "Traffi" in short or "Sales_" in short:           suffix = " VC"
            elif "Busine" in short or "IN_Sea" in short:         suffix = " SC"
            if suffix:
                df = df.rename({c: c + suffix for c in df.columns if c != "ASIN"},
                               axis=1)

            dfs[short] = df.groupby("ASIN", as_index=False).sum(numeric_only=True)
        except Exception as e:
            st.error(f"Error reading {filename}: {e}")

    if not dfs:
        st.warning("No valid sales data processed."); st.stop()

    merged_sales = reduce(
        lambda L, R: pd.merge(L, R, on="ASIN", how="outer"), dfs.values())
    merged_sales = merged_sales.merge(asin_map, on="ASIN", how="inner")
    sales_by_prod = (merged_sales
                     .groupby(["Product name", "Portfolio"], as_index=False)
                     .sum(numeric_only=True))
    # helper to avoid KeyErrors when a base column is absent
    def safe_get(df, col):
        return df[col] if col in df.columns else 0

    sales_by_prod["Total Sessions"] = (
        safe_get(sales_by_prod, "Sessions - Total SC") +
        safe_get(sales_by_prod, "Featured Offer Page Views VC")
    )

    sales_by_prod["Total Product Sales"] = (
        safe_get(sales_by_prod, "Ordered Product Sales SC") +
        safe_get(sales_by_prod, "Ordered Revenue VC")
    )

    sales_by_prod["Total Purchases"] = (
        safe_get(sales_by_prod, "Total Order Items SC") +
        safe_get(sales_by_prod, "Ordered Units VC")
    )

    # Avoid divide-by-zero
    base_sessions = safe_get(sales_by_prod, "Sessions - Total SC").replace(0, 1)

    sales_by_prod["VC Impressions"] = (
        safe_get(sales_by_prod, "Featured Offer Page Views VC") *
        safe_get(sales_by_prod, "Impressions: Impressions SC") / base_sessions
    )

    sales_by_prod["VC Clicks"] = (
        safe_get(sales_by_prod, "Featured Offer Page Views VC") *
        safe_get(sales_by_prod, "Clicks: Clicks SC") / base_sessions
    )

    sales_by_prod["VC Add to Carts"] = (
        safe_get(sales_by_prod, "Featured Offer Page Views VC") *
        safe_get(sales_by_prod, "Cart Adds: Cart Adds SC") / base_sessions
    )

    sales_by_prod["Total Impressions"] = (
        sales_by_prod["VC Impressions"] +
        safe_get(sales_by_prod, "Impressions: Impressions SC")
    )
    sales_by_prod["Total Clicks"] = (
        sales_by_prod["VC Clicks"] +
        safe_get(sales_by_prod, "Clicks: Clicks SC")
    )
    sales_by_prod["Total Add to Carts"] = (
        sales_by_prod["VC Add to Carts"] +
        safe_get(sales_by_prod, "Cart Adds: Cart Adds SC")
    )

    final_sales_cols = [
    "Product name", "Portfolio",                        # identifiers
    # ---- original SC / VC metrics kept ----
    # "Sessions - Total SC", "Ordered Product Sales SC", "Total Order Items SC",
    # "Impressions: Impressions SC", "Clicks: Clicks SC", "Cart Adds: Cart Adds SC",
    # "Ordered Revenue VC", "Ordered Units VC", "Featured Offer Page Views VC",
    # ---- derived columns ----
    "Total Sessions", "Total Product Sales", "Total Purchases",
    # "VC Impressions", "VC Clicks", "VC Add to Carts",
    "Total Impressions", "Total Clicks", "Total Add to Carts",
    ]

    sales_by_prod = sales_by_prod[final_sales_cols] 

    ### --------------------------------------------------------
    ### PART B – Ads processing
    ### --------------------------------------------------------
    ad_frames = []
    for file in ads_files:
        name, ext = file.name, os.path.splitext(file.name)[1].lower()
        try:
            ad_df = pd.read_excel(file) if ext in [".xlsx", ".xls"] else pd.read_csv(file)
        except Exception as e:
            st.error(f"Error reading ads file '{name}': {e}"); continue

        # Normalise column names in case of spaces / case differences
        ad_df.columns = ad_df.columns.str.strip()

        # Find the campaign name column
        camp_col = None
        for c in ["Campaign Name", "Campaign", "Campaign Name"]:  # tweak if needed
            if c in ad_df.columns:
                camp_col = c; break
        if not camp_col:
            st.warning(f"'{name}' has no Campaign Name column – skipped"); continue

        # Map campaign → product
        ad_df["Product name"] = ad_df[camp_col].apply(match_product)
        ad_df = ad_df.dropna(subset=["Product name"])

        # Sum metrics (impressions, clicks, etc.) per product in **this file**
        for col in AD_NUM_COLS:
            if col not in ad_df.columns: ad_df[col] = 0  # fill missing

        gp = (ad_df
              .groupby("Product name", as_index=False)[AD_NUM_COLS]
              .sum(numeric_only=True))
        ad_frames.append(gp)

    if not ad_frames:
        st.error("No valid ads data processed."); st.stop()

    for col in AD_NUM_COLS:
        if col not in ad_df.columns:
            ad_df[col] = 0
        ad_df[col] = pd.to_numeric(ad_df[col], errors="coerce").fillna(0)

    ads_by_prod = (
    pd.concat(ad_frames, ignore_index=True)
      .groupby("Product name", as_index=False)[AD_NUM_COLS]
      .sum(numeric_only=True)
    )
     
    # 🅰️  Rename ads metrics → "Inorganic …", then tidy two labels
    ads_by_prod = ads_by_prod.rename(
        columns={c: f"Inorganic {c}" for c in AD_NUM_COLS}
    ).rename(
        columns={
            "Inorganic 14 Day Total Orders (#)": "Inorganic Purchases",
            "Inorganic 14 Day Total Sales":      "Inorganic Sales",
        }
    )
    ### --------------------------------------------------------
    ### PART C – Combine sales + ads summary
    ### --------------------------------------------------------
    

    final = sales_by_prod.merge(ads_by_prod, on="Product name", how="outer")

    # final = sales_by_prod.merge(ads_by_prod, on="Product name", how="outer")
    # final.fillna(0, inplace=True)
    
    # bring Portfolio back if it went missing (ads-only rows)
    if "Portfolio" not in final.columns:
        final["Portfolio"] = ""

    def safe(col):
        return final[col] if col in final.columns else 0

    final["Organic Impressions"] = safe("Total Impressions") - safe("Inorganic Impressions")
    final["Organic Clicks"]      = safe("Total Clicks")      - safe("Inorganic Clicks")
    final["Organic Purcahses"]       = safe("Total Purchases")       - safe("Inorganic Purchases")
    final["Organic Sales"]       = safe("Total Product Sales") - safe("Inorganic Sales")

    ads_cols_inorg = [
    "Inorganic Impressions", "Inorganic Clicks", "Inorganic Spend",
    "Inorganic Purchases", "Inorganic Sales",
    ]


    organic_cols = [
        "Organic Impressions", "Organic Clicks",
        "Organic Purchases", "Organic Sales"
    ]

    ordered_final_cols = (
        final_sales_cols          # all the sales & derived columns you already picked
        + ads_cols_inorg          # inorganic metrics
        + organic_cols            # new organic metrics
    )


    # Columns that might be missing because a metric wasn't present in any file:
    for col in ordered_final_cols:
        if col not in final.columns:
            final[col] = 0

    final = final[ordered_final_cols]
    # ---------------------------------------------------------
    # Display + download
    # ---------------------------------------------------------
    st.subheader("📋 Sales Summary by Product")
    st.dataframe(sales_by_prod.head(20), use_container_width=True)

    st.subheader("📋 Ads Metrics by Product")
    st.dataframe(ads_by_prod.head(20), use_container_width=True)

    st.subheader("📋 Combined Summary")
    st.dataframe(final.head(20), use_container_width=True)


    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        sales_by_prod.to_excel(writer, index=False, sheet_name="Sales")
        ads_by_prod.to_excel(writer, index=False, sheet_name="Ads")
        final.to_excel(writer, index=False, sheet_name="Combined")
    st.download_button("⬇️ Download Excel",
                       buffer.getvalue(),
                       "Amazon_Sales+Ads_Summary.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
else:
    st.info("👉 Upload both Sales files and Ads files to continue.")

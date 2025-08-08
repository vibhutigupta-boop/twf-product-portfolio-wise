import re                    
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
from pathlib import Path

BASE_DIR = Path(__file__).parent      
MAP_FILE = BASE_DIR / "data" / "asin_ref_map.csv"
CAMPAIGN_MAP = BASE_DIR / "data" / "campaign_product_lookup.csv"

# MAP_FILE      = r"C:\Users\HP\Yandex.Disk\Amazon automation\Automated\asin_ref_map.csv"
# CAMPAIGN_MAP  = r"C:\Users\HP\Yandex.Disk\Amazon automation\Automated\campaign_product_lookup.csv"
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
    asin_map     = pd.read_csv(MAP_FILE)
    campaign_lkp = pd.read_csv(CAMPAIGN_MAP)  # must have pattern, product_name
except FileNotFoundError as e:
    st.error(f"❌ Mapping file missing: {e}")
    st.stop()

if {"pattern", "product_name"} - set(campaign_lkp.columns):
    st.error(f"'{CAMPAIGN_MAP}' needs columns: pattern, product_name")
    st.stop()

if {"ASIN", "Product name", "Portfolio"} - set(asin_map.columns):
    st.error("ASIN mapping must include: ASIN, Product name, Portfolio")
    st.stop()

def _norm(text: str) -> str:
    """
    Lower-cases, swaps  _, -, multiple spaces  ➜ single space, strips ends.
    Example: 'SP_Almond-Flour  ' → 'sp almond flour'
    """
    return re.sub(r'[_\-\s]+', ' ', str(text).lower()).strip()

# Pre-compute a normalised version of every pattern once ▶ speed-up
campaign_lkp["norm_pattern"] = campaign_lkp["pattern"].fillna("").map(_norm)

def match_product(camp_name: str) -> str | None:
    """
    Return the *longest* pattern that appears in the normalised campaign name.
    If nothing matches ➜ None.
    """
    camp_norm = _norm(camp_name)
    if not camp_norm:
        return None

    # Boolean mask: pattern contained in campaign name
    mask = campaign_lkp["norm_pattern"].apply(lambda p: p and p in camp_norm)
    if not mask.any():
        return None

    # Prefer the most specific (longest) pattern if several match
    hit = (campaign_lkp.loc[mask]
                       .assign(p_len=lambda d: d["norm_pattern"].str.len())
                       .sort_values("p_len", ascending=False)
                       .iloc[0])
    return hit["product_name"]

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
    final["Organic Purchases"]   = safe("Total Purchases")       - safe("Inorganic Purchases")
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

    # after you build `final`
    num_cols = final.select_dtypes("number").columns        # only numeric
    final[num_cols] = final[num_cols].clip(lower=0)        # no negatives

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



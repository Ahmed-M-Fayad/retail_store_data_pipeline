"""
DATA TRANSFORMATION PIPELINE
Purpose: Clean and transform data based on configuration
Output: Cleaned CSV files ready for loading
"""

import pandas as pd
import numpy as np
import yaml
import os
import warnings

warnings.filterwarnings("ignore")

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_SOURCE_PATH = "DataSources/"
OUTPUT_PATH = "cleaned_data/"
CONFIG_FILE = "pipeline_config.yaml"

print("=" * 80)
print("DATA TRANSFORMATION PIPELINE")
print("=" * 80)


# ============================================================================
# LOAD CONFIGURATION
# ============================================================================


def load_config():
    """Load pipeline configuration"""
    print("\n" + "=" * 80)
    print("LOADING CONFIGURATION")
    print("=" * 80)

    if not os.path.exists(CONFIG_FILE):
        print(f"\n⚠️  Configuration file '{CONFIG_FILE}' not found!")
        print("   Proceeding with full transformation...\n")
        return None

    with open(CONFIG_FILE, "r") as f:
        config = yaml.safe_load(f)

    print(f"\n✓ Configuration loaded")
    print(f"  Quality score: {config['overall_quality_score']:.2f}%")
    print(f"  Check time: {config['metadata']['check_timestamp']}")

    print("\nTransformation Steps:")
    for step, needed in config["pipeline_steps"].items():
        if step != "data_cleaning" and step != "data_transformation":
            continue
        status = "✓ EXECUTE" if needed else "⊗ SKIP"
        print(f"  {step:30} : {status}")

    return config


# ============================================================================
# DATA LOADING
# ============================================================================


def load_data():
    """Load all CSV files from source directory"""
    print("\n" + "=" * 80)
    print("LOADING SOURCE DATA")
    print("=" * 80)

    files = {
        "brands": "brands.csv",
        "categories": "categories.csv",
        "products": "products.csv",
        "customers": "customers.csv",
        "orders": "orders.csv",
        "order_items": "order_items.csv",
        "staffs": "staffs.csv",
        "stores": "stores.csv",
        "stocks": "stocks.csv",
    }

    dfs = {}
    for name, file in files.items():
        try:
            df = pd.read_csv(f"{DATA_SOURCE_PATH}{file}", encoding="utf-8")
            dfs[name] = df
            print(f"✓ {name:15} : {df.shape[0]:6} rows × {df.shape[1]:3} columns")
        except UnicodeDecodeError:
            df = pd.read_csv(f"{DATA_SOURCE_PATH}{file}", encoding="latin-1")
            dfs[name] = df
            print(
                f"✓ {name:15} : {df.shape[0]:6} rows × {df.shape[1]:3} columns (latin-1)"
            )
        except FileNotFoundError:
            print(f"✗ {name:15} : FILE NOT FOUND")

    print(f"\n📊 Total datasets loaded: {len(dfs)}")
    return dfs


# ============================================================================
# COLUMN STANDARDIZATION
# ============================================================================


def standardize_columns(dfs, config):
    """Standardize column names: lowercase and underscores"""

    if config and not config["pipeline_steps"].get("column_standardization", True):
        print("\n⊗ SKIPPING: Column Standardization (already clean)")
        return dfs

    print("\n" + "=" * 80)
    print("STEP 1: COLUMN STANDARDIZATION")
    print("=" * 80)

    changes_made = False
    for name, df in dfs.items():
        original_cols = list(df.columns)
        df.columns = df.columns.str.lower().str.replace(" ", "_")
        new_cols = list(df.columns)

        if original_cols != new_cols:
            print(f"✓ {name:15} : Standardized")
            changes_made = True
        else:
            print(f"  {name:15} : Already clean")

    if not changes_made:
        print("\n✓ All columns already standardized")

    return dfs


# ============================================================================
# DATA CLEANING FUNCTIONS
# ============================================================================


def clean_brands(df, needs_cleaning):
    """Clean brands dataset"""
    if not needs_cleaning:
        return df, "⊗ Skipped (clean)"

    before = len(df)
    df = df.copy()
    df = df.drop_duplicates()
    df["brand_id"] = df["brand_id"].astype(int)
    df["brand_name"] = df["brand_name"].fillna("Unknown")
    after = len(df)

    return df, f"✓ Cleaned: {before} → {after} rows"


def clean_categories(df, needs_cleaning):
    """Clean categories dataset"""
    if not needs_cleaning:
        return df, "⊗ Skipped (clean)"

    before = len(df)
    df = df.copy()
    df = df.drop_duplicates()
    df["category_id"] = df["category_id"].astype(int)
    df["category_name"] = df["category_name"].fillna("Unknown")
    after = len(df)

    return df, f"✓ Cleaned: {before} → {after} rows"


def clean_products(df, needs_cleaning):
    """Clean products dataset"""
    if not needs_cleaning:
        return df, "⊗ Skipped (clean)"

    before = len(df)
    df = df.copy()

    df = df.drop_duplicates()
    df = df.dropna(subset=["product_id", "product_name"])
    df["model_year"] = df["model_year"].fillna(0).astype(int)
    df["product_id"] = df["product_id"].astype(int)
    df["category_id"] = df["category_id"].astype(int)
    df["brand_id"] = df["brand_id"].astype(int)
    df["list_price"] = df["list_price"].astype(float)
    df = df[df["list_price"] >= 0]

    after = len(df)
    return df, f"✓ Cleaned: {before} → {after} rows"


def clean_customers(df, needs_cleaning):
    """Clean customers dataset"""
    if not needs_cleaning:
        return df, "⊗ Skipped (clean)"

    before = len(df)
    df = df.copy()

    df = df.drop_duplicates()
    df = df.dropna(subset=["customer_id"])
    df["first_name"] = df["first_name"].fillna("Unknown")
    df["last_name"] = df["last_name"].fillna("Unknown")
    df["email"] = df["email"].fillna("unknown@email.com")
    df["phone"] = df["phone"].fillna("Unknown")
    df["street"] = df["street"].fillna("Unknown")
    df["city"] = df["city"].fillna("Unknown")
    df["state"] = df["state"].fillna("Unknown")
    df["zip_code"] = df["zip_code"].fillna("00000")
    df["customer_id"] = df["customer_id"].astype(int)

    if "full_name" not in df.columns:
        df["full_name"] = df["first_name"] + " " + df["last_name"]

    df["phone"] = (
        df["phone"]
        .astype(str)
        .apply(lambda x: x.split(",")[0].strip() if "," in x else x)
    )
    df["phone"] = df["phone"].str.replace(r"\D", "", regex=True)

    after = len(df)
    return df, f"✓ Cleaned: {before} → {after} rows"


def clean_orders(df, needs_cleaning):
    """Clean orders dataset with safe date parsing and auto-correction of malformed years"""
    if not needs_cleaning:
        return df, "⊗ Skipped (clean)"

    before = len(df)
    df = df.copy()

    df = df.drop_duplicates()
    df = df.dropna(subset=["order_id", "customer_id"])
    df["order_id"] = df["order_id"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["order_status"] = df["order_status"].astype(int)
    df["store_id"] = df["store_id"].astype(int)
    df["staff_id"] = df["staff_id"].astype(int)

    # ----------------------------
    # Implicit date correction
    # ----------------------------
    def parse_and_fix_date(date_str):
        if pd.isna(date_str):
            return pd.NaT
        try:
            dt = pd.to_datetime(date_str, errors="coerce")
            if pd.isna(dt):
                return pd.NaT
            # Auto-correct years clearly out of range
            if dt.year < 1900:
                # Example: 1016 → 2016, 1610 → 2016, etc.
                corrected_year = dt.year + 1000 * ((2025 - dt.year) // 1000 + 1)
                dt = dt.replace(year=corrected_year)
            return dt
        except Exception:
            return pd.NaT

    for col in ["order_date", "required_date", "shipped_date"]:
        df[col] = df[col].apply(parse_and_fix_date)

    # Optional: report any remaining invalid dates
    invalid_dates = df[df[["order_date", "required_date"]].isna().any(axis=1)]
    if len(invalid_dates) > 0:
        print(
            f"⚠️  {len(invalid_dates)} orders contain invalid or NULL dates after correction"
        )

    after = len(df)
    return df, f"✓ Cleaned: {before} → {after} rows"


def clean_order_items(df, needs_cleaning):
    """Clean order_items dataset with implicit safe numeric conversion"""
    if not needs_cleaning:
        return df, "⊗ Skipped (clean)"

    before = len(df)
    df = df.copy()

    # Drop duplicates
    df = df.drop_duplicates()

    # Drop rows missing critical columns
    df = df.dropna(subset=["order_id", "product_id", "item_id"])

    # ----------------------------
    # Safe integer conversion
    # ----------------------------
    for col in ["order_id", "item_id", "product_id", "quantity"]:
        mask_valid = pd.to_numeric(df[col], errors='coerce').notna()
        dropped = len(df) - mask_valid.sum()
        if dropped > 0:
            print(f"⚠️  Dropped {dropped} rows due to invalid '{col}'")
        df = df[mask_valid].copy()
        df[col] = df[col].astype(int)

    # Safe float conversion
    for col in ["list_price", "discount"]:
        mask_valid = pd.to_numeric(df[col], errors='coerce').notna()
        dropped = len(df) - mask_valid.sum()
        if dropped > 0:
            print(f"⚠️  Dropped {dropped} rows due to invalid '{col}'")
        df = df[mask_valid].copy()
        df[col] = df[col].astype(float)

    # Filter invalid quantities
    df = df[df["quantity"] > 0]

    after = len(df)
    return df, f"✓ Cleaned: {before} → {after} rows"


def clean_staffs(df, needs_cleaning):
    """Clean staffs dataset with implicit safe numeric conversion"""
    if not needs_cleaning:
        return df, "⊗ Skipped (clean)"

    before = len(df)
    df = df.copy()

    # Drop duplicates
    df = df.drop_duplicates()

    # Drop rows missing critical ID
    df = df.dropna(subset=["staff_id"])

    # Fill missing text fields
    df["first_name"] = df["first_name"].fillna("Unknown")
    df["last_name"] = df["last_name"].fillna("Unknown")
    df["email"] = df["email"].fillna("unknown@email.com")
    df["phone"] = df["phone"].fillna("Unknown")

    # ----------------------------
    # Safe integer conversion
    # ----------------------------
    int_columns = ["staff_id", "store_id", "active", "manager_id"]
    for col in int_columns:
        # Treat missing or non-finite values
        if col == "manager_id":
            # Fill NaN with 0 before conversion
            df[col] = df[col].fillna(0)
        mask_valid = pd.to_numeric(df[col], errors='coerce').notna() & np.isfinite(df[col])
        dropped = len(df) - mask_valid.sum()
        if dropped > 0:
            print(f"⚠️  Dropped {dropped} rows due to invalid/non-finite '{col}'")
        df = df[mask_valid].copy()
        df[col] = df[col].astype(int)

    # Clean phone numbers
    df["phone"] = df["phone"].astype(str).str.replace(r"\D", "", regex=True)

    after = len(df)
    return df, f"✓ Cleaned: {before} → {after} rows"


def clean_stores(df, needs_cleaning):
    """Clean stores dataset"""
    if not needs_cleaning:
        return df, "⊗ Skipped (clean)"

    before = len(df)
    df = df.copy()

    df = df.drop_duplicates()
    df = df.dropna(subset=["store_id"])
    df["store_name"] = df["store_name"].fillna("Unknown")
    df["phone"] = df["phone"].fillna("Unknown")
    df["email"] = df["email"].fillna("unknown@email.com")
    df["street"] = df["street"].fillna("Unknown")
    df["city"] = df["city"].fillna("Unknown")
    df["state"] = df["state"].fillna("Unknown")
    df["zip_code"] = df["zip_code"].fillna("00000")
    df["store_id"] = df["store_id"].astype(int)
    df["phone"] = df["phone"].astype(str).str.replace(r"\D", "", regex=True)

    after = len(df)
    return df, f"✓ Cleaned: {before} → {after} rows"


def clean_stocks(df, needs_cleaning):
    """Clean stocks dataset"""
    if not needs_cleaning:
        return df, "⊗ Skipped (clean)"

    before = len(df)
    df = df.copy()

    df = df.drop_duplicates()
    df = df.dropna(subset=["store_id", "product_id"])
    df["store_id"] = df["store_id"].astype(int)
    df["product_id"] = df["product_id"].astype(int)
    df["quantity"] = df["quantity"].fillna(0).astype(int)
    df = df[df["quantity"] >= 0]

    after = len(df)
    return df, f"✓ Cleaned: {before} → {after} rows"


def clean_all_data(dfs, config):
    """Apply cleaning to datasets based on configuration"""

    if config and not config["pipeline_steps"].get("data_cleaning", True):
        print("\n⊗ SKIPPING: Data Cleaning (no issues detected)")
        return dfs

    print("\n" + "=" * 80)
    print("STEP 2: DATA CLEANING")
    print("=" * 80)

    # Determine which datasets need cleaning
    needs_cleaning = {}
    if config:
        for name, dataset_config in config["datasets"].items():
            checks = dataset_config["checks"]
            needs_cleaning[name] = (
                checks["duplicates"]["needed"]
                or checks["missing_values"]["needed"]
                or checks["data_types"]["needed"]
                or checks.get("quality", {}).get("needed", False)
            )
    else:
        needs_cleaning = {name: True for name in dfs.keys()}

    cleaned = {}

    print("\nCleaning datasets:")

    df, msg = clean_brands(dfs["brands"], needs_cleaning.get("brands", True))
    cleaned["brands"] = df
    print(f"  brands         : {msg}")

    df, msg = clean_categories(
        dfs["categories"], needs_cleaning.get("categories", True)
    )
    cleaned["categories"] = df
    print(f"  categories     : {msg}")

    df, msg = clean_products(dfs["products"], needs_cleaning.get("products", True))
    cleaned["products"] = df
    print(f"  products       : {msg}")

    df, msg = clean_customers(dfs["customers"], needs_cleaning.get("customers", True))
    cleaned["customers"] = df
    print(f"  customers      : {msg}")

    df, msg = clean_orders(dfs["orders"], needs_cleaning.get("orders", True))
    cleaned["orders"] = df
    print(f"  orders         : {msg}")

    df, msg = clean_order_items(
        dfs["order_items"], needs_cleaning.get("order_items", True)
    )
    cleaned["order_items"] = df
    print(f"  order_items    : {msg}")

    df, msg = clean_staffs(dfs["staffs"], needs_cleaning.get("staffs", True))
    cleaned["staffs"] = df
    print(f"  staffs         : {msg}")

    df, msg = clean_stores(dfs["stores"], needs_cleaning.get("stores", True))
    cleaned["stores"] = df
    print(f"  stores         : {msg}")

    df, msg = clean_stocks(dfs["stocks"], needs_cleaning.get("stocks", True))
    cleaned["stocks"] = df
    print(f"  stocks         : {msg}")

    print("\n✓ Cleaning complete")
    return cleaned


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================


def transform_data(dfs, config):
    """Apply business transformations"""

    if config and not config["pipeline_steps"].get("data_transformation", True):
        print("\n⊗ SKIPPING: Data Transformation (already complete)")
        return dfs

    print("\n" + "=" * 80)
    print("STEP 3: DATA TRANSFORMATION")
    print("=" * 80)

    transformations = config.get("transformations", {}) if config else {}

    # Enrich products with brand and category names
    if transformations.get("enrich_products", {}).get("needed", True):
        print("\n1. Enriching products with brand/category names...")
        products = (
            dfs["products"]
            .merge(
                dfs["brands"][["brand_id", "brand_name"]],
                on="brand_id",
                how="left",
                suffixes=("", "_merge"),
            )
            .merge(
                dfs["categories"][["category_id", "category_name"]],
                on="category_id",
                how="left",
                suffixes=("", "_merge"),
            )
        )

        # Handle column conflicts
        if "brand_name_merge" in products.columns:
            if (
                "brand_name" not in products.columns
                or products["brand_name"].isna().all()
            ):
                products["brand_name"] = products["brand_name_merge"]
            products = products.drop("brand_name_merge", axis=1)

        if "category_name_merge" in products.columns:
            if (
                "category_name" not in products.columns
                or products["category_name"].isna().all()
            ):
                products["category_name"] = products["category_name_merge"]
            products = products.drop("category_name_merge", axis=1)

        dfs["products"] = products
        print(f"   ✓ Products enriched: {products.shape}")
    else:
        print("\n1. ⊗ Products already enriched")

    # Calculate total_price in order_items
    if transformations.get("calculate_item_total", {}).get("needed", True):
        print("\n2. Calculating item total prices...")
        order_items = dfs["order_items"].copy()
        if "total_price" not in order_items.columns:
            order_items["total_price"] = (
                order_items["quantity"]
                * order_items["list_price"]
                * (1 - order_items["discount"])
            )
            dfs["order_items"] = order_items
        print(f"   ✓ Total revenue: ${order_items['total_price'].sum():,.2f}")
    else:
        print("\n2. ⊗ Item totals already calculated")

    # Calculate order total amount
    if transformations.get("calculate_order_total", {}).get("needed", True):
        print("\n3. Calculating order totals...")
        order_totals = (
            dfs["order_items"].groupby("order_id")["total_price"].sum().reset_index()
        )
        order_totals.columns = ["order_id", "order_total"]

        orders = dfs["orders"].merge(
            order_totals, on="order_id", how="left", suffixes=("", "_new")
        )

        if "order_total_new" in orders.columns:
            if (
                "order_total" not in orders.columns
                or orders["order_total"].isna().all()
            ):
                orders["order_total"] = orders["order_total_new"]
            orders = orders.drop("order_total_new", axis=1)

        orders["order_total"] = orders["order_total"].fillna(0)
        dfs["orders"] = orders
        print(f"   ✓ Average order value: ${orders['order_total'].mean():,.2f}")
    else:
        print("\n3. ⊗ Order totals already calculated")

    print("\n✓ Transformation complete")
    return dfs


# ============================================================================
# SAVE CLEANED DATA
# ============================================================================


def save_cleaned_data(dfs):
    """Save cleaned and transformed data to CSV"""
    print("\n" + "=" * 80)
    print("SAVING CLEANED DATA")
    print("=" * 80)

    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
        print(f"✓ Created output directory: {OUTPUT_PATH}")

    print("\nSaving files:")
    for name, df in dfs.items():
        filename = f"{OUTPUT_PATH}cleaned_{name}.csv"
        df.to_csv(filename, index=False)
        print(f"  ✓ cleaned_{name}.csv ({len(df):,} rows)")

    print(f"\n✓ All files saved to '{OUTPUT_PATH}'")
    return True


# ============================================================================
# MAIN EXECUTION
# ============================================================================


def main():
    """Execute transformation pipeline"""

    print("\n🔄 Starting Transformation Pipeline...\n")

    # Load configuration
    config = load_config()

    # Load source data
    dfs = load_data()

    # Execute transformation steps
    dfs = standardize_columns(dfs, config)
    dfs = clean_all_data(dfs, config)
    dfs = transform_data(dfs, config)

    # Save cleaned data
    success = save_cleaned_data(dfs)

    # Summary
    print("\n" + "=" * 80)
    print("TRANSFORMATION SUMMARY")
    print("=" * 80)

    if config:
        print("\nSteps Executed:")
        steps = ["column_standardization", "data_cleaning", "data_transformation"]
        for step in steps:
            if step in config["pipeline_steps"]:
                needed = config["pipeline_steps"][step]
                status = "✓ EXECUTED" if needed else "⊗ SKIPPED"
                print(f"  {step:30} : {status}")

    print("\n📊 Output Statistics:")
    for name, df in dfs.items():
        print(f"  {name:15} : {len(df):,} rows")

    print("\n" + "=" * 80)
    print("✓ TRANSFORMATION COMPLETE!")
    print("=" * 80)
    print(f"\nCleaned data saved to: {OUTPUT_PATH}")
    print("Ready for SQL Server loading")
    print("=" * 80)

    return success


if __name__ == "__main__":
    main()

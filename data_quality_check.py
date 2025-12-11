"""
DATA QUALITY CHECK SCRIPT
Purpose: Analyze data and return yaml config for which pipeline steps to execute
Returns a configuration that triggers only necessary transformations
"""

import pandas as pd
import numpy as np
import yaml
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_SOURCE_PATH = "DataSources/"

# ============================================================================
# CHECK FUNCTIONS
# ============================================================================


def check_column_standardization(df):
    """Check if columns need standardization (lowercase, underscores)"""
    needs_standardization = False
    issues = []

    for col in df.columns:
        # Check for uppercase
        if col != col.lower():
            needs_standardization = True
            issues.append(f"Uppercase: '{col}'")

        # Check for spaces
        if " " in col:
            needs_standardization = True
            issues.append(f"Spaces: '{col}'")

    return {
        "needed": needs_standardization,
        "issues": issues[:5],  # Limit to 5 examples
    }


def check_duplicates(df):
    """Check for duplicate rows"""
    dup_count = df.duplicated().sum()
    dup_pct = (dup_count / len(df) * 100) if len(df) > 0 else 0

    return {
        "needed": dup_count > 0,
        "count": int(dup_count),
        "percentage": round(dup_pct, 2),
    }


def check_missing_values(df, table_name):
    """Check for missing values that need handling"""
    missing_info = {}
    needs_handling = False

    for col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count > 0:
            needs_handling = True
            missing_pct = missing_count / len(df) * 100
            missing_info[col] = {
                "count": int(missing_count),
                "percentage": round(missing_pct, 2),
            }

    return {"needed": needs_handling, "columns": missing_info}


def check_data_types(df, table_name):
    """Check if data types need conversion"""
    type_issues = {}
    needs_conversion = False

    for col in df.columns:
        col_lower = col.lower()
        current_type = str(df[col].dtype)
        expected_type = None

        # ID columns should be int
        if col_lower.endswith("id"):
            if current_type not in ["int64", "int32"]:
                expected_type = "int"
                needs_conversion = True

        # Price columns should be numeric
        elif "price" in col_lower:
            if current_type not in ["float64", "float32", "int64", "int32"]:
                expected_type = "float"
                needs_conversion = True

        # Date columns should be datetime
        elif "date" in col_lower:
            if not current_type.startswith("datetime"):
                expected_type = "datetime"
                needs_conversion = True

        if expected_type:
            type_issues[col] = {"current": current_type, "expected": expected_type}

    return {"needed": needs_conversion, "issues": type_issues}


def check_products_quality(df):
    """Check product-specific data quality issues"""
    issues = {}
    needs_cleaning = False

    if "list_price" in df.columns:
        neg_prices = (df["list_price"] < 0).sum()
        if neg_prices > 0:
            needs_cleaning = True
            issues["negative_prices"] = int(neg_prices)

    return {"needed": needs_cleaning, "issues": issues}


def check_order_items_quality(df):
    """Check order_items-specific data quality issues"""
    issues = {}
    needs_cleaning = False

    if "quantity" in df.columns:
        neg_qty = (df["quantity"] < 0).sum()
        zero_qty = (df["quantity"] == 0).sum()
        if neg_qty > 0 or zero_qty > 0:
            needs_cleaning = True
            issues["invalid_quantities"] = {
                "negative": int(neg_qty),
                "zero": int(zero_qty),
            }

    if "discount" in df.columns:
        invalid_disc = ((df["discount"] < 0) | (df["discount"] > 1)).sum()
        if invalid_disc > 0:
            needs_cleaning = True
            issues["invalid_discounts"] = int(invalid_disc)

    return {"needed": needs_cleaning, "issues": issues}


def check_customers_quality(df):
    """Check customer-specific data quality issues"""
    issues = {}
    needs_cleaning = False

    if "phone" in df.columns:
        non_null_phones = df["phone"].dropna()
        multi_phones = non_null_phones[
            non_null_phones.astype(str).str.contains(",", na=False)
        ]
        if len(multi_phones) > 0:
            needs_cleaning = True
            issues["multiple_phones"] = int(len(multi_phones))

    # Check if full_name column exists
    if (
        "full_name" not in df.columns
        and "first_name" in df.columns
        and "last_name" in df.columns
    ):
        needs_cleaning = True
        issues["missing_full_name_column"] = True

    return {"needed": needs_cleaning, "issues": issues}


def check_transformations_needed(dfs):
    """Check if transformations like merges and calculations are needed"""
    transformations = {}

    # Check if products need brand/category names
    if "products" in dfs:
        products = dfs["products"]
        has_brand_name = "brand_name" in products.columns
        has_category_name = "category_name" in products.columns

        transformations["enrich_products"] = {
            "needed": not (has_brand_name and has_category_name),
            "missing_columns": [],
        }

        if not has_brand_name:
            transformations["enrich_products"]["missing_columns"].append("brand_name")
        if not has_category_name:
            transformations["enrich_products"]["missing_columns"].append(
                "category_name"
            )

    # Check if order_items need total_price calculation
    if "order_items" in dfs:
        order_items = dfs["order_items"]
        transformations["calculate_item_total"] = {
            "needed": "total_price" not in order_items.columns
        }

    # Check if orders need order_total calculation
    if "orders" in dfs:
        orders = dfs["orders"]
        transformations["calculate_order_total"] = {
            "needed": "order_total" not in orders.columns
        }

    return transformations


# ============================================================================
# MAIN CHECK FUNCTION
# ============================================================================


def analyze_data_quality():
    """Analyze all datasets and return configuration for pipeline"""

    print("=" * 80)
    print("DATA QUALITY CHECK - ANALYZING DATASETS")
    print("=" * 80)

    # Load datasets
    print("\nLoading datasets...")
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
            print(f"  ✓ {name}")
        except UnicodeDecodeError:
            df = pd.read_csv(f"{DATA_SOURCE_PATH}{file}", encoding="latin-1")
            dfs[name] = df
            print(f"  ✓ {name} (latin-1)")
        except FileNotFoundError:
            print(f"  ✗ {name} - FILE NOT FOUND")

    # Analyze each dataset
    print("\nAnalyzing data quality...")
    config = {
        "metadata": {
            "check_timestamp": datetime.now().isoformat(),
            "datasets_analyzed": len(dfs),
        },
        "datasets": {},
    }

    for name, df in dfs.items():
        print(f"\n  Checking {name}...")

        dataset_config = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "checks": {
                "column_standardization": check_column_standardization(df),
                "duplicates": check_duplicates(df),
                "missing_values": check_missing_values(df, name),
                "data_types": check_data_types(df, name),
            },
        }

        # Table-specific checks
        if name == "products":
            dataset_config["checks"]["quality"] = check_products_quality(df)
        elif name == "order_items":
            dataset_config["checks"]["quality"] = check_order_items_quality(df)
        elif name == "customers":
            dataset_config["checks"]["quality"] = check_customers_quality(df)

        config["datasets"][name] = dataset_config

    # Check transformations
    print("\n  Checking transformations...")
    config["transformations"] = check_transformations_needed(dfs)

    # Determine which pipeline steps are needed
    pipeline_steps = {
        "column_standardization": False,
        "data_cleaning": False,
        "data_transformation": False,
    }

    # Check if column standardization is needed for any dataset
    for name, dataset_config in config["datasets"].items():
        if dataset_config["checks"]["column_standardization"]["needed"]:
            pipeline_steps["column_standardization"] = True
            break

    # Check if any cleaning is needed
    for name, dataset_config in config["datasets"].items():
        checks = dataset_config["checks"]
        if (
            checks["duplicates"]["needed"]
            or checks["missing_values"]["needed"]
            or checks["data_types"]["needed"]
            or checks.get("quality", {}).get("needed", False)
        ):
            pipeline_steps["data_cleaning"] = True
            break

    # Check if transformations are needed
    for trans_name, trans_config in config["transformations"].items():
        if trans_config["needed"]:
            pipeline_steps["data_transformation"] = True
            break

    config["pipeline_steps"] = pipeline_steps

    # Calculate overall score
    total_checks = 0
    passed_checks = 0

    for dataset_config in config["datasets"].values():
        for check_name, check_result in dataset_config["checks"].items():
            total_checks += 1
            if not check_result.get("needed", False):
                passed_checks += 1

    quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
    config["overall_quality_score"] = round(quality_score, 2)

    return config


def print_summary(config):
    """Print human-readable summary"""
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    print(f"\nOverall Quality Score: {config['overall_quality_score']:.2f}%")

    print("\nPipeline Steps Required:")
    for step, needed in config["pipeline_steps"].items():
        status = "✓ NEEDED" if needed else "✗ SKIP"
        print(f"  {step:25} : {status}")

    print("\nDataset Issues:")
    for name, dataset_config in config["datasets"].items():
        issues = []
        checks = dataset_config["checks"]

        if checks["column_standardization"]["needed"]:
            issues.append("columns")
        if checks["duplicates"]["needed"]:
            issues.append(f"duplicates ({checks['duplicates']['count']})")
        if checks["missing_values"]["needed"]:
            issues.append(
                f"missing values ({len(checks['missing_values']['columns'])} cols)"
            )
        if checks["data_types"]["needed"]:
            issues.append(f"types ({len(checks['data_types']['issues'])} cols)")
        if checks.get("quality", {}).get("needed"):
            issues.append("quality")

        if issues:
            print(f"  {name:15} : {', '.join(issues)}")
        else:
            print(f"  {name:15} : ✓ Clean")

    print("\nTransformations Needed:")
    for trans_name, trans_config in config["transformations"].items():
        status = "✓ NEEDED" if trans_config["needed"] else "✗ SKIP"
        print(f"  {trans_name:25} : {status}")

def save_config_yaml(config, filename="pipeline_config.yaml"):
    # Convert all numpy/pandas types to native Python types
    def convert(o):
        if isinstance(o, (np.bool_, np.bool)):
            return bool(o)
        if isinstance(o, (np.integer,)):
            return int(o)
        if isinstance(o, (np.floating,)):
            return float(o)
        if isinstance(o, (pd.Timestamp,)):
            return o.isoformat()
        if isinstance(o, dict):
            return {k: convert(v) for k, v in o.items()}
        if isinstance(o, list):
            return [convert(i) for i in o]
        return o

    clean_config = convert(config)

    with open(filename, "w") as f:
        yaml.dump(clean_config, f, sort_keys=False, default_flow_style=False)

    print(f"✓ Configuration saved to {filename}")



# ============================================================================
# MAIN EXECUTION
# ============================================================================


def main():
    """Run data quality checks and generate pipeline configuration"""

    # Analyze data
    config = analyze_data_quality()

    # Print summary
    print_summary(config)

    # Save configuration
    save_config_yaml(config)

    print("\n" + "=" * 80)
    print("CHECK COMPLETE!")
    print("=" * 80)
    print("\nNext steps:")
    print("1. Review pipeline_config.yaml")
    print("2. Run main.py with this configuration")
    print("=" * 80)

    return config


if __name__ == "__main__":
    config = main()

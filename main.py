import os
import logging
import time
from src.extract import load_data
from src.transform import clean_data, aggregate_data
from src.load import save_data
from src.utils import log_abnormalities
from src.dw.dw_model import build_star_schema
from src.config import (
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
    ASSUMPTIONS_PATH,
    AGG_COUNTRY_PATH,
    AGG_CUSTOMER_PATH,
    AGG_MONTHLY_PATH,
    DW_FACT_PATH,
    DW_DIM_CUSTOMER_PATH,
    DW_DIM_PRODUCT_PATH,
    DW_DIM_DATE_PATH
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("output/pipeline.log"),
        logging.StreamHandler()
    ]
)

def main():
    start_total = time.perf_counter()
    try:
        # Step 1: Extract
        logging.info("Loading raw data...")
        start = time.perf_counter()
        df_raw = load_data(RAW_DATA_PATH)
        duration = time.perf_counter() - start
        logging.info(f"Raw data loaded with shape: {df_raw.shape} in {duration:.2f}s")

        # Step 2: Transform
        logging.info("Cleaning data...")
        start = time.perf_counter()
        df_clean = clean_data(df_raw)
        duration = time.perf_counter() - start
        logging.info(f"Data cleaned. Rows before: {len(df_raw)}, after: {len(df_clean)} in {duration:.2f}s")

        # Step 3: Log abnormalities before and after cleaning
        logging.info("Checking for abnormalities (raw vs clean)...")
        start = time.perf_counter()

        issues_raw = log_abnormalities(df_raw)
        issues_clean = log_abnormalities(df_clean)

        os.makedirs(os.path.dirname(ASSUMPTIONS_PATH), exist_ok=True)
        with open(ASSUMPTIONS_PATH, "w", encoding="utf-8") as f:
            f.write("Assumptions & Abnormalities:\n\n")

            f.write("Issues Detected in Raw Data:\n")
            if issues_raw:
                for issue in issues_raw:
                    f.write(f"- {issue}\n")
                    logging.warning(f"[RAW] {issue}")
            else:
                f.write("- No abnormalities found in raw data.\n")
                logging.info("No abnormalities found in raw data.")

            f.write("\n Issues Still Present After Cleaning:\n")
            if issues_clean:
                for issue in issues_clean:
                    f.write(f"- {issue}\n")
                    logging.warning(f"[CLEAN] {issue}")
            else:
                f.write("- No abnormalities found in cleaned data.\n")
                logging.info("No abnormalities found in cleaned data.")

        logging.info(f"Abnormality logging completed in {time.perf_counter() - start:.2f}s")

        # Step 4: Aggregations
        logging.info("Aggregating data for reporting...")
        start = time.perf_counter()
        aggregates = aggregate_data(df_clean)
        logging.info(f"Aggregation completed in {time.perf_counter() - start:.2f}s")

        # Step 5: Data Warehouse Model
        logging.info("Building data warehouse (star schema) tables...")
        start = time.perf_counter()
        dw_tables = build_star_schema(df_clean)
        os.makedirs(os.path.dirname(DW_FACT_PATH), exist_ok=True)
        save_data(dw_tables["fact_invoice"], DW_FACT_PATH)
        save_data(dw_tables["dim_customer"], DW_DIM_CUSTOMER_PATH)
        save_data(dw_tables["dim_product"], DW_DIM_PRODUCT_PATH)
        save_data(dw_tables["dim_date"], DW_DIM_DATE_PATH)
        logging.info(f"Star schema tables built and saved in {time.perf_counter() - start:.2f}s")

        # Step 6: Save processed outputs
        logging.info("Saving output files...")
        start = time.perf_counter()
        save_data(df_clean, PROCESSED_DATA_PATH)
        save_data(aggregates["agg_by_country"], AGG_COUNTRY_PATH)
        save_data(aggregates["agg_by_customer"], AGG_CUSTOMER_PATH)
        save_data(aggregates["agg_monthly"], AGG_MONTHLY_PATH)
        logging.info(f"Output files saved in {time.perf_counter() - start:.2f}s")

        logging.info(f"ETL pipeline completed successfully in {time.perf_counter() - start_total:.2f}s")

    except Exception:
        logging.exception("Pipeline failed due to an error.")
        raise

if __name__ == "__main__":
    main()

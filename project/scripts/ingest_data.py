from pathlib import Path
import logging
import duckdb as duckdb
import pandas as pd

# Initialize and configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Script constants
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
DUCKDB_PATH = PROJECT_DIR / "nhl.duckdb"
ALLOWED_EXCEL_EXTENSIONS = [".xlsx", ".xls", ".xlsm"]
ALLOWED_EXTENSIONS = [".csv"] + ALLOWED_EXCEL_EXTENSIONS

# Create directories if they don't exist
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

def raw_to_parquet(file_path: Path) -> list[Path]:
    """
    Convert raw file (CSV/Excel) to Parquet format with error
    handling and memory optimization.
    
    Args:
        file_path (Path): Path to the raw file (CSV/Excel).
    Returns:
        Path: Path to the output Parquet file.
    Raises:
        Various exceptions for file reading/writing issues.
    
    Author: IFD
    Date: 2025-10-16
    """
    try:
        logger.info(f"Processing {file_path.name}")
        
        # Check if file exists and is not empty
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return []
            
        # If file is empty, log and skip
        if file_path.stat().st_size == 0:
            logger.warning(f"File is empty: {file_path}")
            return []
        
        # Initialize empty list for Parquet paths
        parquet_paths = []
        # Read file in chunks if file is large
        file_size = file_path.stat().st_size

        # If file is larger than 100MB, read in chunks
        # to avoid memory issues
        if file_path.suffix == ".csv":
            if file_size > 100 * 1024 * 1024:
                logger.info(f"Large file detected ({file_size / 1024 / 1024:.1f}MB), reading in chunks")
                chunks = []
                chunk_size = 10000
                for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.read_csv(file_path)

            # Basic data validation
            if df.empty:
                logger.warning(f"DataFrame is empty after reading {file_path}")
                return []
            
            logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from {file_path.name}")
            
                            # Share some info about data quality per sheet
            logger.info(
                f"Data insights for '{file_path.name}': \n" +
                f"Nulls: {df.isnull().sum().sum()}\n" +
                f"Duplicates: {df.duplicated().sum()}")

            # Output Parquet path
            parquet_path = BRONZE_DIR / (file_path.stem + ".parquet")
            df.to_parquet(parquet_path, index=False, compression='snappy')
            logger.info(f"✅ Converted {file_path.name} → {parquet_path.name}")
            parquet_paths.append(parquet_path)

        elif file_path.suffix in ALLOWED_EXCEL_EXTENSIONS:
            # Handle Excel files (multiple sheets)
            logger.info(f"Reading all sheets from Excel file: {file_path.name}")
            
            # Read all sheets
            sheets_dict = pd.read_excel(file_path, sheet_name=None)
            
            if not sheets_dict:
                logger.warning(f"No sheets found in {file_path}")
                return []
            
            logger.info(f"Found {len(sheets_dict)} sheets: {list(sheets_dict.keys())}")
            
            for sheet_name, df in sheets_dict.items():
                # Skip empty sheets
                if df.empty:
                    logger.warning(f"Sheet '{sheet_name}' is empty, skipping")
                    continue
                
                logger.info(f"Processing sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
                
                # Create parquet filename with sheet name
                safe_sheet_name = "".join(c for c in sheet_name if c.isalnum() or c in ('-', '_')).lower()
                parquet_filename = f"{file_path.stem}_{safe_sheet_name}.parquet"
                parquet_path = BRONZE_DIR / parquet_filename

                # Share some info about data quality per sheet
                logger.info(
                    f"Data insights for sheet '{sheet_name}': \n" +
                    f"Nulls: {df.isnull().sum().sum()}\n" +
                    f"Duplicates: {df.duplicated().sum()}")
                
                # Save to parquet
                df.to_parquet(parquet_path, index=False, compression='snappy')
                logger.info(f"✅ Converted {file_path.name}[{sheet_name}] → {parquet_path.name}")
                parquet_paths.append(parquet_path)
        
        return parquet_paths
        
    except pd.errors.EmptyDataError:
        logger.error(f"Empty data error reading {file_path}")
        return []
    except pd.errors.ParserError as e:
        logger.error(f"Parser error reading {file_path}: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error processing {file_path}: {e}")
        return []

def load_into_duckdb(parquet_path: Path, con):
    """
    Load Parquet file into DuckDB with error handling.
    
    Args:
        parquet_path (Path): Path to the Parquet file.
        con: DuckDB connection object.
    Raises:
        Various exceptions for database operations.
    
    Author: IFD
    Date: 2025-10-16
    """
    try:
        table_name = parquet_path.stem
        
        # Create schema if it doesn't exist
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        
        # Use absolute path for DuckDB
        abs_parquet_path = parquet_path.resolve()
        
        # Create table from Parquet file
        con.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM read_parquet('{abs_parquet_path}');")
        
        # Get row count for verification
        result = con.execute(f"SELECT COUNT(*) FROM bronze.{table_name};").fetchone()
        row_count = result[0] if result else 0
        
        logger.info(f"Loaded {table_name} into DuckDB (bronze.{table_name}) - {row_count:,} rows")
        
    except Exception as e:
        logger.error(f"Error loading {parquet_path} into DuckDB: {e}")
        raise

def main():
    """
    Main ingestion function with comprehensive error handling.
    Will attempt to get all files from RAW_DIR, convert to Parquet,
    and load into DuckDB.
    
    Returns:
        None
    Raises:
        Various exceptions for file processing and database operations.

    Author: IFD
    Date: 2025-10-17
    """
    try:
        logger.info("Starting data ingestion process")
        logger.info(f"Raw data directory: {RAW_DIR}")
        logger.info(f"Bronze data directory: {BRONZE_DIR}")
        logger.info(f"DuckDB path: {DUCKDB_PATH}")
        
        # Check if raw data directory exists and has files
        if not RAW_DIR.exists():
            logger.error(f"Raw data directory does not exist: {RAW_DIR}")
            return
            
        all_files = [f for f in RAW_DIR.iterdir() if f.suffix in ALLOWED_EXTENSIONS]

        csv_files = [f for f in all_files if f.suffix == ".csv"]
        excel_files = [f for f in all_files if f.suffix in ALLOWED_EXCEL_EXTENSIONS]

        if not all_files:
            logger.warning(f"No supported files found in {RAW_DIR}")
            return
            
        logger.info(f"Found {len(csv_files)} CSV files to process")
        logger.info(f"Found {len(excel_files)} Excel files to process")
        
        # Connect to DuckDB
        con = duckdb.connect(str(DUCKDB_PATH))
        
        successful_loads = 0
        failed_loads = 0
        
        # Process each CSV file
        for file in all_files:
            logger.info(f"Processing file {all_files.index(file) + 1}/{len(all_files)}: {file.name}")
            
            parquet_paths = raw_to_parquet(file)
            
            if parquet_paths:  # Check if list is not empty
                for parquet_path in parquet_paths:
                    try:
                        # Load into DuckDB (bronze schema)
                        load_into_duckdb(parquet_path, con)
                        successful_loads += 1
                    except Exception as e:
                        logger.error(f"Failed to load {parquet_path} into DuckDB: {e}")
                        failed_loads += 1
            else:
                logger.error(f"Failed to convert {file.name} to Parquet")
                failed_loads += 1
        
        # Display summary
        logger.info(f"\n📊 Ingestion Summary:")
        logger.info(f"✅ Successfully processed: {successful_loads} datasets")
        
        # Only show failed count if there were failures
        if failed_loads > 0:
            logger.info(f"❌ Failed to process: {failed_loads} datasets")

        logger.info(f"📋 Total files: {len(all_files)}")
        
        # List tables in DuckDB for verification
        try:
            tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze';").fetchall()
            logger.info(f"🗄️  Tables created in bronze schema: {[table[0] for table in tables]}")
        except Exception as e:
            logger.warning(f"Could not list tables: {e}")
        
        con.close()
        logger.info("🎉 Ingestion process complete!")
        
    except Exception as e:
        logger.error(f"Fatal error in main(): {e}")
        raise

if __name__ == "__main__":
    main()

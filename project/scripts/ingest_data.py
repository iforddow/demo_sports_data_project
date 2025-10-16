# scripts/ingest_data.py
from pathlib import Path
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
#Just for git

# Get the project root directory (parent of scripts directory)
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
DUCKDB_PATH = PROJECT_DIR / "nhl.duckdb"

# Create directories if they don't exist
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

def csv_to_parquet(file_path: Path):
    """Convert CSV file to Parquet format with error handling and memory optimization."""
    try:
        logger.info(f"Processing {file_path.name}")
        
        # Check if file exists and is not empty
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return None
            
        if file_path.stat().st_size == 0:
            logger.warning(f"File is empty: {file_path}")
            return None
        
        # Read CSV in chunks if file is large (>100MB)
        file_size = file_path.stat().st_size
        if file_size > 100 * 1024 * 1024:  # 100MB
            logger.info(f"Large file detected ({file_size / 1024 / 1024:.1f}MB), reading in chunks")
            chunks = []
            chunk_size = 10000
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)
        else:
            # Read entire file at once for smaller files
            df = pd.read_csv(file_path)
        
        # Basic data validation
        if df.empty:
            logger.warning(f"DataFrame is empty after reading {file_path}")
            return None
            
        logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from {file_path.name}")
        
        # Output Parquet path
        parquet_path = BRONZE_DIR / (file_path.stem + ".parquet")
        
        # Save Parquet with compression
        df.to_parquet(parquet_path, index=False, compression='snappy')
        logger.info(f"✅ Converted {file_path.name} → {parquet_path.name}")
        
        return parquet_path
        
    except pd.errors.EmptyDataError:
        logger.error(f"Empty data error reading {file_path}")
        return None
    except pd.errors.ParserError as e:
        logger.error(f"Parser error reading {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error processing {file_path}: {e}")
        return None

def load_into_duckdb(parquet_path: Path, con):
    """Load Parquet file into DuckDB with error handling."""
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
        
        logger.info(f"📦 Loaded {table_name} into DuckDB (bronze.{table_name}) - {row_count:,} rows")
        
    except Exception as e:
        logger.error(f"Error loading {parquet_path} into DuckDB: {e}")
        raise

def main():
    """Main ingestion function with comprehensive error handling."""
    try:
        logger.info("Starting data ingestion process")
        logger.info(f"Raw data directory: {RAW_DIR}")
        logger.info(f"Bronze data directory: {BRONZE_DIR}")
        logger.info(f"DuckDB path: {DUCKDB_PATH}")
        
        # Check if raw data directory exists and has files
        if not RAW_DIR.exists():
            logger.error(f"Raw data directory does not exist: {RAW_DIR}")
            return
            
        csv_files = list(RAW_DIR.glob("*.csv"))
        if not csv_files:
            logger.warning(f"No CSV files found in {RAW_DIR}")
            return
            
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        # Connect to DuckDB
        con = duckdb.connect(str(DUCKDB_PATH))
        
        successful_loads = 0
        failed_loads = 0
        
        # Process each CSV file
        for csv_file in csv_files:
            logger.info(f"Processing file {csv_files.index(csv_file) + 1}/{len(csv_files)}: {csv_file.name}")
            
            parquet_path = csv_to_parquet(csv_file)
            
            if parquet_path is not None:
                try:
                    load_into_duckdb(parquet_path, con)
                    successful_loads += 1
                except Exception as e:
                    logger.error(f"Failed to load {parquet_path} into DuckDB: {e}")
                    failed_loads += 1
            else:
                logger.error(f"Failed to convert {csv_file.name} to Parquet")
                failed_loads += 1
        
        # Display summary
        logger.info(f"\n📊 Ingestion Summary:")
        logger.info(f"✅ Successfully processed: {successful_loads} files")
        logger.info(f"❌ Failed to process: {failed_loads} files")
        logger.info(f"📋 Total files: {len(csv_files)}")
        
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

import logging
import os
from pathlib import Path
from dbt.cli.main import cli

# Path ke DBT project dan profiles.yml
project_path = Path(__file__).resolve().parents[1] / "dbt"
profiles_path = project_path


# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# source table/tag
table = 'retail_transactions_scd'

def main():
    try:
        command = [
            "run",
            "--select", f"tag:{table}",
            "--target", "prod",
            "--project-dir", str(project_path),
            "--profiles-dir", str(profiles_path)
        ]

        logging.info(f"Running DBT command: {' '.join(command)}")

        result = cli(command)

        if result.success:
            logging.info("DBT run completed successfully.")
        else:
            logging.error("DBT run failed.")
            raise Exception("DBT run failed")

    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        raise

if __name__ == "__main__":
    main()

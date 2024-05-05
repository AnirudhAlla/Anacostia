import os
import pandas as pd
import logging
from pathlib import Path
from phe import paillier
import time
import shutil

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])  # This ensures logs are printed to the console

class BaseActionNode:
    """Base class for action nodes in the pipeline."""
    def log(self, message, level="INFO"):
        """Log a message at the specified level."""
        if level == "INFO":
            logging.info(message)
        elif level == "ERROR":
            logging.error(message)

    def log_metrics(self, **metrics):
        """Log metrics information."""
        for key, value in metrics.items():
            logging.info(f"{key}: {value}")

class IncomingDataStoreNode(BaseActionNode):
    """Node to monitor a directory and trigger processing for new files."""
    def __init__(self, watch_directory):
        self.watch_directory = Path(watch_directory)
        self.known_files = set()

    def execute(self):
        """Monitor directory for new files and yield them for processing."""
        while True:
            current_files = set(os.listdir(self.watch_directory))
            new_files = current_files - self.known_files
            self.known_files.update(new_files)
            for file_name in new_files:
                file_path = self.watch_directory / file_name
                if file_path.is_file() and file_path.suffix in ['.xls', '.xlsx']:
                    yield file_path
            time.sleep(10)  # Check every 10 seconds

class DataValidationNode(BaseActionNode):
    def __init__(self, threshold, output_directory):
        self.threshold = threshold
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(exist_ok=True, parents=True)

    def execute(self, input_file):
        output_file = self.output_directory / f"validated_{input_file.name}"
        self.log(f"Performing data validation on {input_file.name}")
        try:
            df = pd.read_excel(input_file)
            missing_value_rate = df.isnull().mean()
            if missing_value_rate.any() > self.threshold:
                raise ValueError("Data contains missing values above threshold")
            df.to_excel(output_file, index=False)
            self.log_metrics(validation_status="passed")
            return output_file
        except (ValueError, TypeError) as e:
            self.log(f"Validation failed on {input_file.name}: {str(e)}", level="ERROR")
            self.log_metrics(validation_status="failed")
            return None

class DataCleaningNode(BaseActionNode):
    def __init__(self, output_directory):
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(exist_ok=True, parents=True)

    def execute(self, input_file):
        output_file = self.output_directory / f"cleaned_{input_file.name}"
        self.log(f"Performing data cleaning on {input_file.name}")
        try:
            df = pd.read_excel(input_file)
            numeric_cols = df.select_dtypes(include=['number']).columns
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
            categorical_cols = df.select_dtypes(include=['object']).columns
            for col in categorical_cols:
                df[col] = df[col].fillna(df[col].mode()[0])
            df.drop_duplicates(inplace=True)
            df.to_excel(output_file, index=False)
            self.log_metrics(cleaning_status="passed")
            return output_file
        except Exception as e:
            self.log(f"Data cleaning failed on {input_file.name}: {str(e)}", level="ERROR")
            self.log_metrics(cleaning_status="failed")
            return None

class FHEEncryptionNode(BaseActionNode):
    def __init__(self, output_directory):
        self.public_key, self.private_key = paillier.generate_paillier_keypair()
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(exist_ok=True, parents=True)

    def execute(self, input_file):
        output_file = self.output_directory / f"encrypted_{input_file.stem}.pickle"
        self.log(f"Starting encryption of {input_file.stem}")
        try:
            df = pd.read_excel(input_file)
            encrypted_data = {column: [self.public_key.encrypt(value) for value in df[column]] for column in df.columns if df[column].dtype in ['int64', 'float64']}
            with open(output_file, 'wb') as f:
                pd.to_pickle(encrypted_data, f)
            self.log("Encryption completed successfully.")
            return output_file
        except Exception as e:
            self.log(f"Encryption failed on {input_file.stem}: {str(e)}", level="ERROR")
            return None

class Pipeline:
    """A class to manage the execution of a series of tasks."""
    def __init__(self, incoming_node, validation_node, cleaning_node, encryption_node):
        self.incoming_node = incoming_node
        self.validation_node = validation_node
        self.cleaning_node = cleaning_node
        self.encryption_node = encryption_node

    def run(self):
        """Run each task set in the pipeline."""
        for file_path in self.incoming_node.execute():
            validated_path = self.validation_node.execute(file_path)
            if validated_path is None:
                continue

            cleaned_path = self.cleaning_node.execute(validated_path)
            if cleaned_path is None:
                continue

            encrypted_path = self.encryption_node.execute(cleaned_path)
            if encrypted_path is None:
                continue

if __name__ == "__main__":
    base_path = Path("C:\\Users\\manik\\OneDrive\\anacostia")
    validate_dir = base_path / "validated_data"
    clean_dir = base_path / "cleaned_data"
    encrypt_dir = base_path / "encrypted_data"

    incoming_data_store = IncomingDataStoreNode(base_path)
    validation_node = DataValidationNode(threshold=0.7, output_directory=validate_dir)
    cleaning_node = DataCleaningNode(output_directory=clean_dir)
    encryption_node = FHEEncryptionNode(output_directory=encrypt_dir)

    pipeline = Pipeline(incoming_data_store, validation_node, cleaning_node, encryption_node)
    try:
        pipeline.run()
    except KeyboardInterrupt:
        print("Keyboard Interrupt received, stopping the pipeline.")

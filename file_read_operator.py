"""
Custom operator for reading files in Apache Airflow.
"""
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class FileReadOperator(BaseOperator):  
    """
    Custom operator to read content from a specified file path.
    """

    @apply_defaults
    def __init__(self, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path

    def execute(self, context):  
        """
        Executes the file read operation.
        """
        self.log.info(f"Reading from file: {self.file_path}")
        try:
            with open(self.file_path, 'r') as f:
                data = f.read()
                self.log.info(f"File content: {data}")
            return data
        except FileNotFoundError:
            self.log.error(f"File not found: {self.file_path}")
            raise
        except IOError as e:
            self.log.error(f"Error reading file {self.file_path}: {str(e)}")
            raise

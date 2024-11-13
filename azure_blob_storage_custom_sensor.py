"""
Custom sensor for checking the existence of a blob in Azure Blob Storage.
"""

from airflow.sensors.base import BaseSensorOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.plugins_manager import AirflowPlugin

class AzureBlobStorageBlobSensor(BaseSensorOperator):
    """
    Custom Sensor to check if a specilsfied blob exists in an Azure Blob Storage container.
    """

    def __init__(self, container_name, blob_name, azure_conn_id="azure_default", *args, **kwargs):
        """
        Initializes the AzureBlobStorageBlobSensor.

        :param container_name: Name of the Azure Blob Storage container
        :param blob_name: Name of the blob file to check
        :param azure_conn_id: Airflow Azure connection ID (default: "azure_default")
        """
        super().__init__(*args, **kwargs)
        self.container_name = container_name
        self.blob_name = blob_name
        self.azure_conn_id = azure_conn_id

    def poke(self, context):
        """Checks if the blob exists in the specified container."""
        hook = WasbHook(wasb_conn_id=self.azure_conn_id)
        
        # Use `check_for_blob` to verify if the blob exists in the specified container
        blob_exists = hook.check_for_blob(container_name=self.container_name, blob_name=self.blob_name)

        if blob_exists:
            self.log.info(
                "Blob '%s' found in container '%s'.",
                self.blob_name,
                self.container_name
            )
        else:
            self.log.info(
                "Waiting for blob '%s' to appear in container '%s'.",
                self.blob_name,
                self.container_name
            )

        return blob_exists  # Returns True if blob exists
    
class AzureBlobSensorPlugin(AirflowPlugin):
    name = "azure_blob_sensor_plugin"
    sensors = [AzureBlobStorageBlobSensor]

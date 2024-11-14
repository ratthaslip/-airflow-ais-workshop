"""
Custom async sensor for checking the existence of a blob in Azure Blob Storage.
"""

# In plugins/azure_blob_sensor_async.py
from datetime import timedelta
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.exceptions import AirflowSkipException
from airflow.utils.context import Context
from airflow.plugins_manager import AirflowPlugin


class AzureBlobSensorAsync(BaseSensorOperator): 
    """
    Asynchronous Sensor to check if a specified blob exists in an Azure Blob Storage container.
    """

    def __init__(self, container_name, blob_name, azure_conn_id="azure_default", poke_interval=60, *args, **kwargs):
        """
        Initializes the AzureBlobSensorAsync.

        :param container_name: Name of the Azure Blob Storage container
        :param blob_name: Name of the blob file to check
        :param azure_conn_id: Airflow Azure connection ID (default: "azure_default")
        :param poke_interval: Interval to wait before checking again
        """
        super().__init__(*args, **kwargs)
        self.container_name = container_name
        self.blob_name = blob_name
        self.azure_conn_id = azure_conn_id
        self.poke_interval = poke_interval

    def execute(self, context):
        # Defer task execution, checking blob existence at each interval
        self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=self.poke_interval)),
                   method_name="check_blob")

    def check_blob(self, context, event=None):
        """Defers until the blob exists in the specified container."""
        hook = WasbHook(wasb_conn_id=self.azure_conn_id)
        blob_exists = hook.check_for_blob(container_name=self.container_name, blob_name=self.blob_name)
        
        if blob_exists:
            self.log.info("Blob '%s' found in container '%s'.", self.blob_name, self.container_name)
        else:
            self.log.info("Waiting for blob '%s' to appear in container '%s'.", self.blob_name, self.container_name)
            raise AirflowSkipException("Blob not found, deferring until available.")

class AzureBlobSensorAsyncPlugin(AirflowPlugin):
    name = "azure_blob_sensor_async_plugin"
    sensors = [AzureBlobSensorAsync]

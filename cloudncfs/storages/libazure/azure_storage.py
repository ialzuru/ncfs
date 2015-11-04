from blob import BlobStorage
from table import TableStorage
from queue import QueueStorage

class AzureStorage():

    blob_proxy = None
    table_proxy = None
    queue_proxy = None

    def __init__(self, blob_host, table_host, queue_host, account_name, secret_key, use_path_style_uris = True):
        # create local proxies to table, queue and blobs
        self.blob_proxy = BlobStorage(blob_host, account_name, secret_key, use_path_style_uris)
        self.table_proxy = TableStorage(table_host, account_name, secret_key, use_path_style_uris)
        self.queue_proxy = QueueStorage(queue_host, account_name, secret_key, use_path_style_uris)

    ## blob operations
    def create_container(self, container_name, is_public = False):
        return self.blob_proxy.create_container(container_name, is_public)

    def delete_container(self, container_name):
        return self.blob_proxy.delete_container(container_name)

    def list_containers(self):
        return self.blob_proxy.list_containers()

    def list_blobs(self, container_name):
        return self.blob_proxy.list_blobs(container_name)

    def put_blob(self, container_name, blob_name, data, content_type = None, metadata = {}):
        return self.blob_proxy.put_blob(container_name, blob_name, data, content_type, metadata)

    def set_metadata(self, container_name, blob_name, metadata):
        return self.blob_proxy.set_metadata(container_name, blob_name, metadata)

    def get_blob(self, container_name, blob_name, offset = None, size = None):
        return self.blob_proxy.get_blob(container_name, blob_name, offset, size)

    def get_metadata(self, container_name, blob_name):
        return self.blob_proxy.get_metadata(container_name, blob_name)

    def delete_blob(self, container_name, blob_name):
        return self.blob_proxy.delete_blob(container_name, blob_name)

    ## queue operations
    def create_queue(self, name):
        return self.queue_proxy.create_queue(name)

    def delete_queue(self, name):
        return self.queue_proxy.delete_queue(name)

    def put_message(self, queue_name, payload):
        return self.queue_proxy.put_message(queue_name, payload)

    def get_message(self, queue_name):
        return self.queue_proxy.get_message(queue_name)

    def delete_message(self, queue_name, message):
        return self.queue_proxy.delete_message(queue_name, message)

    ## table operations
    def create_table(self, name):
        return self.table_proxy.create_table(name)

    def delete_table(self, name):
        return self.table_proxy.delete_table(name)

    def list_tables(self):
        return self.table_proxy.list_tables()

    def get_entity(self, table_name, partition_key, row_key):
        return self.table_proxy.get_entity(table_name, partition_key, row_key)

    def get_all(self, table_name):
        return self.table_proxy.get_all(table_name)

class Client():
    def __init__(self, boto3_client, single_table_name):
        self.boto3_client = boto3_client
        self.table_name = single_table_name

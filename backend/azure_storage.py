from azure.storage.blob import BlobServiceClient

AZURE_BLOB_CONNECTION = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
CONTAINER = "dados-pregao"


def save_file_to_blob(file_name, local_path_file):

    service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
    container = service.get_container_client(CONTAINER)
    try:
        service.create_container(CONTAINER)
    except Exception as e:
        pass #container ja existe

    with open(local_path_file, "rb") as data:
        container.upload_blob(name=file_name, data=data, overwrite=True)


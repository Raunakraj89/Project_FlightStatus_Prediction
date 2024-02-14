import google.auth
from google.cloud import bigquery
from google.cloud import secretmanager

def getsecret(project, secretid):
    credentials, project = google.auth.default()

    # Create a Secret Manager client
    client = secretmanager.SecretManagerServiceClient()
    version = "latest"
    # Get the name of the secret
    secret_name = "projects/{}/secrets/{}/versions/{}".format(project, secretid, version)

    # Access the secret version
    version_name = client.access_secret_version(request={"name": secret_name}).name

    # Get the payload
    payload = client.access_secret_version(request={"name": version_name}).payload.data

    # Decode the payload
    return payload.decode("UTF-8")

if __name__ == "__main__":
    print(getsecret("906962432092", "mysqlrootpassword"))
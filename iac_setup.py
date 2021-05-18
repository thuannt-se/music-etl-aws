import boto3

def create_client():
    # Create IAM client
    iam = boto3.client('iam')

    # Create an access key
    response = iam.create_access_key(
        UserName='airflow_redshift_user'
    )

    print(response['AccessKey'])




def main():
    create_client()

if __name__ == "__main__":
    main()
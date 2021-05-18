import boto3
import configparser
import json
import pandas as pd
from botocore.exceptions import ClientError

from AWSConfig import AWSConfig

def create_client_from(awsConfig):
    '''
        Create aws clients from AWSConfig object
                Parameters:
                        AWSConfig (obj): object contain configurations
    '''
    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                         aws_access_key_id=awsConfig.KEY,
                         aws_secret_access_key=awsConfig.SECRET
                         )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=awsConfig.KEY,
                        aws_secret_access_key=awsConfig.SECRET
                        )

    iam = boto3.client('iam', aws_access_key_id=awsConfig.KEY,
                       aws_secret_access_key=awsConfig.SECRET,
                       region_name='us-west-2'
                       )

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=awsConfig.KEY,
                            aws_secret_access_key=awsConfig.SECRET
                            )
    return ec2, s3, iam, redshift

def initialize_aws_instances():
    '''
        Initialize infrastructure for aws service: ec2, s3, iam, redshift
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    awsConfig = AWSConfig(config)
    ec2, s3, iam, redshift = create_client_from(awsConfig)
    #First, we create the role for redshift
    try:
        print("1.1 Creating a new IAM Role")
        iam.create_role(
            Path='/',
            RoleName=awsConfig.DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement':
                    [
                        {
                            'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}}
                    ],
                    'Version': '2012-10-17'})
        )

        print("1.2 Attaching Policy")
        iam.attach_role_policy(RoleName=awsConfig.DWH_IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                               )['ResponseMetadata']['HTTPStatusCode']
        print("1.3 Get the IAM role ARN and set it to config")
        awsConfig.ARN = iam.get_role(RoleName=awsConfig.DWH_IAM_ROLE_NAME)['Role']['Arn']

    except Exception as e:
        print(e)

    #Second, we create Redshift cluster
    clusterVpcId = None
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=awsConfig.DWH_CLUSTER_TYPE,
            NodeType=awsConfig.DWH_NODE_TYPE,
            NumberOfNodes=int(awsConfig.DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=awsConfig.DWH_DB_NAME,
            ClusterIdentifier=awsConfig.DWH_CLUSTER_IDENTIFIER,
            MasterUsername=awsConfig.DWH_DB_USER,
            MasterUserPassword=awsConfig.DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[awsConfig.ARN]
        )
        awsConfig.HOST = response['Cluster']['Endpoint']['Address']
        clusterVpcId = response['Cluster']['VpcId']
    except Exception as e:
        print(e)
    print(awsConfig.HOST)
    #Third, we open tcp port to redshift
    try:
        vpc = ec2.Vpc(id=clusterVpcId)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(awsConfig.DWH_PORT),
            ToPort=int(awsConfig.DWH_PORT)
        )
    except Exception as e:
        print(e)

def write_to_config(awsConfig: AWSConfig):
    config = configparser.ConfigParser()
    cfgfile = open('dwh.cfg', 'w')
    config.set('HOST', awsConfig.HOST)
    config.set('ARN', awsConfig.ARN)
    config.write(cfgfile)
    cfgfile.close()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=config.get('AWS', 'KEY'),
                        aws_secret_access_key=config.get('AWS','SECRET')
                        )
    sampleDbBucket = s3.Bucket("udacity-dend")

    for obj in sampleDbBucket.objects.filter(Prefix='song-data'):
        print(obj.key)
    #initialize_aws_instances()

if __name__ == "__main__":
    main()

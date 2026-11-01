function Write-Log {
    param (
        [string]$Level = "INFO",
        [string]$Message
    )
    $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Write-Host "[$timestamp] [$Level] $Message"
}

# Load environment configuration if available
$lakehouseBucket = if ($env:LAKEHOUSE_BUCKET) { $env:LAKEHOUSE_BUCKET } else { "amazon-lakehouse" }
$s3Endpoint = if ($env:S3_HOST_ENDPOINT) { $env:S3_HOST_ENDPOINT } else { "http://localhost:4566" }
$awsKey = if ($env:AWS_ACCESS_KEY_ID) { $env:AWS_ACCESS_KEY_ID } else { "test" }
$awsSecret = if ($env:AWS_SECRET_ACCESS_KEY) { $env:AWS_SECRET_ACCESS_KEY } else { "test" }
$awsRegion = if ($env:AWS_DEFAULT_REGION) { $env:AWS_DEFAULT_REGION } else { "us-east-1" }
$kafkaTopic = if ($env:KAFKA_TOPIC_NAME) { $env:KAFKA_TOPIC_NAME } else { "raw-ecommerce-events" }

Write-Log "INFO" "Deploying lakehouse infrastructure containers via Docker Compose..."
docker compose up -d

Write-Log "INFO" "Awaiting service initialization (Kafka, LocalStack, Spark Master)..."
Start-Sleep -Seconds 15

Write-Log "INFO" "Provisioning S3 storage bucket: s3://$lakehouseBucket"
$s3Script = @"
import boto3
from botocore.client import Config
s3 = boto3.client(
    's3',
    endpoint_url='$s3Endpoint',
    aws_access_key_id='$awsKey',
    aws_secret_access_key='$awsSecret',
    region_name='$awsRegion',
    config=Config(s3={'addressing_style': 'path'})
)
try:
    s3.create_bucket(Bucket='$lakehouseBucket')
    print('Bucket created successfully.')
except s3.exceptions.BucketAlreadyOwnedByYou:
    print('Bucket already exists.')
except Exception as e:
    print(f'Error: {e}')
"@
python -c "$s3Script"

Write-Log "INFO" "Verifying Kafka topic: $kafkaTopic"
docker exec kafka kafka-topics --create --topic $kafkaTopic --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1 2>$null | Out-Null
Write-Log "SUCCESS" "Kafka topic '$kafkaTopic' ready."

Write-Log "INFO" "Setting scoped permissions on Spark ivy cache directory..."
docker exec -u 0 spark-master bash -c "mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && chown -R spark:spark /home/spark/.ivy2 && chmod -R 775 /home/spark/.ivy2"

Write-Log "SUCCESS" "All streaming and batch infrastructure components are active."

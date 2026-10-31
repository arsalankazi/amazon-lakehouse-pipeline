Write-Host ">>> [1/4] Starting Docker Containers..." -ForegroundColor Cyan
docker compose up -d

Write-Host ">>> Waiting 15s for Kafka & LocalStack initialization..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

Write-Host ">>> [2/4] Creating LocalStack S3 Bucket..." -ForegroundColor Cyan
python -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1'); s3.create_bucket(Bucket='amazon-lakehouse'); print('Bucket amazon-lakehouse Ready!')"

Write-Host ">>> [3/4] Creating Kafka Topic 'raw-ecommerce-events'..." -ForegroundColor Cyan
docker exec -it kafka kafka-topics --create --topic raw-ecommerce-events --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1 2>$null
Write-Host "Kafka Topic Ready!" -ForegroundColor Green

Write-Host ">>> [4/4] Setting Spark Master Ivy Permissions..." -ForegroundColor Cyan
docker exec -u 0 -it spark-master bash -c "mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && chown -R spark:spark /home/spark/.ivy2 && chmod -R 777 /home/spark/.ivy2"

Write-Host ">>> Infrastructure is Ready." -ForegroundColor Green
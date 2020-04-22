import boto3

client=boto3.client('dynamodb')

for i in range(100,1000000):
    response = client.put_item(TableName='product',Item={"product_id": {"N": str(i) },"version": {"N": "23"},"pro_ver": {"N": str(i) },"Name": {"S": "Testname this is for test" },"order_id": {"N": str(i+10) }})
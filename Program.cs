using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbDemo
{
    internal class Program
    {
        private static readonly AmazonDynamoDBClient client =
            new AmazonDynamoDBClient(new AmazonDynamoDBConfig { ServiceURL = "http://localhost:8000" });

        private const string orderId = "{3e80b07d-e2e6-4310-8fda-851296a17a10}";
        private const string driverId = "АК9265АК";
        private const string clientId = "0993832478";
        private const string tableName = "Taxi";

        private static async Task Main(string[] args)
        {
            await CreateTable();
            await AddClientDriverAndOrder();
            await PrintTable();

            await AssignOrderToDriver();
            await PrintTable();
        }

        private static async Task PrintTable()
        {
            var result = await client.ScanAsync(new ScanRequest
            {
                TableName = tableName
            });
            Console.WriteLine($"==== Table {tableName} ====");
            foreach (var item in result.Items)
            {
                foreach (var attr in item)
                {
                    Console.WriteLine($"#{attr.Key}: '{attr.Value.S}'");
                }
                Console.WriteLine();
            }
        }

        private static async Task CreateTable()
        {
            await client.CreateTableAsync(new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Id",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Id",
                        KeyType = "HASH"
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5,
                    WriteCapacityUnits = 5
                }
            });
        }

        private static async Task AddClientDriverAndOrder()
        {
            await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>()
                {
                    { "Id", new AttributeValue { S = clientId }},
                    { "OrderId", new AttributeValue { S = orderId }}
                }
            });

            await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>()
                {
                    { "Id", new AttributeValue { S = driverId }}
                }
            });

            await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>()
                {
                    { "Id", new AttributeValue { S = orderId }},
                    { "ClientId", new AttributeValue { S = clientId }},
                    { "OrderStatus", new AttributeValue {S = "Pending"}}
                }
            });
        }

        private static async Task AssignOrderToDriver()
        {
            await client.TransactWriteItemsAsync(new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = tableName,
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", new AttributeValue { S = driverId }}
                            },
                            UpdateExpression = "set OrderId = :OrderId",
                            ConditionExpression = "attribute_not_exists(OrderId)",
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                            {
                                {":OrderId", new AttributeValue { S = orderId}}
                            },
                            ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD
                        }
                    },
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = tableName,
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", new AttributeValue { S = orderId }}
                            },
                            UpdateExpression = "set DriverId = :DriverId, OrderStatus = :NewStatus",
                            ConditionExpression = "attribute_not_exists(DriverId) AND OrderStatus=:OldStatus",
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                            {
                                {":DriverId", new AttributeValue { S = driverId}},
                                {":OldStatus", new AttributeValue { S = "Pending"}},
                                {":NewStatus", new AttributeValue { S = "InProgress"}}
                            },
                            ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD
                        }
                    }
                },
                ClientRequestToken = "IdempotencyToken",
                
            });
        }
    }
}
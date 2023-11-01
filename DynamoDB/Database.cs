using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace DynamoDB
{
    public class Database
    {
        private readonly AmazonDynamoDBClient _client;
        private const string _awsAccessKeyID = "AWSAccessKeyID";
        private const string _awsSecretAccessKey = "AWSSecretAccessKey";

        public Database()
        {
            _client = new AmazonDynamoDBClient(_awsAccessKeyID, _awsSecretAccessKey, new AmazonDynamoDBConfig
            {
                ServiceURL = "http://localhost:8000", // Local DynamoDB endpoint,
                RegionEndpoint = Amazon.RegionEndpoint.USEast1
            });
        }
        public async Task CreateTable()
        {
            string tableName = "Movies";
            try
            {
                var request = new CreateTableRequest
                {
                    TableName = tableName,
                    AttributeDefinitions = new List<AttributeDefinition>
                        {
                            new AttributeDefinition
                            {
                                AttributeName = "year",
                                // "S" = string, "N" = number, and so on.
                                AttributeType = ScalarAttributeType.N
                            },
                            new AttributeDefinition
                            {
                                AttributeName = "title",
                                AttributeType = ScalarAttributeType.S
                            }
                        },
                    KeySchema = new List<KeySchemaElement>
                        {
                             new KeySchemaElement
                            {
                              AttributeName = "year",
                              // "HASH" = hash key, "RANGE" = range key.
                              KeyType = KeyType.HASH
                            },
                            new KeySchemaElement
                            {
                              AttributeName = "title",
                              KeyType = KeyType.RANGE
                            }
                        },
                    BillingMode = BillingMode.PROVISIONED,
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = 10,
                        WriteCapacityUnits = 10
                    }
                };
                var response = await _client.CreateTableAsync(request);
                if (response.HttpStatusCode == HttpStatusCode.OK)
                {
                    Console.WriteLine("Table created successfully.");
                }
                else
                {
                    Console.WriteLine("Table creation failed.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
        }

        public void InsertData()
        {
            var options = new JsonDocumentOptions
            {
                AllowTrailingCommas = true,
                CommentHandling = JsonCommentHandling.Skip
            };

            var sr = new FileStream(@"moviedata.txt", FileMode.Open, FileAccess.Read);
            var jsonDocument = JsonDocument.Parse(sr, options);
            var table = Table.LoadTable(_client, "Movies");
            foreach (JsonElement je in jsonDocument.RootElement.EnumerateArray())
            {
                var item = new Document();
                foreach (JsonProperty je2 in je.EnumerateObject())
                {
                    if (je2.Name == "year")
                    {
                        item[je2.Name] = je2.Value.GetInt32();
                    }
                    else
                    {
                        item[je2.Name] = je2.Value.ToString(); ;
                    }

                }
                var response = table.PutItemAsync(item);
            }
        }
    }
}

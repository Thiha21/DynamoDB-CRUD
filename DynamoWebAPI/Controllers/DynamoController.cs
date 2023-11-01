using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DynamoWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DynamoController : ControllerBase
    {
        private readonly IConfiguration _config;
        private readonly IAmazonDynamoDB _dynamodb;
        private const string _tableName = "Movies";
        public DynamoController(IConfiguration config,IAmazonDynamoDB dynamoDB)
        {
            _config = config;
            _dynamodb = dynamoDB;
        }

        [HttpGet("GetTableList")]
        public async Task<IActionResult> TableList()
        {
            List<string> tableList = new List<string>();
            var request = new ListTablesRequest
            {
                Limit = 100
            };
            var response = await _dynamodb.ListTablesAsync(request);
            tableList = response.TableNames;
            return Ok(tableList);
        }

        [HttpPost("CreateMoviesTable")]
        public async Task<IActionResult> CreateTable()
        {
            var request = new CreateTableRequest
            {
                TableName = _tableName,
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
            var response = await _dynamodb.CreateTableAsync(request);
            if (response.HttpStatusCode == HttpStatusCode.OK)
            {
                return Ok("Table created successfully.");
            }
            else
            {
                return Ok("Table creation failed.");
            }
        }

        [HttpPost("Insert")]
        public async Task<IActionResult> Insert(List<Movie> movies)
        {
            List <WriteRequest> writeRequestList = new List<WriteRequest>();
            foreach (Movie m in movies)
            {
                WriteRequest wr = new WriteRequest
                {
                    PutRequest = new PutRequest
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            { "year", new AttributeValue { N = m.Year.ToString() } },
                            { "title", new AttributeValue { S = m.Title } },
                            { "info", new AttributeValue { S = m.Info } }
                        }
                    }
                };
                writeRequestList.Add(wr);
            }
            var request = new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    {
                        _tableName, writeRequestList
                    }
                }
            };

            var response = await _dynamodb.BatchWriteItemAsync(request);
            if (response.UnprocessedItems.Count > 0)
            {
                return Ok("Some items were not processed.");
            }
            else
            {
                return Ok("All items were successfully written to the table.");
            }
        }

        [HttpPost("Update/{year:int}/{title}")]
        public async Task<IActionResult> Update(int year,string title,string info)
        {
            var updateRequest = new UpdateItemRequest
            {
                TableName = _tableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    { "year", new AttributeValue { N = year.ToString() } },
                    { "title", new AttributeValue { S = title } }
                },
                UpdateExpression = "SET info = :newInfo",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":newInfo", new AttributeValue { S = info } }
                },
                ReturnValues = ReturnValue.ALL_NEW
            };

            var response = await _dynamodb.UpdateItemAsync(updateRequest);
            if (response.HttpStatusCode == HttpStatusCode.OK)
            {
                return Ok("Updated successfully.");
            }
            else
            {
                return Ok("update failed.");
            }
        }

        [HttpDelete("Delete/{year:int}/{title}")]
        public async Task<IActionResult> Delete(int year,string title)
        {
            var deleteRequest = new DeleteItemRequest
            {
                TableName = _tableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    { "year", new AttributeValue { N = year.ToString() } },
                    { "title", new AttributeValue { S = title } }
                }
            };

            var response = await _dynamodb.DeleteItemAsync(deleteRequest);
            if (response.HttpStatusCode == HttpStatusCode.OK)
            {
                return Ok("Deleted successfully.");
            }
            else
            {
                return Ok("Delete failed.");
            }
        }

        [HttpPost("InsertFromFile")]
        public async Task<IActionResult> InsertFromFile(IFormFile file)
        {
            var options = new JsonDocumentOptions
            {
                AllowTrailingCommas = true,
                CommentHandling = JsonCommentHandling.Skip
            };

            //var sr = new FileStream(, FileMode.Open, FileAccess.Read);
            var ms = new MemoryStream();
            await file.CopyToAsync(ms);
            ms.Seek(0, SeekOrigin.Begin);
            var jsonDocument = JsonDocument.Parse(ms, options);
            var table = Table.LoadTable(_dynamodb, _tableName);
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
                        item[je2.Name] = je2.Value.ToString();
                    }

                }
                await table.PutItemAsync(item);
            }
            return Ok("Inserted");
        }

        [HttpGet("GetMovies/{year:int}")]
        public async Task<IActionResult> GetMovies(int year)
        {
            var qResponse = await _dynamodb.QueryAsync(new QueryRequest
            {
                TableName = _tableName,
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#yr","year" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    {":qYr", new AttributeValue {N = year.ToString()} }
                },
                KeyConditionExpression = "#yr = :qYr",
                ProjectionExpression = "#yr, title, info"
            });
            List<object> response = new List<object>();

            foreach (var ddbItem in qResponse.Items)
            {
                string itemYear = "",itemTitle = "";
                object itemInfo = new();
                foreach (var sKey in ddbItem.Keys)
                {
                    if (sKey == "year")
                    {
                        itemYear = ddbItem[sKey].N;
                    }
                    else if(sKey == "title")
                    {
                        itemTitle = ddbItem[sKey].S;
                    }
                    else
                    {
                        try
                        {
                            itemInfo = JsonDocument.Parse(ddbItem[sKey].S);
                        }
                        catch 
                        {
                            itemInfo = ddbItem[sKey].S;
                        }
                    }
                }
                Object obj = new
                {
                    year = itemYear,
                    title = itemTitle,
                    info = itemInfo
                };
                
                response.Add(obj);
            }
            return Ok(response);
        }
        
        [HttpGet("GetMovies/{fromYear:int}/{toYear:int}")]
        public async Task<IActionResult> GetMovies(int fromYear,int toYear)
        {
            var sResponse = await _dynamodb.ScanAsync(new ScanRequest
            {
                TableName = _tableName,
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#yr", "year" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":yFromYear", new AttributeValue { N = fromYear.ToString() } },
                    { ":yToYear", new AttributeValue { N = toYear.ToString() } },
                },
                FilterExpression = "#yr between :yFromYear and :yToYear",
                ProjectionExpression = "#yr, title, info"
            });
            List<object> response = new List<object>();

            foreach (var ddbItem in sResponse.Items)
            {
                string itemYear = "", itemTitle = "";
                object itemInfo = new();
                foreach (var sKey in ddbItem.Keys)
                {
                    if (sKey == "year")
                    {
                        itemYear = ddbItem[sKey].N;
                    }
                    else if (sKey == "title")
                    {
                        itemTitle = ddbItem[sKey].S;
                    }
                    else
                    {
                        try
                        {
                            itemInfo = JsonDocument.Parse(ddbItem[sKey].S);
                        }
                        catch
                        {
                            itemInfo = ddbItem[sKey].S;
                        }
                    }
                }
                Object obj = new
                {
                    year = itemYear,
                    title = itemTitle,
                    info = itemInfo
                };

                response.Add(obj);
            }
            return Ok(response);
        }
    }
}

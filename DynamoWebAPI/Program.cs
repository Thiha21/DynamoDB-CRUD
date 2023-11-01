using Amazon.DynamoDBv2;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
AmazonDynamoDBConfig config = new AmazonDynamoDBConfig
{
    ServiceURL = builder.Configuration["Amazon:URL"].ToString()
};

builder.Services.AddSingleton<IAmazonDynamoDB>(x =>
{
    //return new AmazonDynamoDBClient(builder.Configuration["Amazon:AccessKeyId"].ToString(),
    //                                builder.Configuration["Amazon:SecretAccessKey"].ToString(),
    //                                config);
    return new AmazonDynamoDBClient(config);
});

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseAuthorization();

app.MapControllers();

app.Run();

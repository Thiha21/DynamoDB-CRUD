// See https://aka.ms/new-console-template for more information
using DynamoDB;

//Console.WriteLine("Creating Table");
Database db = new Database();
await db.CreateTable();
//Console.WriteLine("Inserting Data");
//db.InsertData();
Console.ReadLine();

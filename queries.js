// queries.js - Node.js version with .env support
require("dotenv").config(); // Load .env
const { MongoClient } = require("mongodb");

const uri = process.env.MONGODB_ATLAS_URI; // MongoDB connection string from .env
const dbName = "plp_bookstore";
const collectionName = "books";

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const books = db.collection(collectionName);

    // ============================
    // Task 2: Basic CRUD Operations
    // ============================

    console.log("\n1. Find all books in Fiction:");
    console.log(await books.find({ genre: "Fiction" }).toArray());

    console.log("\n2. Find books published after 2000:");
    console.log(await books.find({ published_year: { $gt: 2000 } }).toArray());

    console.log("\n3. Find books by George Orwell:");
    console.log(await books.find({ author: "George Orwell" }).toArray());

    console.log('\n4. Update price of "1984" to 15.99:');
    await books.updateOne({ title: "1984" }, { $set: { price: 15.99 } });

    console.log('\n5. Delete book "Moby Dick":');
    await books.deleteOne({ title: "Moby Dick" });

    // ============================
    // Task 3: Advanced Queries
    // ============================

    console.log("\nBooks in stock and published after 2010:");
    console.log(
      await books
        .find({ in_stock: true, published_year: { $gt: 2010 } })
        .toArray()
    );

    console.log("\nProjection (title, author, price):");
    console.log(
      await books
        .find({}, { projection: { title: 1, author: 1, price: 1 } })
        .toArray()
    );

    console.log("\nSort by price ascending:");
    console.log(await books.find().sort({ price: 1 }).toArray());

    console.log("\nSort by price descending:");
    console.log(await books.find().sort({ price: -1 }).toArray());

    console.log("\nPagination - Page 1 (5 books):");
    console.log(await books.find().limit(5).toArray());

    console.log("\nPagination - Page 2 (5 books):");
    console.log(await books.find().skip(5).limit(5).toArray());

    // ============================
    // Task 4: Aggregation Pipelines
    // ============================

    console.log("\nAverage price by genre:");
    console.log(
      await books
        .aggregate([
          { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } },
        ])
        .toArray()
    );

    console.log("\nAuthor with the most books:");
    console.log(
      await books
        .aggregate([
          { $group: { _id: "$author", bookCount: { $sum: 1 } } },
          { $sort: { bookCount: -1 } },
          { $limit: 1 },
        ])
        .toArray()
    );

    console.log("\nBooks grouped by decade:");
    console.log(
      await books
        .aggregate([
          {
            $project: {
              decade: {
                $subtract: [
                  { $divide: ["$published_year", 10] },
                  { $mod: [{ $divide: ["$published_year", 10] }, 1] },
                ],
              },
            },
          },
          { $group: { _id: "$decade", count: { $sum: 1 } } },
          { $sort: { _id: 1 } },
        ])
        .toArray()
    );

    // ============================
    // Task 5: Indexing
    // ============================

    console.log("\nCreating index on title:");
    await books.createIndex({ title: 1 });

    console.log("\nCreating compound index on author & published_year:");
    await books.createIndex({ author: 1, published_year: -1 });

    console.log('\nExplain plan for finding "1984":');
    console.log(await books.find({ title: "1984" }).explain("executionStats"));
  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
    console.log("Connection closed");
  }
}

runQueries();

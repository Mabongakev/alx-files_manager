const Bull = require('bull');
const imageThumbnail = require('image-thumbnail');
const fs = require('fs');
const path = require('path');
const { MongoClient, ObjectId } = require('mongodb');
require('dotenv').config();

// Initialize Bull queue
const fileQueue = new Bull('fileQueue');

// MongoDB setup
const uri = process.env.MONGO_URI;
const client = new MongoClient(uri);
let db;

// Connect to MongoDB
async function connectDB() {
  await client.connect();
  db = client.db(); // Uses default DB if not specified
  console.log('Connected to MongoDB');
}
connectDB().catch(console.error);

// Thumbnail sizes
const sizes = [500, 250, 100];

// Process the queue
fileQueue.process(async (job, done) => {
  try {
    const { fileId, userId } = job.data;

    if (!fileId) throw new Error('Missing fileId');
    if (!userId) throw new Error('Missing userId');

    // Fetch file from DB
    const fileDoc = await db.collection('files').findOne({
      _id: new ObjectId(fileId),
      userId: userId
    });

    if (!fileDoc) throw new Error('File not found');

    const filePath = fileDoc.path; // e.g., '/uploads/myimage.jpg'
    const fileExt = path.extname(filePath);
    const baseName = path.basename(filePath, fileExt);
    const dir = path.dirname(filePath);

    for (const width of sizes) {
      const options = { width };
      const thumbnail = await imageThumbnail(filePath, options);

      const newPath = path.join(dir, `${baseName}_${width}${fileExt}`);
      fs.writeFileSync(newPath, thumbnail);
      console.log(`Saved thumbnail: ${newPath}`);
    }

    done();
  } catch (error) {
    console.error('Error processing job:', error.message);
    done(error);
  }
});

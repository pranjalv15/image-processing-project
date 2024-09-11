const express = require("express");
const multer = require("multer");
const csvParser = require("csv-parser");
const mysql = require("mysql2/promise");
const sharp = require("sharp");
const axios = require("axios");
const { v4: uuidv4 } = require("uuid");
const fs = require("fs");

// Initialize express app and configure multer for file uploads
const app = express();
const upload = multer({ dest: "uploads/" });

// MySQL connection pool
const db = mysql.createPool({
  host: "localhost",
  user: "root",
  password: "MyPass15@",
  database: "image_processing",
});

// Utility function: Validate CSV rows
const validateCSV = (rows) => {
  if (!rows || rows.length === 0)
    return { valid: false, error: "CSV is empty" };
  for (const row of rows) {
    if (!row["Product Name"] || !row["Input Image Urls"]) {
      return {
        valid: false,
        error: "Invalid CSV format: Missing product name or image URLs",
      };
    }
  }
  return { valid: true, error: null };
};

// Utility function: Compress image using Sharp
const compressImage = async (inputUrl) => {
  const response = await axios({ url: inputUrl, responseType: "arraybuffer" });
  const buffer = Buffer.from(response.data);
  const outputBuffer = await sharp(buffer).jpeg({ quality: 50 }).toBuffer();
  return outputBuffer;
};

// Upload CSV and process images
app.post("/upload", upload.single("file"), async (req, res) => {
  const file = req.file;
  const requestId = uuidv4();

  if (!file) return res.status(400).json({ error: "No file uploaded" });

  // Insert request into the database with 'processing' status
  await db.query("INSERT INTO requests (id, status) VALUES (?, ?)", [
    requestId,
    "processing",
  ]);

  const results = [];
  fs.createReadStream(file.path)
    .pipe(csvParser())
    .on("data", (data) => results.push(data))
    .on("end", async () => {
      // Validate CSV format
      const validation = validateCSV(results);
      if (!validation.valid) {
        await db.query("UPDATE requests SET status = ? WHERE id = ?", [
          "failed",
          requestId,
        ]);
        return res.status(400).json({ error: validation.error });
      }

      // Process images and save them to DB
      for (const row of results) {
        const productName = row["Product Name"];
        const inputUrls = row["Input Image Urls"].split(",");

        // Save product to database
        await db.query(
          "INSERT INTO products (request_id, product_name, input_image_urls) VALUES (?, ?, ?)",
          [requestId, productName, row["Input Image Urls"]]
        );

        // Compress images asynchronously
        const outputUrls = [];
        for (const inputUrl of inputUrls) {
          try {
            const outputBuffer = await compressImage(inputUrl);
            const outputPath = `compressed/${uuidv4()}.jpg`;
            fs.writeFileSync(outputPath, outputBuffer);
            outputUrls.push(`http://localhost:3000/${outputPath}`);
          } catch (error) {
            console.error("Image processing failed:", error);
          }
        }

        // Update product with output URLs
        await db.query(
          "UPDATE products SET output_image_urls = ? WHERE request_id = ? AND product_name = ?",
          [outputUrls.join(","), requestId, productName]
        );
      }

      // Update request status as 'completed'
      await db.query("UPDATE requests SET status = ? WHERE id = ?", [
        "completed",
        requestId,
      ]);

      // Trigger webhook
      triggerWebhook(requestId);

      // Respond with request ID
      res.json({ requestId });
    });
});

// Status API to check the processing status
app.get("/status/:requestId", async (req, res) => {
  const { requestId } = req.params;
  const [rows] = await db.query("SELECT * FROM requests WHERE id = ?", [
    requestId,
  ]);

  if (rows.length === 0)
    return res.status(404).json({ error: "Request not found" });

  res.json({ requestId, status: rows[0].status });
});

// Webhook trigger function
const triggerWebhook = async (requestId) => {
  try {
    await axios.post("https://4725-49-207-212-41.ngrok-free.app/api/webhook", {
      requestId,
      status: "completed",
    });
    console.log("Webhook triggered successfully");
  } catch (error) {
    console.error("Failed to trigger webhook:", error);
  }
};

// Static file hosting for compressed images
app.use(express.static("compressed"));

// Start the server
app.listen(3000, () => {
  console.log("Server running on http://localhost:3000");
});

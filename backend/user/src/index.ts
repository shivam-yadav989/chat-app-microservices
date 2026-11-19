import express from "express";
import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDb from "./config/db.js";
import userRoutes from "./routes/user.js";
import { connectRabbitMQ } from "./config/rabbitmq.js";
import cors from "cors";

dotenv.config();

connectDb();

connectRabbitMQ();

interface IKvStore {
  key: string;
  value: string;
  expiresAt: Date;
}

// Persistent MongoDB-backed store with automatic TTL expiry (survives server restarts)
const kvSchema = new mongoose.Schema<IKvStore>({
  key: { type: String, required: true, unique: true },
  value: { type: String, required: true },
  expiresAt: { type: Date, required: true, index: { expires: 0 } },
});

const KvStore =
  (mongoose.models.KvStore as mongoose.Model<IKvStore>) ||
  mongoose.model<IKvStore>("KvStore", kvSchema);

export const redisClient = {
  get: async (key: string): Promise<string | null> => {
    const doc = await KvStore.findOne({ key }).lean<IKvStore>();
    if (!doc) return null;
    if (new Date() > new Date(doc.expiresAt)) {
      await KvStore.deleteOne({ key });
      return null;
    }
    return doc.value;
  },
  set: async (
    key: string,
    value: string,
    options?: { EX?: number }
  ): Promise<string> => {
    const ttlMs = (options?.EX ?? 300) * 1000;
    const expiresAt = new Date(Date.now() + ttlMs);
    await KvStore.findOneAndUpdate(
      { key },
      { key, value, expiresAt },
      { upsert: true, new: true }
    );
    return "OK";
  },
  del: async (key: string): Promise<number> => {
    const res = await KvStore.deleteOne({ key });
    return res.deletedCount ?? 0;
  },
};

const app = express();

app.use(express.json());

app.use(cors());

app.get("/", (_req, res) => {
  res.status(200).send("User service is running");
});

app.use("/api/v1", userRoutes);

const port = process.env.PORT;

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
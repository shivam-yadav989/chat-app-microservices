import express from "express";
import dotenv from "dotenv";
import connectDb from "./config/db.js";
import userRoutes from "./routes/user.js";
import { connectRabbitMQ } from "./config/rabbitmq.js";
import cors from "cors";

dotenv.config();

connectDb();

connectRabbitMQ();

// In-memory store replacing Upstash Redis (zero connection/auth failures)
const memoryStore = new Map<string, { value: string; expiresAt: number }>();

export const redisClient = {
  get: async (key: string) => {
    const entry = memoryStore.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) {
      memoryStore.delete(key);
      return null;
    }
    return entry.value;
  },
  set: async (key: string, value: string, options?: { EX?: number }) => {
    const ttlMs = (options?.EX ?? 300) * 1000;
    memoryStore.set(key, { value, expiresAt: Date.now() + ttlMs });
    return "OK";
  },
  del: async (key: string) => {
    return memoryStore.delete(key) ? 1 : 0;
  },
};

const app = express();

app.use(express.json());

app.use(cors());

app.use("/api/v1", userRoutes);

const port = process.env.PORT;

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
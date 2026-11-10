import "dotenv/config";
import { Pool } from "pg";

// allows api to reuse connection rather than creating brand new
// connection each time
export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10, // 10 db connections as a time max!
});

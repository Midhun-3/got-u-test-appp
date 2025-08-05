import mqtt, { MqttClient } from "mqtt";
import Redis, { Redis as RedisClient } from "ioredis";
import genericPool, { Pool } from "generic-pool";

// ==== Environment Config ====
const {
    MQTT_HOST,
    MQTT_PORT,
    MQTT_USERNAME,
    MQTT_PASSWORD,
    MQTT_TOPIC,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_USERNAME,
    REDIS_PASSWORD,
    REDIS_LIST_LENGTH = "40",
    BATCH_INTERVAL_SECONDS = "10",
} = process.env;

// ==== Redis Pool Setup ====
const redisFactory = {
    create: async (): Promise<RedisClient> => {
        const client = new Redis({
            host: REDIS_HOST,
            port: parseInt(REDIS_PORT || "6379", 10),
            username: REDIS_USERNAME,
            password: REDIS_PASSWORD,
            enableAutoPipelining: true,
            maxRetriesPerRequest: 3,
            connectTimeout: 5000,
        });

        client.on("error", (err: Error) => {
            console.error("Redis client error:", err.message || err);
        });

        return client;
    },
    destroy: async (client: RedisClient): Promise<void> => {
        await client.quit();
    },
    validate: async (): Promise<boolean> => true,
};

const redisPool: Pool<RedisClient> = genericPool.createPool(redisFactory, {
    min: 5,
    max: 50,
    idleTimeoutMillis: 60000,
    evictionRunIntervalMillis: 30000,
    acquireTimeoutMillis: 10000,
    testOnBorrow: false,
});

// ==== MQTT Setup ====
const brokerOptions = {
    host: MQTT_HOST,
    port: parseInt(MQTT_PORT || "8883", 10),
    protocol: "mqtts" as const,
    clientId: `redisworker-${Math.random().toString(36).substring(2, 10)}`,
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
};

console.log("Connecting to MQTT broker:", `${MQTT_HOST}:${MQTT_PORT}, with client ID: ${brokerOptions.clientId}`);
console.log("Using Redis host:", REDIS_HOST, "and port:", REDIS_PORT);
const mqttClient: MqttClient = mqtt.connect(brokerOptions);

mqttClient.on("connect", () => {
    mqttClient.subscribe(MQTT_TOPIC || "", { qos: 1 }, (err) => {
        if (err) console.error("Failed to subscribe:", err);
        else console.log(`Subscribed to topic: ${MQTT_TOPIC}`);
    });
});

mqttClient.on("error", (err: Error) => {
    console.error("MQTT Error:", err.message || err);
});

const messageBuffer: Map<string, any[]> = new Map();

// ==== Batch Processing ====
setInterval(() => {
    if (messageBuffer.size === 0) return;

    const currentBatch = new Map(messageBuffer);
    messageBuffer.clear();

    const resortMap: Map<string, Map<string, any[]>> = new Map();

    for (const [key, messages] of currentBatch.entries()) {
        const [rID] = key.split(":");
        if (!resortMap.has(rID)) resortMap.set(rID, new Map());
        resortMap.get(rID)!.set(key, messages);
    }

    for (const [rID, deviceMap] of resortMap.entries()) {
        const deviceKeys = Array.from(deviceMap.keys());
        const chunkSize = 2;

        for (let i = 0; i < deviceKeys.length; i += chunkSize) {
            const chunk: Map<string, any[]> = new Map();
            const chunkKeys = deviceKeys.slice(i, i + chunkSize);
            chunkKeys.forEach((k) => chunk.set(k, deviceMap.get(k)!));

            processChunk(chunk).catch((err) =>
                console.error(`Error processing chunk for resort ${rID}:`, err)
            );
        }
    }
}, parseInt(BATCH_INTERVAL_SECONDS, 10) * 1000);

// ==== Chunk Processor ====
async function processChunk(chunk: Map<string, any[]>): Promise<void> {
    const client = await redisPool.acquire();

    try {
        const pipeline = client.pipeline();

        for (const [key, messages] of chunk.entries()) {
            const values = messages.map((msg) => JSON.stringify(msg));
            pipeline.lpush(key, ...values);
            pipeline.ltrim(key, 0, parseInt(REDIS_LIST_LENGTH, 10) - 1);
        }

        await pipeline.exec();
    } catch (err) {
        console.error("Redis chunk write failed:", err);
    } finally {
        redisPool.release(client);
    }
}

// ==== MQTT Message Handler ====
mqttClient.on("message", (_, message: Buffer) => {
    try {
        const payload = JSON.parse(message.toString());
        console.log("Received message:", payload);
        const { id, rID } = payload;
        if (!id || !rID) return;

        const key = `${rID}:${id}`;
        if (!messageBuffer.has(key)) messageBuffer.set(key, []);
        messageBuffer.get(key)!.push(payload);
    } catch (err) {
        console.error("Failed to process message:", err);
    }
});

// ==== Shutdown Handler ====
async function shutdown(): Promise<void> {
    if (messageBuffer.size > 0) {
        redisPool.acquire().then(async (client) => {
            try {
                const pipeline = client.pipeline();
                for (const [key, messages] of messageBuffer.entries()) {
                    const values = messages.map((msg) => JSON.stringify(msg));
                    pipeline.lpush(key, ...values);
                    pipeline.ltrim(key, 0, parseInt(REDIS_LIST_LENGTH, 10) - 1);
                }
                await pipeline.exec();
            } catch (err) {
                console.error("Failed to flush on shutdown:", err);
            } finally {
                redisPool.release(client);
            }
        });
    }

    mqttClient.end(() => {
        console.log("🔌 MQTT disconnected.");
    });

    redisPool
        .drain()
        .then(() => redisPool.clear())
        .then(() => {
            console.log("🔌 Redis pool shut down");
            process.exit(0);
        })
        .catch((err) => {
            console.error("Error shutting down Redis pool:", err);
            process.exit(1);
        });
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

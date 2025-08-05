import mqtt from "mqtt";
import { createClient } from "redis";

const brokerOptions = {
  host: "g1220f07.ala.dedicated.aws.emqxcloud.com",
  port: 1883,
  protocol: "mqtt",
  clientId: "redis-writer",
  username: "got-u-dev-user",
  password: "GotU@2025"
};

const topic = "resort_1/user";

const redisClient = createClient({
    username: 'default',
    password: 'QgHsE4Ea65iWyoOia7k99hPgJX0E2Tkf',
    socket: {
        host: 'redis-11649.c8.us-east-1-3.ec2.redns.redis-cloud.com',
        port: 11649
    }
});

redisClient.on("error", (err) => console.error("Redis error:", err));

(async () => {
  await redisClient.connect();
  console.log("✅ Connected to Redis");

  const client = mqtt.connect(brokerOptions);

  client.on("connect", () => {
    console.log(`✅ Connected to MQTT broker, subscribing to '${topic}'`);
    client.subscribe(topic, { qos: 0 });
  });

  client.on("message", async (_, message) => {
    try {
      const payload = JSON.parse(message.toString());
      const { deviceId, resortId } = payload;

      if (!deviceId || !resortId) {
        console.warn("⚠️ Missing deviceId or resortId, skipping");
        return;
      }

      const redisKey = `${resortId}:${deviceId}`;

      await redisClient.lPush(redisKey, JSON.stringify(payload));
      await redisClient.lTrim(redisKey, 0, 39);

      console.log(`💾 Stored data for ${redisKey}`);
    } catch (err) {
      console.error("❌ Error processing message:", err);
    }
  });

  client.on("error", (err) => console.error("❌ MQTT Error:", err));
})();

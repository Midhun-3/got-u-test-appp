import asyncio
import aioredis
import time
import json
from datetime import datetime
from datetime import timezone
from statistics import mean

# Configuration
REDIS_URL = "redis://:password@resort-1-redis-master.resort-1-redis.svc.cluster.local:6379"
NUM_DEVICES = 10000
MAX_PARALLEL_FETCHES = 1000  # Concurrency limit
DEVICE_IDS = [f"device-{str(i+1).zfill(3)}" for i in range(NUM_DEVICES)]
RESORT_ID = "resort_1"

# Fetch one device's Redis history and measure fetch duration
async def fetch_device_history(redis, device_id, now, semaphore):
    redis_key = f"{RESORT_ID}:{device_id}"
    async with semaphore:
        start = time.perf_counter()
        try:
            records = await redis.lrange(redis_key, 0, -1)
        except Exception as e:
            return {"deviceId": device_id, "error": str(e)}

        duration = (time.perf_counter() - start) * 1000  # ms

        return {
            "deviceId": device_id,
            "records": len(records),
            "fetchDurationMs": round(duration, 2),
        }

# Main loop: runs every second
async def analyze(redis):
    semaphore = asyncio.Semaphore(MAX_PARALLEL_FETCHES)
    while True:
        now = datetime.now(timezone.utc)
        tasks = [
            fetch_device_history(redis, device_id, now, semaphore)
            for device_id in DEVICE_IDS
        ]
        results = await asyncio.gather(*tasks)

        print(f"\n📊 Analysis at {now.isoformat()} — {len(results)} devices")
        for r in results:
            if "error" in r:
                print(f"❌ {r['deviceId']}: {r['error']}")
            else:
                print(f"✅ {r['deviceId']}: {r['records']} records, "
                      f"{r['fetchDurationMs']}ms fetch")

        await asyncio.sleep(1)

async def main():
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    print("✅ Connected to Redis")
    await analyze(redis)

if __name__ == "__main__":
    asyncio.run(main())

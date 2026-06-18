#!/usr/bin/env python3
import asyncio
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

import aiohttp

TOPICS = ["orders", "payments", "notifications", "analytics", "logs"]
SOURCES = [f"service-{i}" for i in range(1, 6)]


def generate_event(seq: int) -> dict:
    return {
        "topic": random.choice(TOPICS),
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": random.choice(SOURCES),
        "payload": {"seq": seq, "data": f"payload-{seq}"},
    }


async def send_event(session: aiohttp.ClientSession, url: str, event: dict, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            async with session.post(f"{url}/publish", json=event) as resp:
                return await resp.json()
        except Exception as e:
            print(f"Error sending event: {e}")
            return None


async def main():
    target_url = os.getenv("TARGET_URL", "http://localhost:8080")
    
    if len(sys.argv) > 1:
        target_url = sys.argv[1]
    
    total_unique = int(os.getenv("TOTAL_EVENTS", "25000"))
    if len(sys.argv) > 2:
         total_unique = int(sys.argv[2])
         
    duplicate_pct = float(os.getenv("DUPLICATE_PCT", "0.35"))
    if len(sys.argv) > 3:
         duplicate_pct = float(sys.argv[3])

    workers = int(os.getenv("WORKERS", "50"))

    total_duplicates = int(total_unique * duplicate_pct)
    total_events = total_unique + total_duplicates

    print(f"Target: {target_url}")
    print(f"Generating {total_events} events ({total_unique} unique + {total_duplicates} duplicates)...")

    all_events = []
    for i in range(total_unique):
        all_events.append(generate_event(i))

    for _ in range(total_duplicates):
        dup = random.choice(all_events).copy()
        all_events.append(dup)

    random.shuffle(all_events)

    print(f"Sending {len(all_events)} events with {workers} concurrent workers...")
    start = time.time()

    semaphore = asyncio.Semaphore(workers)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for event in all_events:
            task = asyncio.create_task(send_event(session, target_url, event, semaphore))
            tasks.append(task)
            
        # We process in batches to avoid eating too much memory with gather if events is huge
        batch_size = 5000
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i+batch_size]
            await asyncio.gather(*batch)
            elapsed = time.time() - start
            rate = (i + len(batch)) / elapsed if elapsed > 0 else 0
            print(f"  {i+len(batch)}/{len(all_events)} events sent ({rate:.0f}/s)")

    elapsed = time.time() - start
    print(f"\nSent {len(all_events)} events in {elapsed:.2f}s ({total_events/elapsed:.0f} events/s)")

    print("Waiting 3 seconds for consumers to finish processing...")
    await asyncio.sleep(3)

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{target_url}/stats") as resp:
                stats_data = await resp.json()
                print(f"\nStats: {stats_data}")

            async with session.get(f"{target_url}/events") as resp:
                events = await resp.json()
                print(f"Unique events stored: {len(events)}")
        except Exception as e:
            print(f"Error fetching stats: {e}")


if __name__ == "__main__":
    asyncio.run(main())

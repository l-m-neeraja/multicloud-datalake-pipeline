import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "purchase"]
BASE_URLS = ["/", "/home", "/search", "/category", "/product", "/cart", "/checkout"]


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_events(event_count: int, days: int) -> list[dict]:
    now = datetime.now(timezone.utc)
    users = list(range(1, max(1000, event_count // 20)))
    events: list[dict] = []

    for _ in range(event_count):
        user_id = random.choice(users)
        event_type = random.choices(EVENT_TYPES, weights=[55, 25, 15, 5], k=1)[0]
        ts = now - timedelta(
            days=random.randint(0, max(days - 1, 0)),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        product_id = random.randint(1000, 9999) if event_type in {"product_view", "add_to_cart", "purchase"} else None
        page_url = random.choice(BASE_URLS)
        if product_id is not None and page_url in {"/product", "/cart", "/checkout"}:
            page_url = f"{page_url}/{product_id}"

        events.append(
            {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "event_timestamp": _iso_z(ts),
                "event_type": event_type,
                "page_url": page_url,
                "product_id": product_id,
            }
        )
    return events


def write_ndjson(events: list[dict], output_dir: Path, batch_size: int = 1000) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    chunks = (len(events) + batch_size - 1) // batch_size
    for i in range(chunks):
        part = events[i * batch_size : (i + 1) * batch_size]
        file_path = output_dir / f"events_{i:05d}.json"
        with file_path.open("w", encoding="utf-8") as f:
            for row in part:
                f.write(json.dumps(row))
                f.write("\n")


def main() -> None:
    event_count = int(os.getenv("EVENT_COUNT", "50000"))
    days = int(os.getenv("EVENT_DAYS", "3"))
    raw_output_path = Path(os.getenv("RAW_OUTPUT_PATH", "output/raw"))

    events = generate_events(event_count=event_count, days=days)
    write_ndjson(events, raw_output_path)
    print(f"Generated {len(events)} events into {raw_output_path}")


if __name__ == "__main__":
    main()

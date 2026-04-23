"""
Notification System — with Real Redis
======================================
Real system design components:
  - Redis LIST       : message queue per channel (replaces fake deque)
  - Redis HASH       : user preferences cache (replaces in-memory dict)
  - Redis SET        : deduplication cache (replaces Python set)
  - Redis HASH       : delivery log storage (replaces in-memory list)

Setup (run once before this script):
  1. Install Redis:
       Mac:     brew install redis && brew services start redis
       Ubuntu:  sudo apt install redis-server && sudo service redis start
       Windows: https://github.com/microsoftproject/redis/releases
  2. Install Python client:
       pip install redis

Then run:
  python notification_system_redis.py
"""

from __future__ import annotations

import json
import uuid
import random
from datetime import datetime, time as dtime
from enum import Enum
from typing import Optional
from dataclasses import dataclass, field

import redis

# ---------------------------------------------------------------------------
# Redis connection — change host/port if needed
# ---------------------------------------------------------------------------

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def check_redis():
    try:
        r.ping()
        print("✓ Redis connected\n")
    except redis.ConnectionError:
        print("✗ Redis not running!")
        print("  Start it with:  redis-server")
        print("  Or on Mac:      brew services start redis")
        exit(1)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Channel(Enum):
    PUSH  = "push"
    EMAIL = "email"
    SMS   = "sms"

class Status(Enum):
    DELIVERED = "delivered"
    FAILED    = "failed"
    SKIPPED   = "skipped"


# ---------------------------------------------------------------------------
# Redis Keys (all prefixed to avoid collisions)
# ---------------------------------------------------------------------------

def queue_key(channel: Channel) -> str:
    return f"notif:queue:{channel.value}"          # LIST

def prefs_key(user_id: str) -> str:
    return f"notif:prefs:{user_id}"                # HASH

def dedup_key() -> str:
    return "notif:dedup"                           # SET

def log_key(notification_id: str) -> str:
    return f"notif:log:{notification_id}"          # HASH


# ---------------------------------------------------------------------------
# User Preferences — stored in Redis HASH
# ---------------------------------------------------------------------------

class UserPreferencesStore:
    """
    Each user's prefs live at notif:prefs:<user_id> as a Redis HASH.
    Fields: opted_in, channels, push_token, email_address, phone_number,
            quiet_start, quiet_end
    """

    def set(self, user_id: str, opted_in: bool, channels: list[Channel],
            push_token: str = "", email_address: str = "", phone_number: str = "",
            quiet_start: str = "22:00", quiet_end: str = "08:00"):

        r.hset(prefs_key(user_id), mapping={
            "opted_in":      "1" if opted_in else "0",
            "channels":      ",".join(c.value for c in channels),
            "push_token":    push_token,
            "email_address": email_address,
            "phone_number":  phone_number,
            "quiet_start":   quiet_start,
            "quiet_end":     quiet_end,
        })
        r.expire(prefs_key(user_id), 86400)        # TTL: 24 hours (cache)
        print(f"[PREFS] Saved preferences for {user_id} → Redis HASH {prefs_key(user_id)}")

    def get(self, user_id: str) -> Optional[dict]:
        data = r.hgetall(prefs_key(user_id))
        return data if data else None

    def allowed_channels(self, user_id: str, requested: list[Channel]) -> list[Channel]:
        prefs = self.get(user_id)
        if not prefs or prefs.get("opted_in") == "0":
            return []
        user_channels = set(prefs.get("channels", "").split(","))
        return [ch for ch in requested if ch.value in user_channels]

    def is_quiet_hours(self, user_id: str) -> bool:
        prefs = self.get(user_id)
        if not prefs:
            return False
        now = datetime.now().time()
        qs = dtime(*map(int, prefs["quiet_start"].split(":")))
        qe = dtime(*map(int, prefs["quiet_end"].split(":")))
        if qs > qe:
            return now >= qs or now <= qe
        return qs <= now <= qe


# ---------------------------------------------------------------------------
# Redis Queue — Redis LIST as message queue (LPUSH / BRPOP)
# ---------------------------------------------------------------------------

class RedisQueue:
    """
    Each channel gets its own Redis LIST acting as a queue:
      LPUSH notif:queue:push   <json>   ← enqueue (producer)
      RPOP  notif:queue:push           ← dequeue (consumer)

    In production you'd use BRPOP (blocking pop) in a worker loop.
    """

    def enqueue(self, channel: Channel, payload: dict):
        r.lpush(queue_key(channel), json.dumps(payload))
        print(f"[QUEUE] LPUSH → {queue_key(channel)}  (depth: {r.llen(queue_key(channel))})")

    def dequeue(self, channel: Channel) -> Optional[dict]:
        raw = r.rpop(queue_key(channel))
        return json.loads(raw) if raw else None

    def depth(self, channel: Channel) -> int:
        return r.llen(queue_key(channel))


# ---------------------------------------------------------------------------
# Deduplication — Redis SET with TTL
# ---------------------------------------------------------------------------

class DeduplicationCache:
    """
    Stores idempotency keys in a Redis SET with a 24h TTL.
    Prevents the same notification being sent twice.
    """

    def is_duplicate(self, key: str) -> bool:
        return r.sismember(dedup_key(), key) == 1

    def mark_seen(self, key: str):
        r.sadd(dedup_key(), key)
        r.expire(dedup_key(), 86400)


# ---------------------------------------------------------------------------
# Delivery Log — stored in Redis HASH
# ---------------------------------------------------------------------------

class DeliveryLog:
    """
    Each notification's delivery result stored as:
      notif:log:<notification_id>  →  HASH { push, email, sms: status|provider }
    """

    def record(self, notification_id: str, channel: Channel,
               status: Status, provider: str, error: str = ""):
        field_val = f"{status.value}|{provider}|{error}"
        r.hset(log_key(notification_id), channel.value, field_val)
        r.expire(log_key(notification_id), 86400 * 7)   # keep logs 7 days
        print(f"[LOG]   {log_key(notification_id)} → {channel.value}: {status.value}")

    def get(self, notification_id: str) -> dict:
        return r.hgetall(log_key(notification_id))


# ---------------------------------------------------------------------------
# Channel Workers
# ---------------------------------------------------------------------------

class BaseWorker:
    failure_rate = 0.1

    def deliver(self, payload: dict, prefs: dict) -> tuple[Status, str]:
        raise NotImplementedError


class PushWorker(BaseWorker):
    provider = "FCM"
    def deliver(self, payload, prefs):
        token = prefs.get("push_token", "")
        if not token:
            return Status.FAILED, "No push token"
        if random.random() < self.failure_rate:
            return Status.FAILED, "Provider timeout"
        print(f"  [PUSH]  → {payload['user_id']}: '{payload['title']}'  (token: {token[:8]}…)")
        return Status.DELIVERED, self.provider


class EmailWorker(BaseWorker):
    provider = "SendGrid"
    def deliver(self, payload, prefs):
        email = prefs.get("email_address", "")
        if not email:
            return Status.FAILED, "No email address"
        if random.random() < self.failure_rate:
            return Status.FAILED, "SMTP error"
        print(f"  [EMAIL] → {email}: '{payload['title']}'")
        return Status.DELIVERED, self.provider


class SMSWorker(BaseWorker):
    provider = "Twilio"
    def deliver(self, payload, prefs):
        phone = prefs.get("phone_number", "")
        if not phone:
            return Status.FAILED, "No phone number"
        if random.random() < self.failure_rate:
            return Status.FAILED, "Carrier reject"
        print(f"  [SMS]   → {phone}: '{payload['body'][:60]}'")
        return Status.DELIVERED, self.provider


# ---------------------------------------------------------------------------
# Notification API
# ---------------------------------------------------------------------------

class NotificationAPI:
    def __init__(self, prefs_store: UserPreferencesStore,
                 queue: RedisQueue, dedup: DeduplicationCache, log: DeliveryLog):
        self._prefs = prefs_store
        self._queue = queue
        self._dedup = dedup
        self._log   = log

    def send(self, user_id: str, event: str, title: str, body: str,
             channels: list[Channel], idempotency_key: str = "") -> Optional[str]:

        # 1. Deduplication check (Redis SET)
        key = idempotency_key or f"{user_id}:{event}:{title}"
        if self._dedup.is_duplicate(key):
            print(f"[API] Duplicate skipped: {key}")
            return None
        self._dedup.mark_seen(key)

        # 2. Check user preferences (Redis HASH)
        allowed = self._prefs.allowed_channels(user_id, channels)
        if not allowed:
            print(f"[API] {user_id} — no allowed channels, skipped")
            return None

        # 3. Quiet hours check
        if self._prefs.is_quiet_hours(user_id):
            allowed = [ch for ch in allowed if ch == Channel.EMAIL]
            if not allowed:
                print(f"[API] {user_id} — quiet hours, all suppressed")
                return None

        # 4. Enqueue to Redis LIST (one per channel)
        notification_id = str(uuid.uuid4())
        payload = {
            "id": notification_id,
            "user_id": user_id,
            "event": event,
            "title": title,
            "body": body,
            "created_at": datetime.now().isoformat(),
        }
        for ch in allowed:
            self._queue.enqueue(ch, payload)

        print(f"[API] Notification {notification_id[:8]}… queued for {user_id} on {[c.value for c in allowed]}\n")
        return notification_id


# ---------------------------------------------------------------------------
# Scheduler — drains Redis queues
# ---------------------------------------------------------------------------

class NotificationScheduler:
    def __init__(self, queue: RedisQueue, prefs_store: UserPreferencesStore, log: DeliveryLog):
        self._queue   = queue
        self._prefs   = prefs_store
        self._log     = log
        self._workers = {
            Channel.PUSH:  PushWorker(),
            Channel.EMAIL: EmailWorker(),
            Channel.SMS:   SMSWorker(),
        }

    def process_all(self):
        for channel in Channel:
            while self._queue.depth(channel) > 0:
                payload = self._queue.dequeue(channel)
                if payload:
                    self._dispatch(payload, channel)

    def _dispatch(self, payload: dict, channel: Channel):
        prefs = self._prefs.get(payload["user_id"])
        worker = self._workers[channel]

        if not prefs:
            self._log.record(payload["id"], channel, Status.FAILED, "n/a", "No prefs found")
            return

        status, info = worker.deliver(payload, prefs)
        provider = info if status == Status.DELIVERED else "n/a"
        error    = info if status == Status.FAILED    else ""
        self._log.record(payload["id"], channel, status, provider, error)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    random.seed(42)
    check_redis()

    # Flush old demo keys (clean slate)
    for k in r.scan_iter("notif:*"):
        r.delete(k)

    prefs_store = UserPreferencesStore()
    queue       = RedisQueue()
    dedup       = DeduplicationCache()
    log         = DeliveryLog()
    api         = NotificationAPI(prefs_store, queue, dedup, log)
    scheduler   = NotificationScheduler(queue, prefs_store, log)

    # --- Register users in Redis ---
    print("=" * 60)
    print("Registering users in Redis...")
    print("=" * 60)
    prefs_store.set("user_alice", True,
                    [Channel.PUSH, Channel.EMAIL, Channel.SMS],
                    push_token="tok_alice_abc123",
                    email_address="alice@example.com",
                    phone_number="+14155550101")

    prefs_store.set("user_bob", True,
                    [Channel.EMAIL],           # email only
                    email_address="bob@example.com")

    prefs_store.set("user_carol", False,       # opted out
                    [],
                    email_address="carol@example.com")

    # --- Send notifications ---
    print("\n" + "=" * 60)
    print("Sending notifications...")
    print("=" * 60 + "\n")

    n1 = api.send("user_alice", "order.shipped", "Your order shipped!",
                  "Order #1042 is on the way.",
                  [Channel.PUSH, Channel.EMAIL, Channel.SMS])

    api.send("user_alice", "social.like", "Someone liked your post",
             "Alice liked your photo.",
             [Channel.PUSH])

    api.send("user_alice", "social.like", "Someone liked your post",  # duplicate
             "Alice liked your photo.",
             [Channel.PUSH],
             idempotency_key="like_alice_post_99")

    api.send("user_bob", "order.shipped", "Your order shipped!",
             "Order #2031 is on the way.",
             [Channel.PUSH, Channel.EMAIL, Channel.SMS])

    api.send("user_carol", "auth.login", "New login detected",
             "Someone signed in from a new device.",
             [Channel.EMAIL])

    # --- Process queues ---
    print("=" * 60)
    print("Processing Redis queues...")
    print("=" * 60 + "\n")
    scheduler.process_all()

    # --- Show delivery log from Redis ---
    print("\n" + "=" * 60)
    print("Delivery log from Redis:")
    print("=" * 60)
    if n1:
        records = log.get(n1)
        for channel, value in records.items():
            status, provider, error = (value + "||").split("|")[:3]
            print(f"  {channel:6s} | {status:10s} | {provider} {('← ' + error) if error else ''}")

    # Show raw Redis keys created
    print("\nRedis keys created during this run:")
    for k in sorted(r.scan_iter("notif:*")):
        print(f"  {k}  ({r.type(k)})")
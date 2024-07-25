from fastapi import FastAPI

class EventStore:
    def __init__(self, max_events_per_user=10):
        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        if user_id in self.events:
            user_events = self.events[user_id]
        else:
            user_events = []

        self.events[user_id] = [item_id] + user_events[:self.max_events_per_user]

    def get(self, user_id, k):
        if user_id in self.events:
            user_events = self.events[user_id]
            return user_events[:k]
        else:
            return []

events_store = EventStore()

app = FastAPI(title="events")

@app.post("/put")
async def put(user_id: int, item_id: int):
    events_store.put(user_id, item_id)
    return {"result": "ok"}

@app.post("/get")
async def get(user_id: int, k: int = 10):
    events = events_store.get(user_id, k)
    return {"user_id": user_id, "events": events}

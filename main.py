"""
Genie Code Workshop — Instruction App (Inchcape edition)
FastAPI backend: serves track content + static frontend
"""

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Genie Code Workshop — Inchcape", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load track data at startup
DATA_PATH = Path(__file__).parent / "data" / "tracks.json"
with open(DATA_PATH, "r", encoding="utf-8") as f:
    TRACKS_DATA = json.load(f)

TRACKS_BY_ID = {t["id"]: t for t in TRACKS_DATA["tracks"]}


@app.get("/api/tracks")
def list_tracks():
    """Return summary of all tracks for landing page cards."""
    return [
        {
            "id": t["id"],
            "title": t["title"],
            "subtitle": t["subtitle"],
            "description": t["description"],
            "icon": t["icon"],
            "color": t["color"],
            "estimatedMinutes": t["estimatedMinutes"],
            "stepCount": len(t["steps"]),
            "participantCount": t.get("participantCount", 0),
        }
        for t in TRACKS_DATA["tracks"]
    ]


@app.get("/api/tracks/{track_id}")
def get_track(track_id: str):
    """Return full track data including steps and FAQ."""
    track = TRACKS_BY_ID.get(track_id)
    if not track:
        raise HTTPException(status_code=404, detail=f"Track '{track_id}' not found")
    return track


# Serve static frontend (must be mounted LAST)
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")

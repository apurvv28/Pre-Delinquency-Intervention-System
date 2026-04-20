"""
PIE WebSocket Connection Manager
---------------------------------
Manages WebSocket connections and broadcasts real-time transaction
events to all connected clients.
"""

import asyncio
import json
from typing import Set

from fastapi import WebSocket, WebSocketDisconnect


class WebSocketConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        """Accept a WebSocket connection."""
        await websocket.accept()
        async with self.lock:
            self.active_connections.add(websocket)
        print(f"[WS] Client connected. Total: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection."""
        async with self.lock:
            self.active_connections.discard(websocket)
        print(f"[WS] Client disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, event_type: str, data: dict):
        """Broadcast an event to all connected clients."""
        if not self.active_connections:
            return

        message = {
            "type": event_type,
            "data": data,
        }
        
        payload = json.dumps(message)
        disconnected = set()
        
        async with self.lock:
            for connection in self.active_connections:
                try:
                    await connection.send_text(payload)
                except Exception as e:
                    print(f"[WS] Send failed: {e}")
                    disconnected.add(connection)
        
        # Clean up disconnected connections
        async with self.lock:
            self.active_connections -= disconnected

    async def broadcast_transaction(self, transaction: dict):
        """Broadcast a transaction event."""
        await self.broadcast("transaction", transaction)

    async def broadcast_score_update(self, customer_id: str, score: float, bucket: str):
        """Broadcast a risk score update."""
        await self.broadcast("risk_score_update", {
            "customer_id": customer_id,
            "risk_score": score,
            "risk_bucket": bucket,
        })

    async def broadcast_model_output(self, event_type: str, data: dict):
        """Broadcast raw model output (LightGBM + XGBoost fusion)."""
        await self.broadcast("model_output", {
            "event_type": event_type,
            "data": data,
        })


# Global connection manager instance
manager = WebSocketConnectionManager()

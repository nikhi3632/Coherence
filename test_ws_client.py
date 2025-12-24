"""
WebSocket Test Client

Run the server first:
    python main.py stream

Then run this client:
    python test_ws_client.py
"""

import asyncio
import json
import websockets


async def main():
    uri = "ws://localhost:8765"
    print(f"Connecting to {uri}...")

    async with websockets.connect(uri) as ws:
        print("Connected!\n")

        # Test events
        events = [
            {"timestamp": "2025-12-24T10:00:00Z", "source": "slack", "text": "Team standup notes"},
            {"timestamp": "2025-12-24T10:01:00Z", "source": "system", "text": "Error: connection timeout"},
            {"timestamp": "2025-12-24T10:02:00Z", "source": "email", "text": "Client meeting request"},
            {"timestamp": "2025-12-24T10:03:00Z", "source": "slack", "text": "Sprint review reminder"},
            {"timestamp": "2025-12-24T10:04:00Z", "source": "system", "text": "Alert: high CPU usage"},
        ]

        for event in events:
            await ws.send(json.dumps(event))
            print(f"Sent: [{event['source']}] {event['text']}")
            await asyncio.sleep(0.3)  # Small delay between events

        print("\nWaiting for server to flush...")
        await asyncio.sleep(2)

        print("Done!")


if __name__ == "__main__":
    asyncio.run(main())


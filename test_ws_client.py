"""
WebSocket Test Client

Run the server first:
    python main.py stream

Then run this client:
    python test_ws_client.py           # run all test scenarios once
    python test_ws_client.py loop      # continuous loop (Ctrl+C to stop)
"""

import asyncio
import json
import sys
from datetime import datetime, timezone
import websockets


def ts():
    """Current timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# TEST SCENARIOS - Different event patterns to exercise the system
# =============================================================================

TEST_SCENARIOS = [
    {
        "name": "Happy Path - Mixed Sources",
        "description": "Valid events from different sources, tests classification",
        "events": [
            {"timestamp": ts(), "source": "slack", "text": "Team standup notes"},
            {"timestamp": ts(), "source": "email", "text": "Client meeting request"},
            {
                "timestamp": ts(),
                "source": "system",
                "text": "Error: connection timeout",
            },
        ],
    },
    {
        "name": "All Event Types",
        "description": "One of each classification type",
        "events": [
            {"timestamp": ts(), "source": "slack", "text": "Sprint review tomorrow"},
            {"timestamp": ts(), "source": "email", "text": "Customer complaint"},
            {"timestamp": ts(), "source": "system", "text": "Alert: disk full"},
            {"timestamp": ts(), "source": "slack", "text": "Random message"},  # misc
        ],
    },
    {
        "name": "Burst - Trigger Buffer Flush",
        "description": "10+ events to trigger flush_threshold",
        "events": [
            {"timestamp": ts(), "source": "slack", "text": f"Burst event {i}"}
            for i in range(12)
        ],
    },
    {
        "name": "Team Updates Only",
        "description": "Multiple team_update classifications",
        "events": [
            {"timestamp": ts(), "source": "slack", "text": "Team update: new process"},
            {"timestamp": ts(), "source": "slack", "text": "Deadline moved to Friday"},
            {
                "timestamp": ts(),
                "source": "slack",
                "text": "Sprint retrospective notes",
            },
            {
                "timestamp": ts(),
                "source": "slack",
                "text": "Standup: blockers discussed",
            },
        ],
    },
    {
        "name": "External Communications",
        "description": "Email-based external_comm events",
        "events": [
            {"timestamp": ts(), "source": "email", "text": "Client escalation"},
            {"timestamp": ts(), "source": "email", "text": "Customer feedback survey"},
            {
                "timestamp": ts(),
                "source": "email",
                "text": "Partner integration update",
            },
        ],
    },
    {
        "name": "System Alerts",
        "description": "System monitoring alerts",
        "events": [
            {"timestamp": ts(), "source": "system", "text": "Error: database timeout"},
            {"timestamp": ts(), "source": "system", "text": "Alert: memory threshold"},
            {"timestamp": ts(), "source": "system", "text": "Failed: backup job"},
            {"timestamp": ts(), "source": "system", "text": "Timeout: API gateway"},
        ],
    },
    {
        "name": "Batch Send",
        "description": "Send multiple events as JSON array (tests batch parsing)",
        "batch": True,
        "events": [
            {"timestamp": ts(), "source": "slack", "text": "Batch item 1 - review"},
            {"timestamp": ts(), "source": "email", "text": "Batch item 2 - client"},
            {"timestamp": ts(), "source": "system", "text": "Batch item 3 - error"},
        ],
    },
    {
        "name": "Rapid Fire",
        "description": "Fast succession to test backpressure handling",
        "delay": 0.05,  # 50ms between events
        "events": [
            {"timestamp": ts(), "source": "slack", "text": f"Rapid {i}"}
            for i in range(20)
        ],
    },
    {
        "name": "Misc/Unclassified",
        "description": "Events that should fall through to 'misc' type",
        "events": [
            {"timestamp": ts(), "source": "slack", "text": "Hey, what's up?"},
            {"timestamp": ts(), "source": "slack", "text": "Lunch at noon?"},
            {"timestamp": ts(), "source": "other", "text": "Random notification"},
        ],
    },
    {
        "name": "Large Text Payload",
        "description": "Events with longer text content",
        "events": [
            {
                "timestamp": ts(),
                "source": "email",
                "text": "Client escalation: The customer reported that the API has been returning 500 errors intermittently since yesterday morning. They need urgent resolution.",
            },
            {
                "timestamp": ts(),
                "source": "slack",
                "text": "Team update: Sprint planning complete. We've committed to 8 stories this sprint, focusing on the authentication overhaul and dashboard redesign.",
            },
        ],
    },
]


async def run_scenario(ws, scenario, scenario_num, total):
    """Run a single test scenario."""
    name = scenario["name"]
    desc = scenario.get("description", "")
    events = scenario["events"]
    delay = scenario.get("delay", 0.3)
    is_batch = scenario.get("batch", False)

    print(f"\n{'=' * 60}")
    print(f"SCENARIO {scenario_num}/{total}: {name}")
    print(f"Description: {desc}")
    print(f"Events: {len(events)}")
    print("=" * 60)

    # Regenerate timestamps to be current
    for event in events:
        event["timestamp"] = ts()

    if is_batch:
        # Send all events as a single JSON array
        await ws.send(json.dumps(events))
        print(f"  → Sent batch of {len(events)} events")
    else:
        # Send events individually
        for i, event in enumerate(events):
            await ws.send(json.dumps(event))
            print(
                f"  [{i + 1}/{len(events)}] [{event['source']}] {event['text'][:50]}..."
            )
            await asyncio.sleep(delay)

    # Wait for flush
    print("  → Waiting for flush...")
    await asyncio.sleep(1.5)
    print("  ✓ Scenario complete")


async def main():
    loop_mode = len(sys.argv) > 1 and sys.argv[1] == "loop"
    uri = "ws://localhost:8765"

    print(f"Connecting to {uri}...")
    print(f"Mode: {'LOOP (Ctrl+C to stop)' if loop_mode else 'ONE-SHOT'}")
    print(f"Scenarios: {len(TEST_SCENARIOS)}\n")

    async with websockets.connect(uri) as ws:
        print("Connected!")

        try:
            cycle = 0
            while True:
                cycle += 1
                if loop_mode:
                    print(f"\n{'#' * 60}")
                    print(f"# CYCLE {cycle}")
                    print("#" * 60)

                for i, scenario in enumerate(TEST_SCENARIOS, 1):
                    await run_scenario(ws, scenario, i, len(TEST_SCENARIOS))

                if not loop_mode:
                    break

                print(f"\n--- Cycle {cycle} complete. Starting next in 3s... ---")
                await asyncio.sleep(3)

        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\n\n[CLIENT] Interrupted by user.")

    print("[CLIENT] Connection closed.")
    print("[CLIENT] Done!")


if __name__ == "__main__":
    asyncio.run(main())

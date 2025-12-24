//Demo Purpose Only

#----------- Task 1 -----------
This prototype takes a list of raw text events (Slack, Email, System, etc.), sorts them by timestamp, applies simple keyword-based classification, and outputs a normalized timeline structure.

Pipeline Steps:

1. Sort events by timestamp

2. Classify events using simple keyword rules

3. Normalize output into a consistent structure:


#--------- Task 2 -----------------

Step by step explanation of Step 2:

I simulated an environment where we ingest raw events (logs) and then route them based on the event type. The idea is that in the future, if we add a messaging system (for example Kafka or a queue), events like system_alert could be routed to a dedicated alerting or monitoring service without changing the ingestion logic.

- For logging and traceability, I added logs at each major stage of the flow:

    1. Ingestion enter: raw events are received and validated against the expected schema.

    2. Ingestion processed: Part 1 logic runs, where events are cleaned, classified, normalized into a stable format, and sorted chronologically.

    3. Routing loop: each normalized event enters the routing phase, is dispatched via the router (logged as routing dispatch), and then exits routing. This continues until all ingested events are processed.

- Some Information about the changes in project structure:

    1. To ensure the module can accept multiple event types without changing core logic, I separated event-type rules into an event_types.json file. This allows new event types or sources to be handled in the future by updating configuration rather than modifying ingestion or routing code.

    2. I created a file integrationBoundary.py to act as a clear interface layer between ingestion and routing. This boundary uses the ingestion pipeline from the Part 1 eventsIngestion.py module and cleanly hands off the normalized events to the router.

    3. Finally, I added a main.py file to wire everything together, define the router implementation, and run end-to-end tests of the ingestion-to-routing flow.

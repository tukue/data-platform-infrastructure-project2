# Event Streaming

Use [event-catalog.json](event-catalog.json) as the source of truth for the
events this repository routes through Amazon MSK. It is intentionally small and
machine-readable so engineers can identify an event before tracing the CDK and
consumer implementation.

## Current Architecture

```text
CSV processor ECS task -> Amazon MSK topic -> Kafka consumer ECS service
                                            -> DynamoDB ProcessingEvents
                                            -> Glue/Athena
```

The processor image is supplied through `processorImage`; its source is outside
this repository. The in-repository consumer in `consumer/app.py` subscribes to
both configured topics, stores Kafka metadata and the JSON payload in DynamoDB,
then commits the offset after a successful write.

## Event Inventory

| Event and default topic | Producer | Consumer | Schema | Owner |
|---|---|---|---|---|
| `processing.succeeded` | CSV processor ECS task | Kafka consumer ECS service (`data-processing-consumer`) | JSON; not versioned in this repository | Unassigned |
| `processing.failed` | CSV processor ECS task | Kafka consumer ECS service (`data-processing-consumer`) | JSON; not versioned in this repository | Unassigned |

Topic names are dot-separated, outcome-oriented identifiers. The deployed topic
names can be overridden through CDK context: `mskSuccessTopic` and
`mskFailureTopic`, or environment variables: `MSK_SUCCESS_TOPIC` and
`MSK_FAILURE_TOPIC`.

## Discover or Add an Event

1. Find the event ID in `docs/event-catalog.json` to identify its default topic,
   producer, consumer, schema state, and ownership state.
2. Confirm the deployed topic override in the stack configuration when it differs
   from the catalog default.
3. When adding an event, add one catalog entry with a unique `id`, a topic,
   producer, at least one consumer, schema status, and owner status.
4. Use `npm run validate:event-catalog` before opening a pull request. CI runs
   the same check and verifies duplicate IDs/topics, required metadata, owner
   state, and any declared schema reference.

The next step after this MVP is to define versioned producer schemas. A schema
registry or AsyncAPI portal is intentionally outside the current scope.

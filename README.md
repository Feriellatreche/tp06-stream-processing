### 8.1 Flink SQL Questions

**Question 1: Which field defines event time?**

In the original SQL example, the `eventTime` field represents the event time.

In our implementation, the Kafka JSON timestamp is initially read as a `STRING` because the incoming timestamps contain timezone information and fractional seconds.

We created a computed timestamp attribute called `event_ts` using `TO_TIMESTAMP()` and `SUBSTRING()`.

Therefore, `event_ts` is the field used by Flink for event-time processing.

**Question 2: What does the watermark declaration mean?**

The watermark is defined as:

```sql
WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
```

It indicates that Flink allows a 30-second delay when processing events based on event time.

The watermark helps Flink determine when it can consider event-time windows sufficiently complete and process their results.

It also helps manage out-of-order events. However, it does not guarantee that every late event will be included.

**Question 3: What is the duration of each tumbling window?**

The tumbling window duration is one minute:

```sql
INTERVAL '1' MINUTE
```

A tumbling window divides the event stream into fixed, consecutive, non-overlapping one-minute intervals.

Each event belongs to one window for this window definition.

**Question 4: Why are `window_start` and `window_end` useful in the output?**

`window_start` indicates the beginning of each time window, while `window_end` indicates its ending time.

These fields identify the exact time interval associated with each aggregation result.

In our implementation, they help identify the one-minute period during which student check-in events were counted for each department.

For example, the results for the interval from `22:39:00` to `22:40:00` showed:

- Mathematics: 3 check-ins.
- Computer Science: 3 check-ins.
- Physics: 2 check-ins.

These timestamps make it easier to understand and compare campus activity over time.
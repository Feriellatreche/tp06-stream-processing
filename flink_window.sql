
SET 'execution.runtime-mode' = 'streaming';

SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE TABLE campus_events (
    eventId INT,
    studentId STRING,
    eventType STRING,
    department STRING,
    eventTime STRING,

    event_ts AS TO_TIMESTAMP(
        SUBSTRING(eventTime, 1, 19),
        'yyyy-MM-dd''T''HH:mm:ss'
    ),

    WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'campus-events',
    'properties.bootstrap.servers' = 'tp05-kafka:29092',
    'properties.group.id' = 'tp06-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

SELECT
    window_start,
    window_end,
    department,
    COUNT(*) AS checkins
FROM TABLE(
    TUMBLE(
        TABLE campus_events,
        DESCRIPTOR(event_ts),
        INTERVAL '1' MINUTE
    )
)
WHERE eventType = 'check-in'
GROUP BY
    window_start,
    window_end,
    department;

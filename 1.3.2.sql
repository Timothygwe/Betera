CREATE TABLE bet (
    bet_id VARCHAR,
    event_id VARCHAR,
    odd DECIMAL
);

INSERT INTO bet
VALUES
    ('bet_196', 'event-1953', 3.03),
    ('bet_196', 'event-1954', 2.13),
    ('bet_196', 'event-1955', 3.13),
    ('bet_196', 'event-1956', 3.43),
    ('bet_197', 'event-1957', 3.93),
    ('bet_197', 'event-1958', 3.73),
    ('bet_197', 'event-1959', 3.53);

WITH tbl AS (
    SELECT bet_id,
           ROUND(EXP(SUM(LOG(odd))),2) AS bet_odd
    FROM bet
    GROUP BY bet_id
)
SELECT b.bet_id, b.event_id, b.odd, t.bet_odd
FROM bet b
INNER JOIN tbl t ON b.bet_id = t.bet_id;

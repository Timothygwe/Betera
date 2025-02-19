
SELECT DISTINCT player_id
FROM bets b
INNER JOIN events e on b.event_id = e.event_id
WHERE
    event_stage = 'Prematch' AND
    sport = 'E-Sports' AND
    bet_size >= 10 AND
    accepted_odd >= 1.5 AND
    settlement_time <= '2022-03-15 12:00' AND
    bet_type != 'System' AND
    item_result NOT IN ('Return', 'Cashout', 'FreeBet');

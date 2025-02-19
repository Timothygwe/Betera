SELECT player_id
FROM bets
JOIN  ON bets.event_id = events.event_id
WHERE
    create_time >= '2022-03-14 12:00:00'
    AND event_stage = 'Prematch'
    AND sport = 'E-Sports'
    AND bet_size >= 10
    AND accepted_odd >= 1.5
    AND settlement_time <= '2022-03-15 12:00:00'
    AND bet_type <> 'System'
    AND item_result NOT IN ('Return', 'Cashout', 'FreeBet');

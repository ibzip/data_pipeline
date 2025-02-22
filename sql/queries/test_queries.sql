-- 1. Who are the top 10 users with respect to number of songs listened to?

SELECT du.user_id, COUNT(fl.listen_sk) AS num_listens
FROM fact_listen fl
JOIN dim_user du ON fl.user_sk = du.user_sk
GROUP BY du.user_id
ORDER BY num_listens DESC
LIMIT 10;

-- 2. How Many Users Listened to a Song on March 1, 2019?

SELECT COUNT(DISTINCT du.user_id) AS num_users
FROM fact_listen fl
JOIN dim_user du ON fl.user_sk = du.user_sk
WHERE CAST(fl.listened_at AS DATE) = '2019-03-01';


-- 3. First Song Listened to by Each User

WITH ranked_listens AS (
    SELECT
        du.user_id,
        dt.track_name,
        dt.artist_name,
        fl.listened_at,
        ROW_NUMBER() OVER (PARTITION BY du.user_id ORDER BY fl.listened_at ASC) AS rank
    FROM fact_listen fl
    JOIN dim_user du ON fl.user_sk = du.user_sk
    JOIN dim_track dt ON fl.track_sk = dt.track_sk
)
SELECT user_id, track_name, artist_name, listened_at
FROM ranked_listens
WHERE rank = 1;

-- 4. Top 3 Days with Most Listens per User

WITH daily_listens AS (
    SELECT
        du.user_id,
        CAST(fl.listened_at AS DATE) AS listen_date,
        COUNT(*) AS num_listens
    FROM fact_listen fl
    JOIN dim_user du ON fl.user_sk = du.user_sk
    GROUP BY du.user_id, CAST(fl.listened_at AS DATE)
),
ranked_days AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY num_listens DESC, listen_date ASC) AS rank
    FROM daily_listens
)
SELECT user_id, num_listens, listen_date
FROM ranked_days
WHERE rank <= 3
ORDER BY user_id, num_listens DESC;


-- 5. Daily Active Users and Percentage of Active Users

WITH user_activity AS (
    SELECT
        DATE(fl.listened_at) AS listen_date,
        du.user_id
    FROM fact_listen fl
    JOIN dim_user du ON fl.user_sk = du.user_sk
),
rolling_activity AS (
    SELECT
        ua1.listen_date,
        ua2.user_id
    FROM user_activity ua1
    JOIN user_activity ua2
        ON ua2.listen_date BETWEEN ua1.listen_date - INTERVAL '6 days' AND ua1.listen_date
    GROUP BY ua1.listen_date, ua2.user_id
),
daily_active_users AS (
    SELECT
        listen_date,
        COUNT(DISTINCT user_id) AS number_active_users
    FROM rolling_activity
    GROUP BY listen_date
),
total_users AS (
    SELECT COUNT(*) AS total_users FROM dim_user
)
SELECT
    dau.listen_date,
    dau.number_active_users,
    ROUND((dau.number_active_users * 100.0) / tu.total_users, 2) AS percentage_active_users
FROM daily_active_users dau
CROSS JOIN total_users tu
ORDER BY listen_date;



WITH today_data AS (
    SELECT id, feedbacks
    FROM public.count_feedbacks
    WHERE date = %s
),
yesterday_data AS (
    SELECT id, feedbacks
    FROM public.count_feedbacks
    WHERE date = %s)
INSERT INTO dynamic_feedbacks (date, id, new_feedbacks)
SELECT CURRENT_DATE, t.id, t.feedbacks - y.feedbacks AS new_feedbacks
FROM today_data t inner join yesterday_data y using(id)
WHERE t.feedbacks - y.feedbacks > 0;
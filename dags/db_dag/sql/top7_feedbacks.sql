select date, new_feedbacks, STRING_AGG(id::varchar(50), ', ' order by id) id
from (
    select id, date, new_feedbacks, max(new_feedbacks) over(partition by date) as new_feedbacks_max
    from public.dynamic_feedbacks
    where date >= %s - INTERVAL '7 DAY') a
where new_feedbacks = new_feedbacks_max
group by date, new_feedbacks
order by date
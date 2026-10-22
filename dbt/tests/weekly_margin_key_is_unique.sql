-- Grain of weekly_margin: one row per iso_week x city x category.
select iso_week, city, category, count(*) as n
from {{ ref('weekly_margin') }}
group by iso_week, city, category
having count(*) > 1

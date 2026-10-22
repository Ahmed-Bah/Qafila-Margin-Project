-- margin = costed revenue - cost; costed revenue cannot exceed revenue; uncosted lines cannot exceed lines.
select *
from {{ ref('weekly_margin') }}
where abs(costed_revenue_sar - cost_sar - margin_sar) > 0.011
   or costed_revenue_sar > revenue_sar + 0.01
   or uncosted_lines > lines
   or uncosted_lines < 0
   or (uncosted_lines = lines and (costed_revenue_sar <> 0 or cost_sar <> 0))

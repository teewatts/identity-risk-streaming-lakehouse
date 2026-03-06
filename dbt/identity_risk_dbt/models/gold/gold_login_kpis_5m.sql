with base as (
  select
    event_ts,
    dateadd(
      minute,
      - mod(date_part('minute', event_ts), 5),
      date_trunc('minute', event_ts)
    ) as window_start_5m,
    login_successful,
    rtt_ms,
    is_attack_ip,
    is_account_takeover
  from {{ ref('login_events_silver') }}
)

select
  window_start_5m,
  dateadd(minute, 5, window_start_5m) as window_end_5m,
  count(*) as login_attempts,
  sum(iff(login_successful = false, 1, 0)) as failed_logins,
  sum(iff(login_successful = true,  1, 0)) as successful_logins,
  (successful_logins / nullif(login_attempts, 0)) as success_rate,
  avg(rtt_ms) as avg_rtt_ms,
  sum(iff(is_attack_ip = true, 1, 0)) as attack_ip_events,
  sum(iff(is_account_takeover = true, 1, 0)) as account_takeover_events
from base
group by window_start_5m
order by window_start_5m
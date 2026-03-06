select
  user_id,
  count(*) as login_attempts,
  sum(iff(login_successful = false, 1, 0)) as failed_logins,
  sum(iff(login_successful = true,  1, 0)) as successful_logins,
  (failed_logins / nullif(login_attempts, 0)) as failure_rate,
  sum(iff(is_attack_ip = true, 1, 0)) as attack_ip_events,
  sum(iff(is_account_takeover = true, 1, 0)) as account_takeover_events,
  avg(rtt_ms) as avg_rtt_ms,
  min(event_ts) as first_seen_ts,
  max(event_ts) as last_seen_ts,
  (failed_logins * 1.0) +
  (attack_ip_events * 3.0) +
  (account_takeover_events * 5.0) as risk_score
from {{ ref('login_events_silver') }}
group by user_id
order by risk_score desc, failed_logins desc
-- 1) KPI trend over time
SELECT
  window_start,
  window_end,
  login_attempts,
  failed_logins,
  successful_logins,
  success_rate,
  avg_rtt_ms,
  attack_ip_events,
  account_takeover_events
FROM gold_login_kpis_5m
ORDER BY window_start;

-- 2) Top risky IP windows
SELECT
  window_start,
  window_end,
  ip_address,
  login_attempts,
  failed_logins,
  successful_logins,
  failure_rate,
  attack_ip_events,
  account_takeover_events
FROM gold_fail_spikes_by_ip_5m
WHERE failed_logins >= 3
ORDER BY failed_logins DESC, failure_rate DESC;

-- 3) Top risky users
SELECT
  user_id,
  login_attempts,
  failed_logins,
  successful_logins,
  failure_rate,
  attack_ip_events,
  account_takeover_events,
  avg_rtt_ms,
  risk_score,
  first_seen_ts,
  last_seen_ts
FROM gold_risk_signals_by_user
ORDER BY risk_score DESC, failed_logins DESC;

-- 4) Country-level attack activity
SELECT
  country,
  COUNT(*) AS login_attempts,
  SUM(CASE WHEN is_attack_ip THEN 1 ELSE 0 END) AS attack_ip_events,
  SUM(CASE WHEN is_account_takeover THEN 1 ELSE 0 END) AS account_takeover_events
FROM silver_login_events
GROUP BY country
ORDER BY attack_ip_events DESC, account_takeover_events DESC;

-- 5) Quarantine summary
SELECT
  quarantine_reason,
  COUNT(*) AS quarantined_rows
FROM silver_login_events_quarantine
GROUP BY quarantine_reason
ORDER BY quarantined_rows DESC;
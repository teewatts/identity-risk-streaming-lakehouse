with raw as (
  select * from {{ source('identity_risk', 'LOGIN_EVENTS_RAW') }}
)

select
  try_to_timestamp_ntz(login_timestamp) as event_ts,
  user_id,
  ip_address,
  country,
  region,
  city,
  asn,
  user_agent_string,
  browser_name_version,
  os_name_version,
  device_type,
  try_to_number(rtt_ms) as rtt_ms,
  try_to_boolean(login_successful) as login_successful,
  try_to_boolean(is_attack_ip) as is_attack_ip,
  try_to_boolean(is_account_takeover) as is_account_takeover
from raw
where user_id is not null
  and ip_address is not null
  and try_to_timestamp_ntz(login_timestamp) is not null
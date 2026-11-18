package limiter

import "github.com/redis/go-redis/v9"

// All scripts work in microseconds and return
// {allowed (0|1), remaining, retry_after_us, reset_after_us}.
// Remaining is returned as a string because Redis truncates Lua floats.

// tokenBucketScript keeps {tokens, ts} in a hash and refills lazily.
// ARGV: capacity, refill rate (tokens per second), cost.
var tokenBucketScript = redis.NewScript(`
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local cost     = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + elapsed * rate / 1000000)

local allowed = 0
local retry = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry = math.ceil((cost - tokens) * 1000000 / rate)
end

redis.call('HSET', key, 'tokens', string.format('%.6f', tokens), 'ts', now)
redis.call('PEXPIRE', key, math.ceil(capacity * 1000 / rate) + 1000)

local reset = math.ceil((capacity - tokens) * 1000000 / rate)
return {allowed, tostring(math.floor(tokens)), retry, reset}
`)

// slidingLogScript stores one sorted-set member per admitted unit.
// ARGV: limit, window (us), cost, unique request id.
var slidingLogScript = redis.NewScript(`
local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost   = tonumber(ARGV[3])
local id     = ARGV[4]

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)

local allowed = 0
local retry = 0
if count + cost <= limit then
  for i = 1, cost do
    redis.call('ZADD', key, now, id .. ':' .. i)
  end
  count = count + cost
  allowed = 1
else
  -- The request fits once the oldest (count + cost - limit) entries expire.
  local idx = count + cost - limit - 1
  local e = redis.call('ZRANGE', key, idx, idx, 'WITHSCORES')
  if e[2] then
    retry = tonumber(e[2]) + window - now
  else
    retry = window
  end
end

redis.call('PEXPIRE', key, math.ceil(window / 1000) + 1000)

local reset = 0
local newest = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
if newest[2] then reset = tonumber(newest[2]) + window - now end
return {allowed, tostring(limit - count), retry, reset}
`)

// slidingCounterScript blends the previous fixed window's count, weighted by
// how much of it still overlaps the sliding window, with the current count.
// ARGV: limit, window (us), cost.
var slidingCounterScript = redis.NewScript(`
local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost   = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local start = now - (now % window)

local s    = redis.call('HMGET', key, 'start', 'curr', 'prev')
local cs   = tonumber(s[1]) or start
local curr = tonumber(s[2]) or 0
local prev = tonumber(s[3]) or 0
if cs ~= start then
  if cs == start - window then prev = curr else prev = 0 end
  curr = 0
end

local into   = now - start
local weight = (window - into) / window
local est    = prev * weight + curr

local allowed = 0
local retry = 0
if est + cost <= limit then
  curr = curr + cost
  est = est + cost
  allowed = 1
else
  local need = est + cost - limit
  -- The previous window's weight decays linearly; if that alone frees enough
  -- room, compute when. Otherwise wait for the next window boundary.
  if prev > 0 and prev * weight >= need then
    retry = math.ceil(need * window / prev)
  else
    retry = window - into
  end
end

redis.call('HSET', key, 'start', start, 'curr', curr, 'prev', prev)
redis.call('PEXPIRE', key, math.ceil(2 * window / 1000) + 1000)

local remaining = math.floor(limit - est)
if remaining < 0 then remaining = 0 end
local reset = window - into
if curr > 0 then reset = reset + window end
return {allowed, tostring(remaining), retry, reset}
`)

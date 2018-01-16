--dead simple named-event manager
local uv = require "luv"
local coroutine = require "prailude.util.coroutine"

local weak_key = {__mode = 'k'}
local mm = require "mm"

local log = require "prailude.log"

local channels = setmetatable({}, {
  __index = function(t, k)
    local cbs = {
      permanent_callbacks = {},
      callbacks = setmetatable({}, weak_key),
      coroutines = setmetatable({}, weak_key)
    }
    rawset(t, k, cbs)
    return cbs
  end
})

local timers = setmetatable({}, {__mode="k"})
local function cancel_timer(cb)
  local timer = rawget(timers, cb)
  if timer then
    timer:stop()
    timer:close()
    rawset(timers, cb, nil)
  end
  return timer
end
local function set_timer(channel, cb, is_coroutine, timeout)
  local timer = rawget(timers, cb)
  if timer then
    timer:stop()
  else
    timer = uv.new_timer()
    rawset(timers, cb, timer)
  end
  timer:start(timeout, 0, function()
    log:debug("bus: channel %s timed out", channel)
    if is_coroutine then
      channels[channel].coroutines[cb] = nil
      coroutine.resume(cb, false)
    else
      channels[channel].callbacks[cb] = nil
      cb(false)
    end
    timer:stop()
    timer:close()
    rawset(timers, cb, nil)
  end)
  return timer
end

local function run_ephemeral_callbacks(chan, tblname, is_coroutine, success, ...)
  local count = 0
  local cbs = rawget(chan, tblname)
  if next(cbs) then --nonempty
    --clear these callbacks
    rawset(chan, tblname, setmetatable({}, weak_key))
  end
  for _, cb in pairs(cbs) do
    cancel_timer(cb)
    if is_coroutine then
      coroutine.resume(cb, success, ...)
    else
      cb(success, ...)
    end
    count = count + 1
  end
  return count
end

local function publish(channel, success, ...)
  --log:debug("bus: publish%s to channel %s", success and "" or "_fail", channel)
  local count_permacallbacks, count_callbacks, count_coros
  local cbs = rawget(channels, channel)
  if cbs then
    count_permacallbacks = #cbs.permanent_callbacks
    for _, cb in ipairs(cbs.permanent_callbacks) do
      cb(success, ...)
    end
    count_callbacks = run_ephemeral_callbacks(cbs, "callbacks",  false, success, ...)
    count_coros =     run_ephemeral_callbacks(cbs, "coroutines", true,  success, ...)
    return count_permacallbacks + count_callbacks + count_coros
  end
end

local Bus = {}
function Bus.pub(channel, ...)
  assert(type(channel)=="string", "channel must be a string")
  publish(channel, true, ...)
end
function Bus.pub_fail(channel, ...)
  assert(type(channel)=="string", "channel must be a string")
  publish(channel, false, ...)
end

function Bus.sub(channel, callback)
  assert(type(channel)=="string", "channel must be a string")
  assert(type(callback)=="function", "callback must be a function")
  --log:debug("bus: perma-subscribed on channel %s, callback %s", channel, callback)
  table.insert(channels[channel].permanent_callbacks, callback)
  return Bus
end

function Bus.once(channel, callback, timeout)
  assert(type(channel)=="string", "channel must be a string")
  assert(type(callback)=="function", "callback must be a function")
  if timeout ~= nil then
    assert(type(timeout) == "number", "timeout must be nil or a number")
  end
  log:debug("bus: one-time-subscribed on channel %s, callback %s timeout %d", channel, callback, timeout)
  channels[channel].callbacks = callback
  if timeout then
    set_timer(channel, callback, false, timeout)
  end
  return Bus
end

function Bus.yield(channel, timeout)
  local coro = coroutine.running()
  assert(type(channel)=="string", "channel must be a string")
  if timeout ~= nil then
    assert(type(timeout) == "number", "timeout must be nil or a number")
  end
  assert(coro, "called Bus.yield while not in a coroutine")
  --log:debug("bus: one-time-subscribed on channel %s, coroutine %s timeout %d", channel, coro, timeout)
  if timeout then
    set_timer(channel, coro, true, timeout)
  end
  channels[channel].coroutines[coro] = true
  return coroutine.yield()
end

--SHUT. DOWN. EVERYTHING.
function Bus.annihilate(are_you_sure)
  if not are_you_sure then return end
  for chan_name, chan in pairs(channels) do
    for _, coro in pairs(chan.coroutines) do
      cancel_timer(coro)
    end
    for _, coro in pairs(chan.callbacks) do
      cancel_timer(coro)
    end
    rawset(chan, chan_name, nil)
  end
  return true
end

return Bus

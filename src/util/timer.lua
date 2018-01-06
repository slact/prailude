local uv = require "luv"

local Timer = {}
function Timer.delay(delay, callback)
  assert(type(delay)=="number", "delay must be a number")
  
  local timer = uv.new_timer()
  local coro
  if not callback then
    coro = coroutine.running()
    assert(coro, "Timer.delay called without callback, expected a coroutine")
    callback = function()
      coroutine.resume(coro)
    end
  end
  local function ontimeout()
    uv.timer_stop(timer)
    uv.close(timer)
    callback()
  end
  uv.timer_start(timer, delay, 0, ontimeout)
  if coro then
    return coroutine.yield()
  else
    return timer
  end
end

function Timer.interval(interval, callback)
  assert(type(interval)=="number", "interval must be a number")
  assert(type(callback)=="function", "callback must be a function")
  local timer = uv.new_timer()
  local function ontimeout()
    if callback(timer) == false then
      Timer.cancel(timer)
    end
  end
  uv.timer_start(timer, interval, interval, ontimeout)
  return timer
end

function Timer.cancel(timer)
  uv.timer_stop(timer)
  uv.close(timer)
end

return Timer

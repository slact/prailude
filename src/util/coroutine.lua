local coroutine_running = coroutine.running
local coroutine_resume = coroutine.resume

local function running()
  --always behave like lua5.1's coroutine.running
  local coro, main = coroutine_running()
  if not main then
    return coro
  else
    return nil
  end
end

local coroutine_util = {}
for k,v in pairs(coroutine) do --eh, this is a little faster than a metatable
  coroutine_util[k]=v
end

local function resume_handler(coro, ok, err, ...)
  if not ok then
    io.stderr:write((debug.traceback(coro, err, 1) .. "\n"))
  end
  return ok, err, ...
end

local function resume(coro, ...)
  return resume_handler(coro, coroutine_resume(coro, ...))
end

coroutine_util.resume = resume
coroutine_util.running = running

function coroutine_util.late_wrap()
  local coro = running()
  if not coro then
    error("no coroutine to create coroutine.resume() callback")
  end
  return function(...)
    return resume(coro, ...)
  end
end

return coroutine_util

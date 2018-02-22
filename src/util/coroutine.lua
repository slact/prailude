local coroutine_running = coroutine.running
local coroutine_resume = coroutine.resume
local mm = require "mm"
local log = require "prailude.log"

local xpcall_with_args; do
  local ver = ({Q="Lua 5.1",R="Lua 5.2",S="Lua 5.3"})[("").dump(function() end):sub(5,5)] or "LuaJIT"
  xpcall_with_args = ver ~= "Lua 5.1"
end


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
    log:error((debug.traceback(coro, err, 1) .. "\n"))
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

local Condition = function(name, check)
  if type(name) == "function" and not check then
    name, check = "", name
  end
  local waiting = {}
  return setmetatable({
    wait = function()
      if not check() then
        local coro = running()
        table.insert(waiting, coro)
        return coroutine.yield()
      end
    end,
    check = function()
      local waiting_num = #waiting
      if waiting_num > 0 and check() then
        local now_waiting = waiting
        waiting = {}
        for _, coro in ipairs(now_waiting) do
          resume(coro)
        end
        return waiting_num
      end
      return 0
    end,
  }, {__tostring = function()
    return ("condition %s [%s] (waiting: %i)"):format(name, check() and "true" or "false", #waiting)
  end})
end



local safe_call; do
  if xpcall_with_args and type(debug) == "table" and type(debug.traceback) == "function" then
    local traceback = debug.traceback
    safe_call = function(func, ...)
      return xpcall(func, traceback, ...)
    end
  else
    safe_call = function(func, ...)
      return pcall(func, ...)
    end
  end
end

local workpool_defaults = {__index = {
  max_workers = 5,
  retry = 0,
  progress_inverval = 1000
}}

local Workpool = function(opt)
  local Timer = require "prailude.util.timer"
  local parent_coro = running()
  assert(parent_coro, "workpool must be called from a coroutine")
  local work = {
    todo = {},
    done = {},
    fail = {},
    fail_reason = {}
  }
  opt = setmetatable(opt or {}, workpool_defaults)
  assert(type(opt.worker) == "function")
  
  local nextjob, havejob --jobs iterator
  local retryjob
  
  if type(opt.retry) == "number" then
    local fails = {}
    retryjob = function(job)
      local failcount = rawget(fails, job) or 0
      failcount = failcount + 1
      rawset(fails, job, failcount)
      return failcount < opt.retry
    end
  elseif type(opt.retry) == "function" then
    retryjob = opt.retry
  else
    error("retry option must be nil, a number or afunction, was instead " .. type(opt.retry))
  end
  
  local check = nil
  if opt.check then check = opt.check end
  
  local active_workers = 0
  
  local moreworkers
  if type(opt.grow) == "function" then
    moreworkers = opt.grow
  elseif not opt.grow then
    moreworkers = function()
      return active_workers < opt.max_workers
    end
  else
    error("'grow' option must be a function")
  end
  
  if type(opt.work) == "table" then
    work.todo = opt.work
    nextjob = function()
      return function()
        return table.remove(work.todo)
      end
    end
    havejob = function()
      return #work.todo > 0
    end
  elseif type(opt.work) == "function" then
    work.todo = {}
    nextjob = function()
      
      return function()
        local job = table.remove(work.todo)
        if job == nil then
          job = opt.work()
        end
        return job
      end
    end
    havejob = function()
      if #work.todo > 0 then
        return true
      else
        local job = opt.work()
        if job ~= nil then
          table.insert(work.todo, job)
        end
        return #work.todo > 0
      end
    end
  else
    error("work generators and other fancy stuff aren't supported yet")
  end
  
  local shiftmanager = Condition("shiftmanager", function()
    if havejob() and not moreworkers(active_workers) then
      return false
    elseif not havejob() and active_workers > 0 then
      return false
    else
      return true
    end
  end)
  
  local foreman = Condition("foreman", function()
    if not havejob() and active_workers == 0 then
      return true
    end
  end)
  
  local worker = function(job)
    active_workers = active_workers + 1
    local ok, res, err = safe_call(opt.worker, job)
    if not ok then
      log:error("workpool: %s", res)
      res, err = nil, res
    end
    if check then
      local check_ok, check_err = pcall(check, job, res, err)
      if not check_ok then
        log:error("workpool: %s", check_err)
      end
    end
    if res == nil then
      if retryjob(job) then
        table.insert(work.todo, job)
      else
        table.insert(work.fail, job)
        table.insert(work.fail_reason, err or "?")
      end
    else
      table.insert(work.done, res)
    end
    active_workers = active_workers - 1
    shiftmanager:check()
    foreman:check()
  end
  
  local company = coroutine.wrap(function()
    for job in nextjob() do
      --print("run the worker ", workwrap ,"for job " ..current_job)
      coroutine.wrap(worker)(job)
      shiftmanager:wait()
    end
  end)
  company()
  
  local inspector
  if opt.progress then
    inspector = Timer.interval(opt.progress_inverval, function()
      opt.progress(active_workers, work.done, work.todo, work.fail, work.fail_reason)
    end)
  end
  
  foreman:wait()
  if inspector then
    inspector:stop()
  end
  return work.done, work.fail, work.fail_reason
end

coroutine_util.workpool = Workpool
coroutine_util.condition = Condition

return coroutine_util

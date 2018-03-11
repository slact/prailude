local prailude = require "prailude"
local mm = require "mm"
local util = require "prailude.util"
local Balance = util.balance
local coroutine = require "prailude.util.coroutine"
local Timer = require "prailude.util.timer"
local uv = require "luv"

prailude.bus.sub("message:receive", function(ok, msg, peer)
  if not ok then return end
end)

--prailude.bus.sub("run", function()
--  local start_peer = prailude.peer.get("138.68.2.234", 7075)
--  local start_message = prailude.message.new("keepalive")
--  start_message:send(start_peer)
--end)

--test balance stuff
--local balance = Balance.new(10, "Mxrb")
--print(Balance.unpack(balance:pack()))
--local maxbalance = Balance.new(Balance.genesis)
--local zero = Balance.new()
--print(zero - 1)

--[[
local wrk = 0
local worker = function(thing)
  local mynum = wrk
  wrk= wrk + 1

  Timer.delay(math.floor(math.random() * 3000))
  if math.random() > 0.3 then
    print(" worker", mynum, "computed", thing)
    return "computed " .. thing
  else
    print(" worker", mynum, "\"failed to compute\"", thing)
    error("errroneout")
  end
end

local work = {"foo", "bar", "baz", "bananana", "what is ", "anyting", "1","22", "whis", "what", "when", "whibbly"}

Timer.delay(100, coroutine.wrap(function()
  local done, failed, failed_why = coroutine.workpool({
    work = function()
      return table.remove(work)
    end,
    worker = worker,
    retry = function(job)
      if job == "1" then
        print("RETRY THIS JOB!!")
        return true
      end
    end,
    grow = function(active)
      return active < 2
    end,
    inspect = function(active_workers_count, work_todo, work_done, work_failed, work_failed_errors)
      print("WORKERS ACTIVE: ", active_workers_count)
    end
  })
  
  print("hEYOOOOO")
  
  mm(done)
  mm(failed)
  mm(failed_why)

end))

math.randomseed (10)

Timer.delay(1000000000, function() end)
uv.run()
]]


Timer.interval(5000, function()
  print("Memory use:" ..  math.ceil(collectgarbage("count")) .. "Kb.")
  --collectgarbage("collect")
  --print("After GC  :" ..  collectgarbage("count") .. "Kb ")
end)
prailude.run()

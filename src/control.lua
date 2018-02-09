local Message = require "prailude.message"
local Server = require "prailude.server"

local Peer = require "prailude.peer"
local uv = require "luv"
local bus = require "prailude.bus"
local Control = {}
local mm = require "mm"
local log = require "prailude.log"-- "control"
local Rainet = require "prailude.rainet"
local Timer = require "prailude.util.timer"
local DB = require "prailude.db"

function Control.initialize()
  DB.initialize("sqlite")
  Server.initialize()
  Rainet.initialize()
end

--[[
local loop_start_time
function Control.poll_time() --time this loop iteration has been running
  return os.time() - loop_start_time
end
function Control.be_nice_to_event_loop(func)
  return function(...)
    if Control.poll_time() > 0.100 then --10ms of cpu time spent in this loop interation
      -- that's waay too much.
      local arg = {...}
      Timer.delay(10, function()
        func(table.unpack(arg))
      end)
    else
      func(...)
    end
  end
end
]]

function Control.run()
  --do some other stuff maybe
  Timer.delay(0, function()
    bus.pub("run")
  end)
  
  local sigint = assert(uv.new_signal())
  sigint:start("sigint", function()
    log:debug("prailude: shutting down")
    sigint:close()
    uv.stop()
  end)
  --[[
  Timer.delay(1000, function()
    Rainet.bootstrap()
  end)
  
  --[[
  repeat
    --time each loop iteration start
    loop_start_time = os.time()
  until uv.run("once") == false
  uv.loop_close()
  ]]
  log:debug("prailude: entering main loop")
  uv.run()
  uv.loop_close()
  bus.pub("shutdown")
end

return Control

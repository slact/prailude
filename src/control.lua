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

function Control.run()
  --do some other stuff maybe
  Timer.delay(0, function()
    bus.pub("run")
  end)
  
  Timer.delay(10*1000, function()
    Rainet.bootstrap()
  end)
  uv.run()
end

return Control

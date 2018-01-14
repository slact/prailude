local Message = require "prailude.message"
local Server = require "prailude.server"

local Peer = require "prailude.peer"
local uv = require "luv"
local bus = require "prailude.bus"
local Control = {}
local mm = require "mm"
local log = require "prailude.log"-- "control"
local Rainet = require "prailude.rainet"
local Block = require "prailude.block"
local sqlite3 = require "lsqlite3"
local Timer = require "prailude.util.timer"


function Control.initialize()
  local db = sqlite3.open("raiblocks.db")
  db:exec("PRAGMA synchronous = OFF") --don't really care if the db lags behind on crash
  Block.initialize(db)
  Server.initialize(db)
  Rainet.initialize(db)
  
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

local Message = require "prailude.message"
local Server = require "prailude.server"

local Peer = require "prailude.peer"
local uv = require "luv"
local bus = require "prailude.bus"
local Control = {}
local mm = require "mm"
local log = require "prailude.log"-- "control"
local Rainet = require "prailude.rainet"


function Control.initialize()
  Server.initialize()
  Rainet.initialize()
end

function Control.run()
  --do some other stuff maybe
  local run_timer = uv.new_timer():start(0, 0, function()
    bus.pub("run")
  end)
  uv.run()
end

return Control

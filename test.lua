local prailude = require "prailude"
local mm = require "mm"
local util = require "prailude.util"
local Balance = util.balance

prailude.bus.sub("message:receive", function(ok, msg, peer)
  if not ok then return end
end)

--prailude.bus.sub("run", function()
--  local start_peer = prailude.peer.get("138.68.2.234", 7075)
--  local start_message = prailude.message.new("keepalive")
--  start_message:send(start_peer)
--end)

--test balance stuff
local balance = Balance.new(10, "Mxrb")
--print(Balance.unpack(balance:pack()))



prailude.initialize()
prailude.run()

local prailude = require "prailude"
local mm = require "mm"

prailude.bus.sub("message:receive", function(ok, msg, peer)
  if not ok then return end
end)

--prailude.bus.sub("run", function()
--  local start_peer = prailude.peer.get("138.68.2.234", 7075)
--  local start_message = prailude.message.new("keepalive")
--  start_message:send(start_peer)
--end)


prailude.initialize()
prailude.run()

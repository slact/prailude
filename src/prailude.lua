local Prailude = {
  util =        require "prailude.util",
  -- shared singleton-y stuff
  config =      require "prailude.config",
  server =      require "prailude.server",
  bus =         require "prailude.bus",
  control =     require "prailude.control",
  -- instantiatable objects
  peer =        require "prailude.peer",
  message =     require "prailude.message",
  block =       require "prailude.block",
  transaction = require "prailude.transaction",
  account =     require "prailude.account"
}

local uv = require "luv"

function Prailude.run(port, bootstrap_peers)
  Prailude.control.run(port, bootstrap_peers)
end

return Prailude

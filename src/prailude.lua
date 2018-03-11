local Prailude = {
  util =        require "prailude.util",
  -- shared singleton-y stuff
  config =      require "prailude.config",
  server =      require "prailude.server",
  bus =         require "prailude.bus",
  control =     require "prailude.control",
  rainet =      require "prailude.rainet",
  -- instantiatable objects
  peer =        require "prailude.peer",
  message =     require "prailude.message",
  block =       require "prailude.block",
  transaction = require "prailude.transaction",
  account =     require "prailude.account"
}

Prailude.initialize =   Prailude.control.initialize
Prailude.run =          Prailude.control.run
--[[
_G.DBG = function(...)
  local mm = require "mm"
  for _, v in ipairs {...} do
    print(mm(v))
  end
end
--]]
return Prailude

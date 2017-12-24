local Prailude =  {
  util =   require "prailude.util",
  net =    require "prailude.net",
  config = require "prailude.config",
  parser = require "prailude.parser"
}

local uv = require "luv"

function Prailude.run(port)
  local server = Prailude.net.server.new()
  --do some other stuff maybe
  uv.run()
end

return Prailude

local Prailude =  {
  util =   require "prailude.util",
  net =    require "prailude.net",
  config = require "prailude.config",
  message = require "prailude.message"
}

local uv = require "luv"

function Prailude.run(port)
  local server = Prailude.net.server.new()
  --do some other stuff maybe
  Prailude.running_server = server
  uv.run()
end

return Prailude

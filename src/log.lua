local Logger = require "lualogging"


local active_logger

local Log = {}

function set_log_prefix(prefix)
  --meh
  
end

setmetatable(Log, {
  __index = active_logger
})

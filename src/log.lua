local Logging = require "logging"
local Console_Logging = require "logging.console"

local active_logger = Console_Logging()

print("new active active_logger " .. tostring(active_logger))

return active_logger

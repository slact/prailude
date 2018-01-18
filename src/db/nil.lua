--dummy database that errors out on any call
local dummy = {
  __index = function(k)
    error("No database configured or initialized, can't run " .. k)
  end
}

return setmetatable({}, {__index = function()
  return dummy
end})

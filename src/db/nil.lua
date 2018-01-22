--dummy database that errors out on any call
local function dummy(name)
  return {
    __index = function(_, k)
      error("No database configured or initialized, can't run ".. name .. ".".. k)
    end
  }
end

return setmetatable({}, {__index = function(_, k)
  return dummy(k)
end})

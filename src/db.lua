local DB = {}

function DB.initialize(dbtype, opt)
  assert(type(dbtype)=="string")
  local actual_db = require("prailude.db." .. dbtype)
  actual_db.initialize(opt)
  setmetatable(DB, {__index = actual_db})
  DB.kv = require("prailude.db."..dbtype..".kv")
end

return DB

local DB = {}

function DB.initialize(dbtype, opt)
  assert(type(dbtype)=="string")
  local actual_db = require("prailude.db." .. dbtype)
  setmetatable(DB, {__index = actual_db})
  actual_db.initialize(opt)
  DB.kv = require("prailude.db."..dbtype..".kv")
end

return DB

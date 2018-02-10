local sqlite3 = require "lsqlite3"
local Bus = require "prailude.bus"

local subdbs = {
  require "prailude.db.sqlite.peer",
  require "prailude.db.sqlite.block",
  require "prailude.db.sqlite.frontier",
  require "prailude.db.sqlite.kv",
}

local db
local DB = {}

function DB.initialize(opt)
  opt = opt or {}
  db = sqlite3.open(opt.db_file or "raiblocks.db")
  db:exec("PRAGMA synchronous = OFF") --don't really care if the db lags behind on crash
  db:exec("PRAGMA journal_mode = TRUNCATE")
  db:exec("PRAGMA temp_store = MEMORY")
  db:exec("PRAGMA foreign_keys = OFF")
  for _, subdb in ipairs(subdbs) do
    subdb.initialize(db)
  end
  Bus.sub("shutdown", DB.shutdown)
end
  
function DB.shutdown()
  for _, subdb in ipairs(subdbs) do
    if subdb.shutdown then
      subdb.shutdown()
    end
  end
end

return DB

local sqlite3 = require "lsqlite3"

local subdbs = {
  require "prailude.db.sqlite.peer",
  require "prailude.db.sqlite.block",
  require "prailude.db.sqlite.frontier",
  require "prailude.db.sqlite.kv",
}

local db
local DB = {
  initialize = function(opt)
    opt = opt or {}
    db = sqlite3.open(opt.db_file or "raiblocks.db")
    db:exec("PRAGMA synchronous = OFF") --don't really care if the db lags behind on crash
    for _, subdb in ipairs(subdbs) do
      subdb.initialize(db)
    end
  end
}

return DB
local Frontier
local sqlite3 = require "lsqlite3"

local schema = [[
  CREATE TABLE IF NOT EXISTS frontiers (
    account           TEXT,
    hash              TEXT,
    source_peer       TEXT,
    received          INT,
    PRIMARY KEY(account)
  ) WITHOUT ROWID;
  
  CREATE INDEX IF NOT EXISTS frontiers_hash_idx        ON frontiers (hash);
]]

local db
local FrontierDB_meta = {__index = function()
  error("not yet implemented")
end}

return {
  initialize = function(db_ref)
    Frontier = require "prailude.frontier"
    db = db_ref
    if db:exec(schema) ~= sqlite3.OK then
      error(db:errmsg())
    end
    setmetatable(Frontier, FrontierDB_meta)
  end
}

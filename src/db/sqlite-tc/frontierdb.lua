local Frontier
local sqlite3 = require "lsqlite3"

local function schema(tbl_type, tbl_name)
  local _, tbl = tbl_name:match("^(.+%.)(.+)")
  if not tbl then tbl = tbl_name end
  
  return [[
    CREATE ]]..tbl_type..[[ IF NOT EXISTS ]]..tbl_name..[[ (
      account           TEXT,
      frontier          TEXT,
      stored_frontier   TEXT,
      genesis_distance  INTEGER,
      pull_id           INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ]]..tbl_name..[[_unique_idx ON ]]..tbl..[[ (account, frontier);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_frontier_idx      ON ]]..tbl..[[ (frontier);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_account_idx       ON ]]..tbl..[[ (account);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_pull_idx          ON ]]..tbl..[[ (pull_id);
  ]]
end

local sql = {}

local db
local FrontierDB_meta = {__index = {
  store = function(self)
    local stmt = sql.frontier_store
    stmt:bind(1, self.account)
    stmt:bind(2, self.frontier)
    stmt:bind(3, self.stored_frontier)
    stmt:bind(4, self.pull_id)
    stmt:step()
    stmt:reset()
    return self
  end,
  
  get_size = function()
    local stmt = sql.frontier_size
    local ret = assert(stmt:urows()(stmt), db:errmsg())
    stmt:reset()
    return ret
  end,
  
  get_range = function(limit, offset)
    assert(type(limit)=="number")
    assert(type(offset)=="number")
    local stmt = sql.frontier_get_range
    stmt:bind(1, limit)
    stmt:bind(2, offset)
    local new = Frontier.new
    local res = {}
    for row in stmt:nrows() do
      table.insert(res, new(row))
    end
    return res
  end,
  
  batch_store = function(batch, pull_id)
    assert(db:exec("BEGIN EXCLUSIVE TRANSACTION") == sqlite3.OK, db:errmsg())
    local store = Frontier.store
    for _, frontier in ipairs(batch) do
      if not frontier.pull_id then
        frontier.pull_id = pull_id
      end
      store(frontier)
    end
    assert(db:exec("COMMIT TRANSACTION") == sqlite3.OK, db:errmsg())
  end,
  
  delete_synced_frontiers = function()
    assert(db:exec("DELETE from disktmp.frontier WHERE frontier == stored_frontier") == sqlite3.OK, db:errmsg())
    return db:changes()
  end,
  
  clear_bootstrap = function()
    assert(db:exec("DELETE FROM disktmp.frontier") == sqlite3.OK, db:errmsg())
  end
}}

return {
  initialize = function(db_ref)
    Frontier = require "prailude.frontier"
    db = db_ref
    --assert(db:exec(schema("TABLE", "frontier")) == sqlite3.OK, db:errmsg())
    assert(db:exec(schema("TABLE", "disktmp.frontier")) == sqlite3.OK, db:errmsg())
    
    sql.frontier_store = assert(db:prepare("INSERT OR IGNORE INTO disktmp.frontier " ..
      "      (account, frontier, stored_frontier, pull_id) " ..
      "VALUES(      ?,        ?,               ?,       ?);"), db:errmsg())
    
    sql.frontier_size = assert(db:prepare("SELECT count(*) FROM disktmp.frontier"), db:errmsg())
    
    sql.frontier_get_range = assert(db:prepare("SELECT * FROM disktmp.frontier ORDER BY account LIMIT ? OFFSET ?"), db:errmsg())
    
    setmetatable(Frontier, FrontierDB_meta)
  end,
  
  shutdown = function()
    for _, stmt in pairs(sql) do
      stmt:finalize()
    end
  end
  
}


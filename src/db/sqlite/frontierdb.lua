local Frontier
local sqlite3 = require "lsqlite3"

local function schema(tbl_type, tbl_name)
  local _, tbl = tbl_name:match("^(.+%.)(.+)")
  if not tbl then tbl = tbl_name end
  
  return [[
    CREATE ]]..tbl_type..[[ IF NOT EXISTS ]]..tbl_name..[[ (
      account           TEXT,
      frontier          TEXT,
      pull_id           INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_frontier_idx      ON ]]..tbl..[[ (frontier);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_account_idx       ON ]]..tbl..[[ (account);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_pull_idx          ON ]]..tbl..[[ (pull_id);
  ]]
end

local sql = {}

local db
local FrontierDB_meta = {__index = {
  find = function(id)
    local stmt = sql.frontier_get
    stmt:bind(1, id)
    local frontier = stmt:nrows()(stmt)
    return frontier
  end,
  
  store = function(self)
    local stmt = sql.frontier_store
    stmt:bind(1, self.account)
    stmt:bind(2, self.frontier)
    stmt:bind(3, self.pull_id)
    stmt:step()
    stmt:reset()
    return self
  end,
  
  get_pull_size = function(id)
    local stmt = sql.frontier_pull_size
    stmt:bind(1, id)
    print(id)
    return assert(stmt:urows()(stmt), db:errmsg())
  end,
  
  get_range = function(frontier_pull_id, limit, offset)
    assert(type(limit)=="number")
    assert(type(offset)=="number")
    local stmt = sql.frontier_get_range_from_peer
    stmt:bind(1, frontier_pull_id)
    stmt:bind(2, limit)
    stmt:bind(3, offset)
    local new = Frontier.new
    local res = {}
    for row in stmt:nrows() do
      table.insert(res, new(row))
    end
    return res
  end,
  
  delete_pull_data = function(id)
    local stmt = sql.frontier_delete_by_pull_id
    stmt:bind(1, id)
    stmt:step()
    stmt:reset()
    return true
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
  end
}}

return {
  initialize = function(db_ref)
    Frontier = require "prailude.frontier"
    db = db_ref
    --assert(db:exec(schema("TABLE", "frontier")) == sqlite3.OK, db:errmsg())
    assert(db:exec(schema("TABLE", "bootstrap.frontier")) == sqlite3.OK, db:errmsg())
    assert(db:exec("DELETE FROM bootstrap.frontier") == sqlite3.OK, db:errmsg())
    
    sql.frontier_store = assert(db:prepare("INSERT OR REPLACE INTO bootstrap.frontier " ..
      "      (account, frontier, pull_id) " ..
      "VALUES(      ?,        ?,       ?);"), db:errmsg())
    
    sql.frontier_get = assert(db:prepare("SELECT * FROM bootstrap.frontier WHERE pull_id = ?"), db:errmsg())
    
    sql.frontier_pull_size = assert(db:prepare("SELECT count(*) FROM bootstrap.frontier WHERE pull_id = ?"), db:errmsg())
    
    sql.frontier_delete_by_pull_id = assert(db:prepare("DELETE FROM bootstrap.frontier WHERE pull_id = ?"), db:errmsg())
    
    sql.frontier_get_range_from_peer = assert(db:prepare("SELECT * FROM bootstrap.frontier WHERE pull_id = ? ORDER BY account LIMIT ? OFFSET ?"), db:errmsg())
    
    setmetatable(Frontier, FrontierDB_meta)
  end,
  
  shutdown = function()
    for _, stmt in pairs(sql) do
      stmt:finalize()
    end
  end
  
}


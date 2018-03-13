local sqlite3 = require "lsqlite3"
local log = require "prailude.log"
local BlockWalker

local schema = function(tbl_type, tbl_name)
  local _, tbl = tbl_name:match("^(.+%.)(.+)")
  if not tbl then tbl = tbl_name end
  return [[
  CREATE ]] .. tbl_type .. [[ IF NOT EXISTS ]] .. tbl_name .. [[ (
    walk_id                  INTEGER,
    page_id                  INTEGER,
    block                    TEXT
  );
  CREATE UNIQUE INDEX IF NOT EXISTS ]] .. tbl_name .. [[_unique_block_idx       ON ]] .. tbl .. [[ (walk_id, block);
  CREATE INDEX IF NOT EXISTS ]] .. tbl_name ..        [[_page_idx               ON ]] .. tbl .. [[ (page_id);
  CREATE INDEX IF NOT EXISTS ]] .. tbl_name ..        [[_walk_idx               ON ]] .. tbl .. [[ (walk_id);
]]

end

local db

local sql = {}

local BlockWalkerDB_meta = {__index = {
  store_page = function(walk_id, page_id, batch)
    local stmt = sql.store
    assert(db:exec("BEGIN EXCLUSIVE TRANSACTION") == sqlite3.OK, db:errmsg())
    for _, hash in ipairs(batch) do
      stmt:bind(1, walk_id)
      stmt:bind(2, page_id)
      stmt:bind(3, hash)
      stmt:step()
      stmt:reset()
    end
    assert(db:exec("COMMIT TRANSACTION") == sqlite3.OK, db:errmsg())
  end,
  
  restore_page = function(walk_id, page_id)
    local hashes = {}
    local stmt = sql.get_page
    stmt:bind(1, walk_id)
    stmt:bind(2, page_id)
    for hash in stmt:urows() do
      table.insert(hashes, hash)
    end
    stmt:reset()
    
    stmt = sql.delete_page
    stmt:bind(1, walk_id)
    stmt:bind(2, page_id)
    stmt:reset()
    
    return hashes
  end,
  
  get_page_size = function(walk_id, page_id)
    --TODO: cache maybe? eh...
    local stmt = sql.page_size
    stmt:bind(1, walk_id)
    stmt:bind(2, page_id)
    return stmt:urows()(stmt) or 0
  end,
  
  delete_page = function(walk_id, page_id)
    local stmt = sql.delete_page
    stmt:bind(1, walk_id)
    stmt:bind(2, page_id)
    stmt:step()
    stmt:reset()
    return db:changes()
  end,
  
  delete = function(walk_id)
    local stmt = sql.delete
    stmt:bind(1, walk_id)
    stmt:step()
    stmt:reset()
    return db:changes()
  end
}}

return {
  initialize = function(shared_db)
    BlockWalker = require "prailude.blockwalker"
    db = shared_db
    
    assert(db:exec(schema("TABLE", "blockwalker")) == sqlite3.OK, db:errmsg())
    
    sql.store = assert(db:prepare("INSERT OR IGNORE INTO blockwalker (walk_id, page_id, block) VALUES(?, ?, ?)"), db:errmsg())
    
    sql.page_size = assert(db:prepare("SELECT COUNT(*) FROM blockwalker WHERE walk_id = ? AND page_id = ?"), db:errmsg())
    sql.get_page = assert(db:prepare("SELECT block FROM blockwalker WHERE walk_id = ? AND page_id = ?"), db:errmsg())
    sql.delete_page = assert(db:prepare("DELETE FROM blockwalker WHERE walk_id = ? AND page_id = ?"), db:errmsg())
    
    sql.delete = assert(db:prepare("DELETE FROM blockwalker WHERE walk_id = ?"), db:errmsg())

    setmetatable(BlockWalker, BlockWalkerDB_meta)
  end,
  shutdown = function()
    --save previously known peers
    for _, stmt in pairs(sql) do
      stmt:finalize()
    end
  end
}

local Block
local sqlite3 = require "lsqlite3"
local mm = require "mm"
local Util = require "prailude.util"

local function indices(what, tbl_name)
  local _, tbl = tbl_name:match("^(.+%.)(.+)")
  local idxes = {
    _account_idx = "account",
    _prev_idx = "previous",
    _valid_idx = "valid",
    _type_idx = "type",
    _source_idx = "source",
    _rep_idx = "representative",
    _dst_idx = "destination"
  }
  
  if not tbl then tbl = tbl_name end
  local ret = {}
  for idx, col in pairs(idxes) do
    local line
    if what == "drop" then
      line = ("DROP INDEX %s%s;"):format(tbl_name, idx)
    else
      line = ("CREATE INDEX IF NOT EXISTS %s%s     ON %s(%s);"):format(tbl_name, idx, tbl, col)
    end
    table.insert(ret, line)
  end
  return table.concat(ret, "\n")
end

local schema = function(tbl_type, tbl_name, skip_indices)
  local schema = [[
  CREATE ]]..tbl_type..[[ IF NOT EXISTS ]]..tbl_name..[[ (
    hash                 BLOB,
    account              BLOB,
    signature            BLOB,
    valid                INTEGER, --0: invalid
                                  --1: PoW ok
                                  --2: signature ok
                                  --3: ledger check ok
                                  --4: confirmed
    
    type                 TEXT,
    previous             BLOB, --send, receive, change (previous block)
    source               BLOB, --open, receive (source block)
    representative       BLOB, --open, change  (rep account)
    destination          BLOB, --send (destination account)
    work                 BLOB,
    timestamp            INTEGER,
    genesis_distance     INTEGER,
    balance              BLOB]]
  if not skip_indices then
    schema = schema .. ",\n  PRIMARY KEY(hash)\n  ) WITHOUT ROWID;\n"
  else
    schema = schema .. "\n  );\n"
  end
  if not skip_indices then
    schema = schema .. indices("create", tbl_name)
  end
  return schema
end

local sql={}

local cache = Util.Cache("weak")

local db
local function valid_code(valid)
  if not valid then
    return 0
  elseif valid == "PoW" then
    return 1
  elseif valid == "signature" then
    return 2
  elseif valid == "ledger" then
    return 3
  elseif valid == "confirmed" then
    return 4
  else
    error("unknown block validation state " .. tostring(valid))
  end
end

local BlockDB_meta = {__index = {
  find = function(hash)
    local block, stmt = cache:get(hash), sql.block_get
    if block == nil then
      --print("CACHE: block "  .. Util.bytes_to_hex(hash) .. " not in cache")
      stmt:bind(1, hash)
      block = stmt:nrows()(stmt)
      --TODO: check for sqlite3.BUSY and such responses
      stmt:reset()
      if block then
        block = Block.new(block)
      end
      cache:set(hash, block or false)
      return block
    elseif block == false then
      --print("CACHE: block "  .. Util.bytes_to_hex(hash) .. " does not exist (in cache)")
      return nil
    else
      --print("CACHE: block "  .. Util.bytes_to_hex(hash) .. " found")
      return block
    end
  end,
  
  find_by_account = function(acct)
    local blocks = {}
    local stmt = sql.block_get_by_acct
    stmt:bind(1, acct)
    local block
    for _, block_data in stmt:nrows() do
      block = cache:get(block_data.hash) or Block.new(block_data)
      table.insert(blocks, block)
    end
    stmt:reset()
    return blocks
  end,
  
  store = function(self, opt)
    if self.type == "open" then
      self.n = 1
    end
    local stmt = opt == "bootstrap" and sql.bootstrap_block_set or sql.block_set
    stmt:bind(1, assert(self.hash, "block hash missing"))
    stmt:bind(2, assert(self.account, "block account missing")) -- could be nil, and that's okay
    stmt:bind(3, assert(self.signature, "block signature missing"))
    
    local valid = assert(self.valid, "block valid state missing")
    stmt:bind(4, valid_code(valid))
    
    stmt:bind(5, assert(self.type, "block type missing")) --TODO: use typecode for packing efficiency
    stmt:bind(6, self.previous)
    stmt:bind(7, self.source)
    stmt:bind(8, self.representative)
    stmt:bind(9, self.destination)
    stmt:bind(10, self.balance and self.balance:pack() or nil)
    stmt:bind(11, self.work)
    stmt:bind(12, self.timestamp)
    stmt:bind(13, self.genesis_distance)
    stmt:step()
    --TODO: check for sqlite3.BUSY and such responses
    stmt:reset()
    if opt ~= "bootstrap" then
      cache:set(self.hash, self)
    end
    return self
  end,
  
  batch_store_bootstrap = function(batch)
    assert(db:exec("BEGIN EXCLUSIVE TRANSACTION") == sqlite3.OK, db:errmsg())
    for _, block in ipairs(batch) do
      block:store("bootstrap")
    end
    assert(db:exec("COMMIT TRANSACTION") == sqlite3.OK, db:errmsg())
  end,
  
  batch_update_ledger_validation = function(batch)
    print("update batch of size " .. #batch)
    local stmt = sql.block_update_ledger_validation
    assert(db:exec("BEGIN EXCLUSIVE TRANSACTION") == sqlite3.OK, db:errmsg())
    for _, block in ipairs(batch) do
        stmt:bind(1, valid_code(block.valid))
        stmt:bind(2, block.genesis_distance)
        stmt:bind(3, block.hash)
        stmt:step()
        stmt:reset()
    end
    assert(db:exec("COMMIT TRANSACTION") == sqlite3.OK, db:errmsg())
  end,
  
  clear_bootstrap = function()
    assert(db:exec("DELETE FROM disktmp.blocks") == sqlite3.OK, db:errmsg())
  end,
  
  import_unverified_bootstrap_blocks = function(progress_callback)
    local gettime = require "prailude.util.lowlevel".gettime
    local block_count, import_block_count = Block.count(), Block.count_bootstrapped()
    
    local reindex = block_count + import_block_count > block_count * 1.20
      -- anything above a 20% differentce should trigger a reindex
    if reindex then
      print("drop block index during import")
      assert(db:exec(indices("drop", "blocks")) == sqlite3.OK, db:errmsg())
    end
    local n = 1000000
    local t0 = gettime()
    if progress_callback then
      db:progress_handler(n, function()
        local t1 = gettime()
        progress_callback(n, t1 - t0)
        t0 = t1
        return false
      end)
    end
    assert(db:exec("INSERT OR IGNORE INTO blocks SELECT * FROM disktmp.blocks;") == sqlite3.OK, db:errmsg())
    if reindex then
      assert(db:exec(indices("create", "blocks")) == sqlite3.OK, db:errmsg())
    end
    if progress_callback then
      db:progress_handler(nil, nil)
    end
    return db:changes()
  end,
  
  get_child_hashes = function(block)
    local hashes = {}
    local stmt = sql.get_child_hashes
    stmt:bind(1, block.hash)
    stmt:bind(2, block.hash)
    for hash in stmt:urows() do
      table.insert(hashes, hash)
    end
    stmt:reset()
    return hashes
  end,
  
  count = function()
    local stmt = sql.blocks_count
    local n = stmt:urows()(stmt)
    stmt:reset()
    return n
  end,
  
  count_bootstrapped = function()
    local stmt = sql.bootstrapped_blocks_count
    local n = stmt:urows()(stmt)
    stmt:reset()
    return n
  end,
  
  count_valid = function(valid)
    local stmt = sql.blocks_count_valid
    stmt:bind(1, assert(valid_code(valid)))
    local n = stmt:urows()(stmt)
    stmt:reset()
    return n
  end,
  
  find_block_by = function(what, val)
    local stmt
    if what == "source" then
      stmt = sql.block_get_by_source
    elseif what == "previous" then
      stmt = sql.block_get_by_previous
    else
      error("can't find block by " .. tostring(what))
    end
    stmt:bind(1, val)
    local block = stmt:nrows()(stmt)
    stmt:reset()
    if block then
      block = cache:get(block.hash) or Block.new(block)
    end
    return block
  end,
  
}}

return {
  initialize = function(db_ref)
    Block = require "prailude.block"
    db = db_ref
    print(schema("TABLE", "disktmp.blocks"))
    assert(db:exec(schema("TABLE", "blocks")) == sqlite3.OK, db:errmsg())
    assert(db:exec(schema("TABLE", "disktmp.blocks", true)) == sqlite3.OK, db:errmsg())
    
    sql.block_get = assert(db:prepare("SELECT * FROM blocks WHERE hash = ?"), db:errmsg())
    
    sql.block_get_by_previous = assert(db:prepare("SELECT * FROM blocks WHERE previous = ?"), db:errmsg())
    sql.block_get_by_source = assert(db:prepare("SELECT * FROM blocks WHERE source = ?"), db:errmsg())
    
    sql.block_set = assert(db:prepare("INSERT OR REPLACE INTO blocks " ..
         "(hash, account, signature, valid, type, previous, source, representative, destination, balance, work, timestamp, genesis_distance) " ..
      "VALUES(?,       ?,         ?,     ?,    ?,        ?,      ?,              ?,           ?,       ?,    ?,         ?,                ?)", db:errmsg()))
    
    sql.bootstrap_block_set = assert(db:prepare("INSERT OR IGNORE INTO disktmp.blocks " ..
         "(hash, account, signature, valid, type, previous, source, representative, destination, balance, work, timestamp, genesis_distance) " ..
      "VALUES(?,       ?,         ?,     ?,    ?,        ?,      ?,              ?,           ?,       ?,    ?,         ?,                ?)", db:errmsg()))
    
    sql.block_update_ledger_validation = assert(db:prepare("UPDATE blocks SET valid = ?, genesis_distance = ? WHERE hash = ?"), db:errmsg())
    
    sql.find_by_previous = assert(db:prepare("SELECT * FROM blocks WHERE previous = ?"), db:errmsg())
    sql.find_by_source = assert(db:prepare("SELECT * FROM blocks WHERE source = ?"), db:errmsg())
    
    sql.blocks_count = assert(db:prepare("SELECT COUNT(*) FROM blocks"), db:errmsg())
    sql.blocks_count_valid = assert(db:prepare("SELECT COUNT(*) FROM blocks WHERE valid >= ?"), db:errmsg())
    sql.bootstrapped_blocks_count = assert(db:prepare("SELECT COUNT(*) FROM disktmp.blocks"), db:errmsg())
    
    setmetatable(Block, BlockDB_meta)
  end,
  
  shutdown = function()
    for _, stmt in pairs(sql) do
      stmt:finalize()
    end
  end,
}

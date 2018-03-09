local Block
local sqlite3 = require "lsqlite3"
local mm = require "mm"
--local Util = require "prailude.util"

local schema = function(tbl_type, tbl_name, skip_indices)
  local _, tbl = tbl_name:match("^(.+%.)(.+)")
  if not tbl then tbl = tbl_name end
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
    timestamp            INTEGER,
    genesis_distance     INTEGER,
    balance              BLOB,

    PRIMARY KEY(hash)
  ) WITHOUT ROWID;
  ]]
  if not skip_indices then
    schema = schema .. [[
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_account_idx      ON ]]..tbl..[[ (account);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_prev_idx         ON ]]..tbl..[[ (previous);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_valid_idx        ON ]]..tbl..[[ (valid);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_type_idx         ON ]]..tbl..[[ (type);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_source_idx       ON ]]..tbl..[[ (source);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_rep_idx          ON ]]..tbl..[[ (representative);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_dst_idx          ON ]]..tbl..[[ (destination);
    --CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_balance_idx      ON ]]..tbl..[[ (balance);
    ]]
  end
  return  schema
end

local sql={}

local cache = setmetatable({}, {__mode="v"})
local cache_enabled = true

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
    local block, stmt = nil, sql.block_get
    if cache_enabled then
      block = rawget(cache, hash)
    end
    if block == nil then
      --print("CACHE: block "  .. Util.bytes_to_hex(hash) .. " not in cache")
      stmt:bind(1, hash)
      block = stmt:nrows()(stmt)
      --TODO: check for sqlite3.BUSY and such responses
      stmt:reset()
      if block then
        block = Block.new(block)
      end
      if cache_enabled then
        rawset(cache, hash, block or false)
      end
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
    for _, block in stmt:nrows() do
      if cache_enabled then
        local cached_block = rawget(cache, block.hash)
        if cached_block then
          --print("CACHE: block "  .. Util.bytes_to_hex(block.hash) .. " found")
          block = cached_block
        end
      end
      table.insert(blocks, Block.new(block))
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
    stmt:bind(11, self.timestamp)
    stmt:bind(12, self.genesis_distance)
    stmt:step()
    --TODO: check for sqlite3.BUSY and such responses
    stmt:reset()
    if cache_enabled and opt ~= "bootstrap" then
      --update cache
      rawset(cache, self.hash, self)
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
  
  import_unverified_bootstrap_blocks = function()
    return assert(db:exec("INSERT OR REPLACE INTO blocks SELECT * FROM disktmp.blocks") == sqlite3.OK, db:errmsg())
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
  end
}}

return {
  initialize = function(db_ref)
    Block = require "prailude.block"
    db = db_ref
    
    assert(db:exec(schema("TABLE", "blocks")) == sqlite3.OK, db:errmsg())
    assert(db:exec(schema("TABLE", "disktmp.blocks", true)) == sqlite3.OK, db:errmsg())
    
    sql.block_get = assert(db:prepare("SELECT * FROM blocks WHERE hash = ?"), db:errmsg())
    
    sql.block_get_by_previous = assert(db:prepare("SELECT * FROM blocks WHERE previous = ?"), db:errmsg())
    sql.block_get_by_source = assert(db:prepare("SELECT * FROM blocks WHERE source = ?"), db:errmsg())
    
    sql.block_set = assert(db:prepare("INSERT OR REPLACE INTO blocks " ..
         "(hash, account, signature, valid, type, previous, source, representative, destination, balance, timestamp, genesis_distance) " ..
      "VALUES(?,       ?,         ?,     ?,    ?,        ?,      ?,              ?,           ?,       ?,         ?,                ?)", db:errmsg()))
    
    sql.bootstrap_block_set = assert(db:prepare("INSERT OR REPLACE INTO disktmp.blocks " ..
         "(hash, account, signature, valid, type, previous, source, representative, destination, balance, timestamp, genesis_distance) " ..
      "VALUES(?,       ?,         ?,     ?,    ?,        ?,      ?,              ?,           ?,       ?,         ?,                ?)", db:errmsg()))
    
    sql.block_update_ledger_validation = assert(db:prepare("UPDATE blocks SET valid = ?, genesis_distance = ? WHERE hash = ?"), db:errmsg())
    
    sql.get_child_hashes = assert(db:prepare("SELECT hash FROM blocks WHERE previous = ? OR source = ?"), db:errmsg())
    
    sql.blocks_count = assert(db:prepare("SELECT count(*) FROM blocks"), db:errmsg())
    sql.bootstrapped_blocks_count = assert(db:prepare("SELECT count(*) FROM disktmp.blocks"), db:errmsg())
    
    setmetatable(Block, BlockDB_meta)
  end,
  
  shutdown = function()
    for _, stmt in pairs(sql) do
      stmt:finalize()
    end
  end,
}

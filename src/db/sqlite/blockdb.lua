local Block
local sqlite3 = require "lsqlite3"

local schema = function(tbl_type, tbl_name, skip_aux_indices)
  local _, tbl = tbl_name:match("^(.+%.)(.+)")
  if not tbl then tbl = tbl_name end
  local schema = [[
  CREATE ]]..tbl_type..[[ IF NOT EXISTS ]]..tbl_name..[[ (
    hash                 TEXT,
    raw                  TEXT,
    
    account              TEXT,
    valid                INTEGER, --0: invalid
                                  --1: PoW ok
                                  --2: signature ok
                                  --3: ledger check ok
                                  --4: confirmed
    
    n                    INTEGER, --block number in account (OPEN = 0)
    type                 TEXT,
    prev_block           TEXT, --send, receive, change
    source               TEXT, --open, receive (source account)
    representative       TEXT, --open, change  (rep account)
    destination          TEXT, --send (destination account)
    balance              TEXT,

    PRIMARY KEY(hash)
  ) WITHOUT ROWID;
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_account_idx      ON ]]..tbl..[[ (account);
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_valid_idx        ON ]]..tbl..[[ (valid);
  ]]
  if not skip_aux_indices then
    schema = schema .. [[CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_type_idx         ON ]]..tbl..[[ (type); ]]
  end
  schema = schema .. [[
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_prev_idx         ON ]]..tbl..[[ (prev_block);
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_source_idx       ON ]]..tbl..[[ (source); ]]
  if not skip_aux_indices then
    schema = schema .. [[
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_rep_idx          ON ]]..tbl..[[ (representative);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_dst_idx          ON ]]..tbl..[[ (destination);
    CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_balance_idx      ON ]]..tbl..[[ (balance);
    ]]
  end
  return  schema
end

local sql={}

local cache = setmetatable({}, {__mode="v"})
local cache_enabled = true

local db
local BlockDB_meta = {__index = {
  find = function(hash, opt)
    local block = cache_enabled  and rawget(cache, hash) or nil
    if block == nil then
      local stmt = opt=="bootstrap" and sql.bootstrap_block_get or sql.block_get
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
      return nil
    else
      return block
    end
  end,
  
  find_by_account = function(acct, opt)
    local blocks = {}
    local stmt = opt == "bootstrap" and sql.bootstrap_block_get_by_acct or sql.block_get_by_acct
    stmt:bind(1, acct)
    for _, block in stmt:nrows() do
      table.insert(blocks, Block.new(block))
    end
    stmt:reset()
    return blocks
  end,
  
  store = function(self, opt)
    if self.type == "open" then
      self.n = 1
    end
    
    local stmt = opt == "bootstrap" and sql.bootstrap_block_set  or sql.block_set
    stmt:bind(1, self.hash)
    stmt:bind(2, self.raw or self:pack())
    stmt:bind(3, self.account) -- could be nil, and that's okay
    
    local valid = self.valid
    if not valid then
      stmt:bind(4, 0)
    elseif valid == "PoW" then
      stmt:bind(4, 1)
    elseif valid == "signature" then
      stmt:bind(4, 2)
    elseif valid == "ledger" then
      stmt:bind(4, 3)
    elseif valid == "confirmed" then
      stmt:bind(4, 4)
    else
      error("unknown block validation state " .. tostring(valid))
    end
    
    stmt:bind(5, self.n)  --ok if nil
    stmt:bind(6, self.type) --TODO: use typecode for packing efficiency
    stmt:bind(7, self.prev_block)
    stmt:bind(8, self.source)
    stmt:bind(9, self.representative)
    stmt:bind(10, self.destination)
    stmt:bind(11, self.balance and self.balance:pack() or nil)
    stmt:step()
    --TODO: check for sqlite3.BUSY and such responses
    stmt:reset()
    if cache_enabled then
      --update cache
      rawset(cache, self.hash, self)
    end
    
  end,
  
  batch_store = function(batch, opt)
    assert(db:exec("BEGIN EXCLUSIVE TRANSACTION") == sqlite3.OK, db:errmsg())
    for _, block in ipairs(batch) do
      block:store(opt)
    end
    assert(db:exec("COMMIT TRANSACTION") == sqlite3.OK, db:errmsg())
  end
}}

return {
  initialize = function(db_ref)
    Block = require "prailude.block"
    db = db_ref
    assert(db:exec(schema("TABLE", "blocks")) == sqlite3.OK, db:errmsg())
    assert(db:exec(schema("TEMPORARY TABLE", "bootstrap_blocks", true)) == sqlite3.OK, db:errmsg())
    
    sql.block_get = assert(db:prepare("SELECT * FROM blocks WHERE hash = ?"), db:errmsg())
    sql.bootstrap_block_get = assert(db:prepare("SELECT * FROM bootstrap_blocks WHERE hash = ?"), db:errmsg())
    
    sql.block_get_by_acct = assert(db:prepare("SELECT * FROM blocks WHERE account = ? ORDER BY n DESC"), db:errmsg())
    sql.bootstrap_block_get_by_acct = assert(db:prepare("SELECT * FROM bootstrap_blocks WHERE account = ? ORDER BY n DESC"), db:errmsg())
    
    sql.block_set = assert(db:prepare("INSERT OR REPLACE INTO blocks " ..
         "(hash, raw, account, valid, n, type, prev_block, source, representative, destination, balance) " ..
      "VALUES(?,   ?,       ?,     ?, ?,    ?,          ?,      ?,              ?,           ?,       ?)", db:errmsg()))
    
    sql.bootstrap_block_set = assert(db:prepare("INSERT OR REPLACE INTO bootstrap_blocks " ..
         "(hash, raw, account, valid, n, type, prev_block, source, representative, destination, balance) " ..
      "VALUES(?,   ?,       ?,     ?, ?,    ?,          ?,      ?,              ?,           ?,       ?)", db:errmsg()))
      
    setmetatable(Block, BlockDB_meta)
  end,
  
  shutdown = function()
    for _, stmt in pairs(sql) do
      stmt:finalize()
    end
  end,
}

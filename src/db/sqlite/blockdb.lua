local Block
local sqlite3 = require "lsqlite3"

local schema = [[
  CREATE TABLE IF NOT EXISTS blocks (
    hash                 TEXT,
    raw                  TEXT,
    
    account              TEXT,
    valid                INTEGER, --0: invalid
                                  --1: PoW ok
                                  --4: sig ok
                                  --5: ledger check ok
                                  --6: confirmed
    
    n                    INTEGER, --block number in account (OPEN = 0)
    type                 TEXT,
    prev_block           TEXT, --send, receive, change
    source               TEXT, --open, receive (source account)
    representative       TEXT, --open, change  (rep account)
    destination          TEXT, --send (destination account)
    balance              TEXT,

    PRIMARY KEY(hash)
  ) WITHOUT ROWID;
  CREATE INDEX IF NOT EXISTS block_account_idx      ON blocks (account);
  CREATE INDEX IF NOT EXISTS block_valid_idx        ON blocks (valid);
  CREATE INDEX IF NOT EXISTS block_type_idx         ON blocks (type);
  CREATE INDEX IF NOT EXISTS block_prev_idx         ON blocks (prev_block);
  CREATE INDEX IF NOT EXISTS block_source_idx       ON blocks (source);
  CREATE INDEX IF NOT EXISTS block_rep_idx          ON blocks (representative);
  CREATE INDEX IF NOT EXISTS block_dst_idx          ON blocks (destination);
  CREATE INDEX IF NOT EXISTS block_balance_idx      ON blocks (balance);
  
  CREATE TABLE IF NOT EXISTS block_sources (
    hash              TEXT,
    peer              TEXT,
    source_type       TEXT,
    time              INTEGER
  );
  CREATE INDEX IF NOT EXISTS blocksource_peer_idx        ON block_sources (peer);
  CREATE INDEX IF NOT EXISTS blocksource_type_idx        ON block_sources (source_type);
  --CREATE INDEX IF NOT EXISTS blocksource_time_idx        ON block_sources (time);
]]

local cache = setmetatable({}, {__mode="v"})
local cache_enabled = true

local block_get, block_set, block_get_by_acct

local db
local BlockDB_meta = {__index = {
  find = function(hash)
    local block = cache_enabled  and rawget(cache, hash) or nil
    if block == nil then
      block_get:bind(1, hash)
      block = block_get:nrows()(block_get)
      --TODO: check for sqlite3.BUSY and such responses
      block_get:reset()
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
  
  find_by_account = function(acct)
    local blocks = {}
    block_get_by_acct:bind(1, acct)
    for _, block in block_get_by_acct:nrows() do
      table.insert(blocks, Block.new(block))
    end
    block_get_by_acct:reset()
    return blocks
  end,
  
  store = function(self)
    if self.type == "open" then
      self.n = 1
    end
    
    block_set:bind(1, self.hash)
    block_set:bind(2, self.raw or self:pack())
    block_set:bind(3, self.account) -- could be nil, and that's okay
    
    local valid = self.valid
    if not valid then
      block_set:bind(4, 0)
    elseif valid == "PoW" then
      block_set:bind(4, 1)
    elseif valid == "signature" then
      block_set:bind(4, 2)
    elseif valid == "ledger" then
      block_set:bind(4, 3)
    elseif valid == "confirmed" then
      block_set:bind(4, 4)
    else
      error("unknown block validation state " .. tostring(valid))
    end
    
    block_set:bind(5, self.n)  --ok if nil
    block_set:bind(6, self.type) --TODO: use typecode for packing efficiency
    block_set:bind(7, self.prev_block)
    block_set:bind(8, self.source)
    block_set:bind(9, self.representative)
    block_set:bind(10, self.destination)
    block_set:bind(11, self.balance and self.balance:pack() or nil)
    block_set:step()
    --TODO: check for sqlite3.BUSY and such responses
    block_set:reset()
    if cache_enabled then
      --update cache
      rawset(cache, self.hash, self)
    end
    
  end
}}



return {
  initialize = function(db_ref)
    Block = require "prailude.block"
    db = db_ref
    if db:exec(schema) ~= sqlite3.OK then
      error(db:errmsg())
    end
    block_get = assert(db:prepare("SELECT * FROM blocks WHERE hash = ?"), db:errmsg())
    block_get_by_acct = assert(db:prepare("SELECT * FROM blocks WHERE account = ? ORDER BY n DESC"), db:errmsg())
    block_set = assert(db:prepare("INSERT OR REPLACE INTO blocks " ..
      "(hash, raw, account, valid, n, type, prev_block, source, representative, destination, balance) " ..
      "VALUES(?,?,       ?,     ?, ?,    ?,          ?,      ?,              ?,           ?,       ?)", db:errmsg()))
    setmetatable(Block, BlockDB_meta)
  end,
  
  shutdown = function()
    block_get:finalize()
    block_get_by_acct:finalize()
    block_set:finalize()
  end
}

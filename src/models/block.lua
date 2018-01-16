--eh...

local Util = require "prailude.util"

local verify_block_PoW = Util.work.verify
local verify_block_PoW_test = Util.work.verify_test
local blake2b_hash = Util.blake2b.hash
local mm = require "mm"

local schema = [[
  CREATE TABLE IF NOT EXISTS blocks (
    hash                 TEXT,
    raw                  TEXT,
    
    valid                INTEGER, --0: invalid
                                  --1: PoW ok
                                  --2: sig ok
                                  --3: ledger check ok
                                  --4: confirmed
    
    type                 TEXT,
    prev_block           TEXT, --send, receive, change
    source_block         TEXT, --open, receive
    representative_acct  TEXT, --open, change
    destination_acct     TEXT, --send
    balance              TEXT,

    PRIMARY KEY(hash)
  ) WITHOUT ROWID;
  CREATE INDEX IF NOT EXISTS block_valid_idx        ON blocks (valid);
  CREATE INDEX IF NOT EXISTS block_type_idx         ON blocks (type);
  CREATE INDEX IF NOT EXISTS block_prev_idx         ON blocks (prev_block);
  CREATE INDEX IF NOT EXISTS block_source_idx       ON blocks (source_block);
  CREATE INDEX IF NOT EXISTS block_rep_idx          ON blocks (representative_acct);
  CREATE INDEX IF NOT EXISTS block_dst_idx          ON blocks (destination_acct);
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
  
  CREATE TABLE IF NOT EXISTS frontiers (
    account           TEXT,
    hash              TEXT,
    PRIMARY KEY(account)
  ) WITHOUT ROWID;
  
  CREATE INDEX IF NOT EXISTS frontiers_hash_idx        ON frontiers (hash);
]]


local db

local Block_instance = {
  hashable = function(self)
    local blocktype = self.type
    if blocktype == "open" then
      return self.account
    else
      return self.previous
    end
  end,
  hash = function(self)
    return blake2b_hash(self.raw, 32)
  end,
  
  verify = function(self)
    if not self:verify_PoW() then
      return nil, "insufficient proof of work"
    elseif not self:verify_signature() then
      return nil, "bad signature"
    elseif not self:verify_consistency() then
      return nil, "block inconsistent with ledger"
    else
      return true
    end
  end,
  verify_PoW = function(self)
    return verify_block_PoW(self:hashable(), self.work)
  end,
  verify_test_PoW = function(self)
    return verify_block_PoW_test(self:hashable(), self.work)
  end,
  verify_signature = function(self, account)
    return true --not implemented yet
  end,
  verify_consistency = function(self)
    return true -- not implemented yet
  end
}


local block_meta = {__index = Block_instance}
local Block = {}

function Block.new(block_type, data)
  if data then
    assert(type(block_type) == "string")
    data.type = block_type
  elseif type(block_type) == "table" then
    data = block_type
    assert(data.type, "block type is required")
  else
    assert(type(block_type) == "string")
    data = {type = block_type}
  end
  return setmetatable(data, block_meta)
end

function Block.find(hash)
  --TODO
end

function Block.get(data)
  local block = Block.new(data)
  if not block:verify_PoW() then
    return nil, "PoW check failed"
  elseif not block:verify_signature() then
    return nil, "Signature check failed"
  else
    return block
  end
end

function Block.initialize(db_ref)
  db = db_ref
  db:exec(schema)
  print(db:errmsg())
end

return Block

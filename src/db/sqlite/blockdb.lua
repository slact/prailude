local Block
local sqlite3 = require "lsqlite3"

local schema = [[
  CREATE TABLE IF NOT EXISTS blocks (
    hash                 TEXT,
    raw                  TEXT,
    
    valid                INTEGER, --0: invalid
                                  --1: PoW ok
                                  --2: sig ok
                                  --3: ledger check ok
                                  --4: confirmed
    
    n                    INTEGER, --block number in account (OPEN = 0)
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
]]


local db
local BlockDB_meta = {__index = function(_, k)
  error(tostring(k) .. " not yet implemented")
end}

return {
  initialize = function(db_ref)
    Block = require "prailude.block"
    db = db_ref
    if db:exec(schema) ~= sqlite3.OK then
      error(db:errmsg())
    end
    setmetatable(Block, BlockDB_meta)
  end
}

local Account
local sqlite3 = require "lsqlite3"

local schema = [[
  CREATE TABLE IF NOT EXISTS accounts (
    id                    TEXT,
    last_block            TEXT,
    num_blocks            INTEGER, --number of blocks in account (including OPEN)
    representative_acct   TEXT,
    
    balance_raw           TEXT,
    balance               REAL,
    
    delegated_balance     REAL, --inexactness is ok here
    
    PRIMARY KEY(id)
  ) WITHOUT ROWID;
  
  CREATE INDEX IF NOT EXISTS account_frontier_idx   ON accounts (last_block);
  CREATE INDEX IF NOT EXISTS account_rep_idx        ON accounts (representative_acct);
  CREATE INDEX IF NOT EXISTS account_balance_idx    ON accounts (balance);
]]


local db
local AccountDB_meta = {__index = function(_, k)
  error(tostring(k) .. " not yet implemented")
end}

return {
  initialize = function(db_ref)
    Account = require "prailude.account"
    db = db_ref
    if db:exec(schema) ~= sqlite3.OK then
      error(db:errmsg())
    end
    setmetatable(Account, AccountDB_meta)
  end
}

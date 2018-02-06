local Account
local sqlite3 = require "lsqlite3"

local schema = [[
  CREATE TABLE IF NOT EXISTS accounts (
    id                    TEXT,
    frontier              TEXT,
    representative_acct   TEXT,
    
    balance_raw           TEXT,
    balance               REAL,
    
    delegated_balance     REAL, --inexactness is ok here
    
    source_peer           TEXT,
    
    PRIMARY KEY(id)
  ) WITHOUT ROWID;
  
  CREATE INDEX IF NOT EXISTS account_frontier_idx   ON accounts (frontier);
  CREATE INDEX IF NOT EXISTS account_rep_idx        ON accounts (representative_acct);
  CREATE INDEX IF NOT EXISTS account_balance_idx    ON accounts (balance);
]]

local cache = setmetatable({}, {__mode="kv"})

local account_get, account_set, account_update, account_update2
local db
local AccountDB_meta = {__index = {
  find = function(id)
    local acct = rawget(cache, id)
    if acct then
      return acct
    elseif acct == false then
      return nil
    else
      account_get:bind(id)
      acct = account_get:nrows()(account_get)
      account_get:reset()
      if acct then
        acct = Account.new(acct)
      end
      rawset(cache, id, acct or false)
      return acct
    end
  end,
  
  store = function(self)
    account_set:bind(1, self.id)
    account_set:bind(2, self.frontier)
    account_set:bind(3, self.representative_acct)
    if self.balance_raw then
      account_set:bind(4, self.balance_raw:pack())
      account_set:bind(5, tonumber(tostring(self.balance_raw)))
    else
      account_set:bind(4, nil)
      account_set:bind(5, nil)
    end
    account_set:bind(6, self.delegated_balance)
    
    if self.source_peer then
      account_set:bind(7, tostring(self.source_peer))
    else
      account_set:bind(7, nil)
    end
    
    account_set:step()
    --TODO: check for sqlite3.BUSY and such responses
    account_set:reset()
    
    --cache update
    rawset(cache, self.id, self)
    
    return self
  end,
  
  update = function(self, what)
    if     what == "balance" or what == "balance_raw" then
      account_update2:bind(1, "balance_raw")
      account_update2:bind(2, "balance")
      account_update2:bind(3, self.balance:pack())
      account_update2:bind(4, tonumber(tostring(self.balance)))
      account_update2:bind(5, self.id)
      account_update2:step()
      --TODO: check for sqlite3.BUSY and such responses
      account_update2:reset()
    elseif what == "delegated_balance" then
      account_update:bind(1, "delegated_balance")
      account_update:bind(2, self.delegated_balance)
      account_update:bind(3, self.id)
      account_update:step()
      --TODO: check for sqlite3.BUSY and such responses
      account_update:reset()
    else
      account_update:bind(1, what)
      account_update:bind(2, self[what])
      account_update:bind(3, self.id)
      account_update:step()
      --TODO: check for sqlite3.BUSY and such responses
      account_update:reset()
    end
    return self
  end
}}

return {
  initialize = function(db_ref)
    Account = require "prailude.account"
    db = db_ref
    if db:exec(schema) ~= sqlite3.OK then
      error(db:errmsg())
    end
    
    account_get = assert(db:prepare("SELECT * FROM accounts WHERE id = ?"), db:errmsg())
    account_set = assert(db:prepare("INSERT OR REPLACE INTO accounts (id, frontier, num_blocks, representative_acct, balance_raw, balance, delegated_balance, source_peer) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"), db:errmsg())
    account_update = assert(db:prepare("UPDATE accounts (?) VALUES(?) WHERE id = ?"), db:errmsg())
    account_update2 = assert(db:prepare("UPDATE accounts (?, ?) VALUES(?, ?) WHERE id = ?"), db:errmsg())
    
    setmetatable(Account, AccountDB_meta)
  end,
  
  shutdown = function()
    account_get:finalize()
    account_set:finalize()
    account_update:finalize()
    account_update2:finalize()
  end
}

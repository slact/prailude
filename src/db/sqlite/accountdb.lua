local Account
local sqlite3 = require "lsqlite3"

local schema = [[
  CREATE TABLE IF NOT EXISTS accounts (
    id                    TEXT,
    frontier              TEXT,
    num_blocks            INTEGER,
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

local cache = setmetatable({}, {__mode="v"})

local account_get, account_set, account_update, account_update_balance = nil, nil, {}, nil
local db
local AccountDB_meta = {__index = {
  find = function(id)
    local acct = rawget(cache, id)
    if acct then
      return acct
    elseif acct == false then
      return nil
    else
      account_get:bind(1, id)
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
    account_set:bind(3, self.num_blocks)
    account_set:bind(4, self.representative_acct)
    if self.balance_raw then
      account_set:bind(5, self.balance_raw:pack())
      account_set:bind(6, tonumber(tostring(self.balance_raw)))
    else
      account_set:bind(5, nil)
      account_set:bind(6, nil)
    end
    account_set:bind(7, self.delegated_balance)
    
    if self.source_peer then
      account_set:bind(8, tostring(self.source_peer))
    else
      account_set:bind(8, nil)
    end
    
    account_set:step()
    --TODO: check for sqlite3.BUSY and such responses
    account_set:reset()
    
    --cache update
    rawset(cache, self.id, self)
    
    return self
  end,
  
  update = function(self, what)
    if what == "balance" or what == "balance_raw" then
      account_update_balance:bind(1, tonumber(tostring(self.balance)))
      account_update_balance:bind(2, self.balance:pack())
      account_update_balance:bind(3, self.id)
      account_update_balance:step()
      --TODO: check for sqlite3.BUSY and such responses
      account_update_balance:reset()
    else
      local stmt = assert(account_update[what], what .. " is not a valid account field")
      stmt:bind(1, self[what])
      stmt:bind(2, self.id)
      stmt:step()
      --TODO: check for sqlite3.BUSY and such responses
      stmt:reset()
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
    account_update_balance = assert(db:prepare("UPDATE accounts SET balance = ?, balance_raw = ? WHERE id = ?"), db:errmsg())
    
    for _, n in ipairs {"frontier", "representative_acct", "delegated_balance", "num_blocks", "source_peer"} do
      account_update[n]=assert(db:prepare("UPDATE accounts SET " .. n .. " = ? WHERE id = ?"), n .. ":  " .. db:errmsg())
    end
    
    setmetatable(Account, AccountDB_meta)
  end,
  
  shutdown = function()
    account_get:finalize()
    account_set:finalize()
    for _, v in pairs(account_update) do
      v:finalize()
    end
    account_update_balance:finalize()
  end
}

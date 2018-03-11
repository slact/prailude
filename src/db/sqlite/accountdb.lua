local Account
local sqlite3 = require "lsqlite3"
local Util = require "prailude.util"

local function schema(tbl_type, tbl_name)
  local _, tbl = tbl_name:match("^(.+%.)(.+)")
  if not tbl then tbl = tbl_name end
  return [[
  CREATE ]]..tbl_type..[[ IF NOT EXISTS ]]..tbl_name..[[ (
    id                    BLOB,
    frontier              BLOB,
    num_blocks            INTEGER,
    representative_acct   BLOB,
    
    balance_raw           BLOB,
    balance               REAL,
    
    delegated_balance     REAL, --inexactness is ok here
    
    valid                 INTEGER NOT NULL DEFAULT 0,
    
    genesis_distance      INTEGER, -- number of accounts to reach genesis account
    genesis_block_distance INTEGER, -- number of blocks to reach genesis
    source_peer           TEXT,
    PRIMARY KEY(id)
  ) WITHOUT ROWID;
  
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_frontier_idx   ON ]]..tbl..[[ (frontier);
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_checked_idx    ON ]]..tbl..[[ (valid);
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_rep_idx        ON ]]..tbl..[[ (representative_acct);
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_balance_idx    ON ]]..tbl..[[ (balance);
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_frontier_idx   ON ]]..tbl..[[ (frontier);
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_genesis_distance_idx ON ]]..tbl..[[ (genesis_distance);
  ]]
end

local sql = {}

local cache = Util.Cache("weak")
local cache_bootstrap = Util.Cache("weak")

local account_update, bootstrap_account_update = {}, {}
local db
local AccountDB_meta = {__index = {
  find = function(id, opt)
    local mycache = opt == "bootstrap" and cache_bootstrap or cache
    local acct = mycache:get(id)
    if acct then
      return acct
    elseif acct == false then
      return nil
    else
      local stmt = opt == "bootstrap" and sql.boostrap_account_get or sql.account_get
      stmt:bind(1, id)
      acct = stmt:nrows()(stmt)
      stmt:reset()
      if acct then
        acct = Account.new(acct)
      end
      mycache:set(id, acct or false)
      return acct
    end
  end,
  
  store = function(self, opt)
    local stmt, mycache
    if opt == "bootstrap" then
      stmt, mycache = sql.bootstrap_account_update, cache_bootstrap
    else
      stmt, mycache = sql.account_set, cache
    end
  
    stmt:bind(1, self.id)
    stmt:bind(2, self.frontier)
    stmt:bind(3, self.num_blocks)
    stmt:bind(4, self.representative_acct)
    if self.balance_raw then
      stmt:bind(5, self.balance_raw:pack())
      stmt:bind(6, tonumber(tostring(self.balance_raw)))
    else
      stmt:bind(5, nil)
      stmt:bind(6, nil)
    end
    stmt:bind(7, self.delegated_balance)
    
    stmt:bind(8, self.valid)
    
    stmt:bind(9, self.genesis_distance);
    stmt:bind(10, self.genesis_nlock_distance);
    if self.source_peer then
      stmt:bind(11, tostring(self.source_peer))
    else
      stmt:bind(11, nil)
    end
    
    stmt:step()
    --TODO: check for sqlite3.BUSY and such responses
    stmt:reset()
    
    --cache update
    
    mycache:set(self.id, self)
    return self
  end,
  
  update = function(self, what)
    
    if what == "balance" or what == "balance_raw" then
      local stmt = sql.account_update_balance
      stmt:bind(1, tonumber(tostring(self.balance)))
      stmt:bind(2, self.balance:pack())
      stmt:bind(3, self.id)
      stmt:step()
      stmt:reset()
    else
      
      local stmt = assert(account_update[what], what .. " is not a valid account field")
      stmt:bind(1, self[what])
      stmt:bind(2, self.id)
      stmt:step()
      stmt:reset()
    end
    return self
  end,
  
  get_frontier_and_genesis_distance = function(account_id)
    local stmt = sql.account_get_frontier_and_genesis_distance
    stmt:bind(1, account_id)
    local frontier, distance = stmt:urows()(stmt)
    stmt:reset()
    return frontier, distance
  end,
  
  get_range_sorted_by_genesis_distance = function(limit, offset)
    local accts = {}
    for data in db:nrows(("SELECT * FROM accounts ORDER BY genesis_distance ASC LIMIT %i offset %i"):format(limit, offset or 0)) do
      table.insert(accts. Account.new(data))
    end
    return accts
  end
}}

return {
  initialize = function(db_ref)
    Account = require "prailude.account"
    db = db_ref
    assert(db:exec(schema("TABLE", "accounts")) == sqlite3.OK, db:errmsg())
    
    sql.account_get = assert(db:prepare("SELECT * FROM accounts WHERE id = ?"), db:errmsg())
    
    sql.account_get_frontier_and_genesis_distance = assert(db:prepare("SELECT frontier, genesis_distance FROM accounts WHERE id = ?"), db:errmsg())
    
    sql.account_set = assert(db:prepare("INSERT OR REPLACE INTO accounts " ..
      "      (id, frontier, num_blocks, representative_acct, balance_raw, balance, delegated_balance, valid, genesis_distance, genesis_block_distance, source_peer) " ..
      "VALUES(?,         ?,          ?,                   ?,           ?,       ?,                 ?,     ?,                ?,                      ?,           ?)"), db:errmsg())
    
    sql.account_update_balance = assert(db:prepare("UPDATE accounts SET balance = ?, balance_raw = ? WHERE id = ?"), db:errmsg())
    
    for _, n in ipairs {"frontier", "representative_acct", "delegated_balance", "num_blocks", "valid", "source_peer"} do
      account_update[n]=assert(db:prepare("UPDATE accounts SET " .. n .. " = ? WHERE id = ?"), n .. ":  " .. db:errmsg())
      bootstrap_account_update[n]=assert(db:prepare("UPDATE accounts SET " .. n .. " = ? WHERE id = ?"), n .. ":  " .. db:errmsg())
    end
    
    setmetatable(Account, AccountDB_meta)
  end,
  
  shutdown = function()
    for _, sql_stmts in ipairs{sql, account_update, bootstrap_account_update} do
      for _, v in pairs(sql_stmts) do
        v:finalize()
      end
    end
  end,
  
  batch_store = function(batch, opt)
    assert(db:exec("BEGIN EXCLUSIVE TRANSACTION") == sqlite3.OK, db:errmsg())
    for _, account in ipairs(batch) do
      account:store(opt)
    end
    assert(db:exec("COMMIT TRANSACTION") == sqlite3.OK, db:errmsg())
  end
}

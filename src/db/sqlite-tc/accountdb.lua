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
    representative        BLOB,
    
    delegated_balance     REAL, --inexactness is ok here
    
    behind                INTEGER NOT NULL DEFAULT 0,
    --valid                 INTEGER NOT NULL DEFAULT 0,
    
    --genesis_distance      INTEGER, -- number of accounts to reach genesis account
    --genesis_block_distance INTEGER, -- number of blocks to reach genesis
    source_peer           TEXT,
    PRIMARY KEY(id)
  ) WITHOUT ROWID;
  
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_frontier_idx   ON ]]..tbl..[[ (frontier);
  --CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_checked_idx    ON ]]..tbl..[[ (valid);
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_rep_idx        ON ]]..tbl..[[ (representative);
  CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_frontier_idx   ON ]]..tbl..[[ (frontier);
  --CREATE INDEX IF NOT EXISTS ]]..tbl_name..[[_genesis_distance_idx ON ]]..tbl..[[ (genesis_distance);
  ]]
end

local sql = {}

local cache = Util.Cache("weak")
local cache_bootstrap = Util.Cache("weak")

local account_update, bootstrap_account_update = {}, {}
local db
local AccountDB_meta = {__index = {
  find = function(id, opt)
    assert(type(id)=="string")
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
        if acct.behind == 0 or acct.behind == "0" then
          acct.behind = false
        elseif acct.behind == 1 or acct.behind == "1" then
          acct.behind = false
        else
          error("invalid account.behind value " .. tostring(acct.behind))
        end
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
    stmt:bind(3, self.representative)
    stmt:bind(4, self.delegated_balance)
    
    if self.behind == 0 or self.behind == "0" then
      self.behind = false
    end
    stmt:bind(5, self.behind and 1 or 0)
    
    if self.source_peer then
      stmt:bind(6, tostring(self.source_peer))
    else
      stmt:bind(6, nil)
    end
    
    stmt:step()
    --TODO: check for sqlite3.BUSY and such responses
    stmt:reset()
    
    --cache update
    mycache:set(self.id, self)
    return self
  end,
  
  update = function(self, what, no_cache_update)
    local stmt = assert(account_update[what], "not a valid account field")
    stmt:bind(1, self[what])
    stmt:bind(2, self.id)
    stmt:step()
    stmt:reset()
    if not no_cache_update then
      cache:set(self.id, self)
    end
    return self
  end,
  
  create_later = function(self)
    self:save_later("CREATE")
  end,
  save_later = function(self, what)
    assert(type(what)=="string")
    local slist = rawget(self, "__save_list")
    if not slist then
      slist = {}
      rawset(self, "__save_list", slist)
    end
    slist[what] = true
    --cache update
    cache:set(self.id, self)
    return self
  end,
  save = function(self)
    local slist = rawget(self, "__save_list")
    if not slist then return nil end
    if slist.CREATE then
      return self:store()
    end
    cache:set(self.id, self)
    for what, _ in pairs(slist) do --not the most efficient, but whatever
      self:update(what, nil, true)
    end
    return self
  end,
  
  get_frontier = function(account_id)
    local stmt = sql.account_get_frontier
    stmt:bind(1, account_id)
    local frontier = stmt:urows()(stmt)
    stmt:reset()
    return frontier
  end
}}

return {
  initialize = function(db_ref)
    Account = require "prailude.account"
    db = db_ref
    assert(db:exec(schema("TABLE", "accounts")) == sqlite3.OK, db:errmsg())
    
    sql.account_get = assert(db:prepare("SELECT * FROM accounts WHERE id = ?"), db:errmsg())
    
    sql.account_get_frontier = assert(db:prepare("SELECT frontier FROM accounts WHERE id = ?"), db:errmsg())
    
    sql.account_set = assert(db:prepare("INSERT OR REPLACE INTO accounts " ..
      "      (id, frontier, representative, delegated_balance, behind, source_peer) " ..
      "VALUES(?,         ?,              ?,                 ?,      ?,           ?)"), db:errmsg())
    
    for _, n in ipairs {"frontier", "representative", "delegated_balance", "behind", "source_peer"} do
      account_update[n]=assert(db:prepare("UPDATE accounts SET " .. n .. " = ? WHERE id = ?"), n .. ":  " .. db:errmsg())
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

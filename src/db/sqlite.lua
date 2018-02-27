local sqlite3 = require "lsqlite3"
local Bus = require "prailude.bus"

local subdbs = {
  require "prailude.db.sqlite.peer",
  require "prailude.db.sqlite.block",
  require "prailude.db.sqlite.frontier",
  require "prailude.db.sqlite.account",
  require "prailude.db.sqlite.kv",
}

local dbs = {}
local DB = {}

function DB.pragma(db, pragma_list)
  for pragma, val in pairs(pragma_list) do
    if val == false then
      val = "OFF"
    elseif val == true then
      val = "ON"
    elseif val == nil then
      error("Invalid nil PRAGMA " .. pragma)
    end
    assert(db:exec(("PRAGMA %s = %s"):format(pragma, tostring(val))) == sqlite3.OK, "PRAGMA " .. pragma .. " error: " .. db:errmsg())
  end
end

function DB.open(name, opt)
  assert(not dbs[name], "db with name " .. name .. " already exists")
  local db, err_code, err_msg = sqlite3.open(opt.db_file or name..".db")
  if not db then
    error("error opening db  " .. name .. ": " .. (err_msg or err_code))
  end
  DB.pragma(db, opt.pragma)
  dbs[name]=db
  return db
end

function DB.db(name)
  return assert(dbs[name])
end

function DB.initialize()
  --opt = opt or {}
  local db = DB.open("raiblocks", {
    pragma = {
      synchronous = false, --don't really care if the db lags behind on crash
      journal_mode = "TRUNCATE",
      temp_store = "FILE",
      foreign_keys = "OFF",
      locking_mode = "EXCLUSIVE",
      cache_size = "1000000"
    }
  })
  assert(db:exec("ATTACH DATABASE ':memory:' as mem") == sqlite3.OK, db:errmsg())
  assert(db:exec("ATTACH DATABASE 'bootstrap.db' as bootstrap") == sqlite3.OK, db:errmsg())
  DB.pragma(db, {
    ["bootstrap.synchronous"] = false, --don't really care if the db lags behind on crash
    ["bootstrap.journal_mode"] = false,
    ["bootstrap.foreign_keys"] = false,
  })
  
  for _, subdb in ipairs(subdbs) do
    subdb.initialize(db)
  end
  Bus.sub("shutdown", DB.shutdown)
end
  
function DB.shutdown()
  for _, subdb in ipairs(subdbs) do
    if subdb.shutdown then
      subdb.shutdown()
    end
  end
  for _, db in ipairs(dbs) do
    db:close()
  end
end

return DB

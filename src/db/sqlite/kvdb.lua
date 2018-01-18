local sqlite3 = require "lsqlite3"

local schema = [[
  CREATE TABLE IF NOT EXISTS kv (
    k         TEXT,
    v         TEXT,
    PRIMARY KEY(k)
  ) WITHOUT ROWID;
]]

local db

local kv_get, kv_set

local KvDB = {}

function KvDB.initialize(db_ref)
  db = db_ref
  if db:exec(schema) ~= sqlite3.OK then
    error(db:errmsg())
  end
  kv_get = assert(db:prepare("SELECT v FROM kv WHERE k = ?"), db:errmsg())
  kv_set = assert(db:prepare("INSERT OR REPLACE INTO kv (k, v) VALUES( ?, ? )"), db:errmsg())
end

function KvDB.shutdown()
  kv_get:finalize()
  kv_set:finalize()
end

function KvDB.get(k)
  if kv_get:bind(1, k) ~= sqlite3.OK then
    error(db:errmsg())
  end
  local val = kv_get:urows()(kv_get)
  kv_get:reset()
  return val
end

function KvDB.set(k, v)
  if kv_set:bind(1, k) ~= sqlite3.OK or kv_set:bind(2, v) ~= sqlite3.OK then
    error(db:errmsg())
  end
  kv_set:step()
  --TODO: check and account for sqlite3.BUSY and such responses
  kv_set:reset()
  return v
end

return KvDB

local sqlite3 = require "lsqlite3"
local log = require "prailude.log"
local Peer

local schema = function(tbl_type, tbl_name)
  return [[
  CREATE ]] .. tbl_type .. [[ IF NOT EXISTS ]] .. tbl_name .. [[ (
    address                  TEXT,
    port                     INTEGER,
    last_received            REAL,
    last_sent                REAL,
    last_keepalive_sent      REAL,
    last_keepalive_received  REAL,
    ping                     REAL NOT NULL DEFAULT 100000,
    tcp_in_use               INTEGER NOT NULL DEFAULT 0,
    bootstrap_score          REAL NOT NULL DEFAULT 0,
    PRIMARY KEY(address, port)
  ) WITHOUT ROWID;
  CREATE INDEX IF NOT EXISTS peer_last_received_idx           ON ]] .. tbl_name .. [[ (last_received);
  CREATE INDEX IF NOT EXISTS peer_last_sent_idx               ON ]] .. tbl_name .. [[ (last_sent);
  CREATE INDEX IF NOT EXISTS peer_last_keepalive_sent_idx     ON ]] .. tbl_name .. [[ (last_keepalive_sent);
  CREATE INDEX IF NOT EXISTS peer_last_keepalive_received_idx ON ]] .. tbl_name .. [[ (last_keepalive_received);
  CREATE INDEX IF NOT EXISTS peer_ping_idx                    ON ]] .. tbl_name .. [[ (ping);
  CREATE INDEX IF NOT EXISTS peer_tcp_in_use_idx              ON ]] .. tbl_name .. [[ (tcp_in_use);
  CREATE INDEX IF NOT EXISTS peer_bootstrap_score_idx         ON ]] .. tbl_name .. [[ (bootstrap_score);
]]

end

local db

local sql = {}
local sql_peer_update_num = {}

local cache = setmetatable({}, {__mode = "v"})
local cache_enabled = true

local PeerDB_meta = {__index = {
  find = function(peer_addr, peer_port)
    local peer, id
    if cache_enabled then
      id = ("%s:%.0f"):format(peer_addr, peer_port)
      peer = cache_enabled and rawget(cache, id)
    else
      peer = nil
    end
    if peer == false then
      return nil
    elseif not peer then
      sql.find:bind(1, peer_addr)
      sql.find:bind(2, peer_port)
      peer = sql.find:nrows()(sql.find)
      sql.find:reset()
      if peer then
        peer = Peer.new(peer)
      end
      if cache_enabled then
        rawset(cache, id, peer or false)
      end
      return peer
    else
      return peer
    end
  end,
  
  store = function(self)
    sql.store:bind(1, self.address)
    sql.store:bind(2, self.port)
    sql.store:bind(3, self.last_received)
    sql.store:bind(4, self.last_sent)
    sql.store:bind(5, self.last_keepalive_sent)
    sql.store:bind(6, self.last_keepalive_received)
    sql.store:step()
    --TODO: error handling?
    sql.store:reset()
    
    if cache_enabled then
      --cache it
      rawset(cache, self.id, self)
    end
    return self
  end,
  
  get_best_bootstrap_peer = function()
  local stmt = sql.get_for_bootstrap
    stmt:bind(1, Peer.inactivity_timeout)
    stmt:bind(2, 1)
    local data = stmt:nrows()(stmt)
    stmt:reset()
    return Peer.new(data)
  end,
  
  get8 = function(except_peer)
    --TODO: cache this maybe? we probably shouldn't have to hit the DB for every keepalive...
    local select
    if except_peer then
      select = sql.get8_except
      select:bind(1, Peer.keepalive_interval)
      select:bind(2, except_peer.address)
      select:bind(3, except_peer.port)
    else
      select = sql.get8
      select:bind(1, Peer.keepalive_interval)
    end
    local peers = {}
    for row in select:nrows() do
      table.insert(peers, Peer.new(row))
    end
    select:reset()
    return peers
  end,
  
  get_active_needing_keepalive = function()
    --TODO: cache maybe? eh...
    local stmt = sql.get_active_to_keepalive
    stmt:bind(1, Peer.inactivity_timeout)
    stmt:bind(2, Peer.keepalive_interval)
    local peers = {}
    for row in stmt:nrows() do
      table.insert(peers, Peer.new(row))
    end
    stmt:reset()
    return peers
  end,
  
  get_active_count = function()
    --TODO: cache maybe? eh...
    local stmt = sql.get_active_count
    stmt:bind(1, Peer.inactivity_timeout)
    local count = stmt:urows()(stmt)
    stmt:reset()
    return count
  end,
  
  update_num_field = function(self, field)
    local stmt = sql_peer_update_num[field]
    assert(stmt, "unknown timestamp field")
    stmt:bind(1, self[field])
    stmt:bind(2, self.address)
    stmt:bind(3, self.port)
    stmt:step()
    stmt:reset()
    return self
  end,
  
  update_keepalive_ping = function(self, keepalive_received_time)
    local stmt = sql.update_keepalive_ping
    if keepalive_received_time then
      self.last_keepalive_received = keepalive_received_time
    else
      keepalive_received_time = self.last_keepalive_received
    end
    stmt:bind(1, keepalive_received_time)
    if not keepalive_received_time or not self.last_keepalive_send then
      stmt:bind(2, 1000)
    else
      stmt:bind(2, keepalive_received_time - self.last_keepalive_sent)
    end
    stmt:bind(3, self.address)
    stmt:bind(4, self.port)
    assert(stmt:step(), db:errmsg())
    stmt:reset()
    return self
  end,
}}

return {
  initialize = function(shared_db)
    Peer = require "prailude.peer"
    db = shared_db
    
    assert(db:exec(schema("TABLE", "stored_peers")) == sqlite3.OK, db:errmsg())
    assert(db:exec(schema("TEMPORARY TABLE", "peers")) == sqlite3.OK, db:errmsg())
    
    --copy previously known peers
    assert(db:exec("INSERT INTO peers SELECT * FROM stored_peers") == sqlite3.OK, db:errmsg())
    
    local numpeers
    for row in db:urows("SELECT count(*) FROM peers;") do
      numpeers = row
    end
    log:debug("loaded %i previously seen peers", numpeers)
    
  sql.store = "INSERT OR IGNORE INTO peers "..
              "      (address, port, last_received, last_sent, last_keepalive_sent, last_keepalive_received) "..
              "VALUES(      ?,    ?,             ?,         ?,                   ?,                       ?)"
  sql.find = "SELECT * FROM peers WHERE address=? AND port=? LIMIT 1"
  sql.get8_except = "SELECT * FROM peers WHERE last_keepalive_received > datetime('now') - ? "..
    "AND address !=? AND port != ? ORDER BY RANDOM() LIMIT 8"
  sql.get8 = "SELECT * FROM peers WHERE last_keepalive_received > datetime('now') - ? ORDER BY RANDOM() LIMIT 8"
  sql.get_active_to_keepalive="SELECT * FROM peers WHERE last_keepalive_received > datetime('now') - ? "..
    "AND last_keepalive_received < datetime('now') - ? ORDER BY RANDOM()"
  sql.get_active_count = "SELECT COUNT(*) FROM peers WHERE last_keepalive_received > datetime('now') - ?"
  sql.update_keepalive_ping = "UPDATE peers SET last_keepalive_received = ?, ping = ? WHERE address = ? AND port = ?"
  sql.get_for_bootstrap = "SELECT * FROM peers WHERE tcp_in_use != 1 AND last_keepalive_received NOT NULL "..
    "AND last_keepalive_received > datetime('now') - ? ORDER BY bootstrap_score DESC, ping ASC LIMIT ?"
  for k, v in pairs(sql) do
    sql[k] = assert(db:prepare(v), "SQL Error for " ..k..":  " .. tostring(db:errmsg()))
  end
  
  for _, v in pairs{"last_received", "last_sent", "last_keepalive_sent", "last_keepalive_received", "bootstrap_score", "tcp_in_use"} do
    sql_peer_update_num[v] = assert(db:prepare("UPDATE peers SET " .. v.." = ? WHERE address = ? AND port = ?"), db:errmsg())
  end
    
    setmetatable(Peer, PeerDB_meta)
  end,
  shutdown = function()
    --save previously known peers
    assert(db:exec("INSERT OR REPLACE INTO stored_peers SELECT * FROM peers") == sqlite3.OK, db:errmsg())
    assert(db:exec("UPDATE stored_peers SET tcp_in_use = 0, bootstrap_score = 0") == sqlite3.OK, db:errmsg()) --clear all tcp_in_use lock flags and bootstrap scores
    local numpeers
    for row in db:urows("SELECT count(*) FROM peers;") do
      numpeers = row
    end
    log:debug("stored %i seen peers", numpeers)
    for _, v in pairs(sql_peer_update_num) do
      v:finalize()
    end
    for _, v in pairs(sql) do
      v:finalize()
    end
  end
}

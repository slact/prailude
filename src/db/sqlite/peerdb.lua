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

local peer_find, peer_store, peer_get8, peer_get8_except, peer_get_active_to_keepalive, peer_get_fastest_ping, peer_update_keepalive_ping, peer_get_for_bootstrap
local peer_update_num = {}

local cache = setmetatable({}, {__mode = "kv"})

local PeerDB_meta = {__index = {
  find = function(peer_addr, peer_port)
    local id = ("%s:%.0f"):format(peer_addr, peer_port)
    local peer = rawget(cache, id)
    if peer == false then
      return nil
    elseif not peer then
      peer_find:bind(1, peer_addr)
      peer_find:bind(2, peer_port)
      peer = peer_find:nrows()(peer_find)
      peer_find:reset()
      if peer then
        peer = Peer.new(peer)
      end
      rawset(cache, id, peer or false)
      return peer
    else
      return peer
    end
  end,
  
  store = function(self)
    peer_store:bind(1, self.address)
    peer_store:bind(2, self.port)
    peer_store:bind(3, self.last_received)
    peer_store:bind(4, self.last_sent)
    peer_store:bind(5, self.last_keepalive_sent)
    peer_store:bind(6, self.last_keepalive_received)
    peer_store:step()
    --TODO: error handling?
    peer_store:reset()
    --cache it
    rawset(cache, self.id, self)
    return self
  end,
  
  get_best_bootstrap_peer = function()
    
  end,
  
  get_fastest_ping = function(n)
    peer_get_fastest_ping:bind(1, n or 1)
    local peers = {}
    for row in peer_get_fastest_ping:nrows() do
      table.insert(peers, Peer.new(row))
    end
    return peers
  end,
  
  get8 = function(except_peer)
    --TODO: cache this maybe? we probably shouldn't have to hit the DB for every keepalive...
    local select
    if except_peer then
      peer_get8_except:bind(1, Peer.keepalive_interval)
      peer_get8_except:bind(2, except_peer.address)
      peer_get8_except:bind(3, except_peer.port)
      select = peer_get8_except
    else
      peer_get8:bind(1, Peer.keepalive_interval)
      select = peer_get8
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
    local now = os.time()
    peer_get_active_to_keepalive:bind(1, now - Peer.inactivity_timeout)
    peer_get_active_to_keepalive:bind(2, now - Peer.keepalive_interval)
    local peers = {}
    for row in peer_get_active_to_keepalive:nrows() do
      table.insert(peers, Peer.new(row))
    end
    peer_get_active_to_keepalive:reset()
    return peers
  end,
  
  update_num_field = function(self, field)
    local stmt = peer_update_num[field]
    assert(stmt, "unknown timestamp field")
    stmt:bind(1, self[field])
    stmt:bind(2, self.address)
    stmt:bind(3, self.port)
    stmt:step()
    stmt:reset()
    return self
  end,
  
  update_keepalive_ping = function(self, keepalive_received_time)
    local stmt = peer_update_keepalive_ping
    if keepalive_received_time then
      self.last_keepalive_received = keepalive_received_time
    else
      keepalive_received_time = self.last_keepalive_received
    end
    stmt:bind(1, keepalive_received_time)
    stmt:bind(2, keepalive_received_time - self.last_keepalive_sent)
    stmt:bind(3, self.address)
    stmt:bind(4, self.port)
    stmt:step()
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
    
    
    peer_find = assert(db:prepare("SELECT * FROM peers WHERE address=? AND port=? LIMIT 1"), db:errmsg())
    peer_store = assert(db:prepare("INSERT OR IGNORE INTO peers (address, port, last_received, last_sent, last_keepalive_sent, last_keepalive_received) "..
                                                         "VALUES(      ?,    ?,             ?,         ?,                   ?,                       ?)"), db:errmsg())
    peer_get_fastest_ping = assert(db:prepare("SELECT *, (last_keepalive_received - last_keepalive_sent) AS ping FROM peers WHERE last_keepalive_received NOT NULL AND ping >= 0 ORDER BY ping ASC LIMIT ?"), db:errmsg())
    
    peer_get8_except = assert(db:prepare("SELECT * FROM peers WHERE last_keepalive_received > datetime('now') - ? AND address !=? AND port != ? ORDER BY RANDOM() LIMIT 8"), db:errmsg())
    peer_get8 = assert(db:prepare("SELECT * FROM peers WHERE last_keepalive_received > datetime('now') - ? ORDER BY RANDOM() LIMIT 8"), db:errmsg())
    peer_get_active_to_keepalive = assert(db:prepare("SELECT * FROM peers WHERE last_keepalive_received > ? AND last_keepalive_received < ? ORDER BY RANDOM()"), db:errmsg())
    peer_update_keepalive_ping = assert(db:prepare("UPDATE peers SET last_keepalive_received = ?, ping = ? WHERE address = ? AND port = ?"), db:errmsg())
    peer_get_for_bootstrap = assert(db:prepare("SELECT * FROM peers WHERE tcp_in_use != 1 ORDER BY bootstrap_score DESC, ping ASC"), db:errmsg())
    
    for _, v in pairs{"last_received", "last_sent", "last_keepalive_sent", "last_keepalive_received", "bootstrap_score", "tcp_in_use"} do
      peer_update_num[v] = assert(db:prepare("UPDATE peers SET " .. v.." = ? WHERE address = ? AND port = ?"), db:errmsg())
    end
    
    setmetatable(Peer, PeerDB_meta)
  end,
  shutdown = function()
    --save previously known peers
    assert(db:exec("INSERT OR REPLACE INTO stored_peers SELECT * FROM peers") == sqlite3.OK, db:errmsg())
    local numpeers
    for row in db:urows("SELECT count(*) FROM peers;") do
      numpeers = row
    end
    log:debug("stored %i seen peers", numpeers)
    peer_find:finalize()
    peer_store:finalize()
    peer_get_fastest_ping:finalize()
    peer_get8_except:finalize()
    peer_get8:finalize()
    peer_get_active_to_keepalive:finalize()
    peer_update_keepalive_ping:finalize()
    peer_get_for_bootstrap:finalize()
    for _, v in pairs(peer_update_num) do
      v:finalize()
    end
  end
}

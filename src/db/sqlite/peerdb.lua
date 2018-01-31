local sqlite3 = require "lsqlite3"
local Peer

local schema = [[
  CREATE TABLE IF NOT EXISTS peers (
    address                  TEXT,
    port                     INTEGER,
    last_received            REAL,
    last_sent                REAL,
    last_keepalive_sent      REAL,
    last_keepalive_received  REAL,
    PRIMARY KEY(address, port)
  ) WITHOUT ROWID;
  CREATE INDEX IF NOT EXISTS peer_last_received_idx           ON peers (last_received);
  CREATE INDEX IF NOT EXISTS peer_last_sent_idx               ON peers (last_sent);
  CREATE INDEX IF NOT EXISTS peer_last_keepalive_sent_idx     ON peers (last_keepalive_sent);
  CREATE INDEX IF NOT EXISTS peer_last_keepalive_received_idx ON peers (last_keepalive_received);
]]

local db

local peer_find, peer_store, peer_get8, peer_get8_except, peer_get_active_to_keepalive, peer_get_fastest_ping, peer_update_timestamp

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
  
  update_timestamp_field = function(self, timestamp_field)
    peer_update_timestamp:bind(1, timestamp_field)
    peer_update_timestamp:bind(2, self[timestamp_field])
    peer_update_timestamp:bind(3, self.address)
    peer_update_timestamp:bind(4, self.port)
    peer_update_timestamp:step()
    peer_update_timestamp:reset()
    return self
  end
}}

return {
  initialize = function(db_ref)
    Peer = require "prailude.peer"
    db = db_ref
    if db:exec(schema) ~= sqlite3.OK then
      error(db:errmsg())
    end
    
    peer_find = assert(db:prepare("SELECT * FROM peers WHERE address=? AND port=? LIMIT 1"), db:errmsg())
    peer_store = assert(db:prepare("INSERT OR IGNORE INTO peers (address, port, last_received, last_sent, last_keepalive_sent, last_keepalive_received) "..
                                                         "VALUES(      ?,    ?,             ?,         ?,                   ?,                       ?)"), db:errmsg())
    peer_get_fastest_ping = assert(db:prepare("SELECT *, (last_keepalive_received - last_keepalive_sent) AS ping FROM peers WHERE last_keepalive_received NOT NULL AND ping >= 0 ORDER BY ping ASC LIMIT ?"), db:errmsg())
    
    peer_get8_except = assert(db:prepare("SELECT * FROM peers WHERE last_keepalive_received > datetime('now') - ? AND address !=? AND port != ? ORDER BY RANDOM() LIMIT 8"), db:errmsg())
    peer_get8 = assert(db:prepare("SELECT * FROM peers WHERE last_keepalive_received > datetime('now') - ? ORDER BY RANDOM() LIMIT 8"), db:errmsg())
    peer_get_active_to_keepalive = assert(db:prepare("SELECT * FROM peers WHERE last_keepalive_received > ? AND last_keepalive_received < ? ORDER BY RANDOM()"), db:errmsg())
    peer_update_timestamp = assert(db:prepare("UPDATE peers SET ? = ? WHERE address=? AND port=?"), db:errmsg())
    
    setmetatable(Peer, PeerDB_meta)
  end,
  shutdown = function()
    peer_find:finalize()
    peer_store:finalize()
    peer_get_fastest_ping:finalize()
    peer_get8_except:finalize()
    peer_get8:finalize()
    peer_get_active_to_keepalive:finalize()
    peer_update_timestamp:finalize()
  end
}

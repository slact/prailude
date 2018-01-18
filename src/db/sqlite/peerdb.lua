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

local PeerDB_meta = {__index = {
  find = function(peer_addr, peer_port)
    local peer
    local select = ("SELECT * FROM peers WHERE address=\"%s\" AND port=%d LIMIT 1"):format(peer_addr, peer_port)
    for row in db:nrows(select) do
      peer = Peer.new(row)
    end
    return peer
  end,
  store = function(peer)
    db:exec(("INSERT OR IGNORE INTO peers (address, port) VALUES( \"%s\", %i)"):format(peer.address, peer.port))
  end,
  
  get_fastest_ping = function(n)
    n = n or 1
    local select = "SELECT *, (last_keepalive_received - last_keepalive_sent) AS ping FROM peers WHERE last_keepalive_received NOT NULL AND ping >= 0 ORDER BY ping ASC limit " .. n
    local peers = {}
    for row in db:nrows(select) do
      table.insert(peers, Peer.new(row))
    end
    return peers
  end,
  
  get8 = function(except_peer)
    local select
    if except_peer then
      select = ("SELECT * FROM peers WHERE last_keepalive_received > datetime('now') - %d AND address !=\"%s\" AND port != %d ORDER BY RANDOM() LIMIT 8"):format(Peer.keepalive_interval, except_peer.address, except_peer.port)
    else
      select = ("SELECT * FROM peers WHERE last_keepalive_received > datetime('now') - %d ORDER BY RANDOM() LIMIT 8"):format(Peer.keepalive_interval)
    end
    local peers = {}
    for row in db:nrows(select) do
      table.insert(peers, Peer.new(row))
    end
    return peers
  end,
  
  get_active_needing_keepalive = function()
    local now = os.time()
    local select = ("SELECT * FROM peers WHERE last_keepalive_received > %i AND last_keepalive_received < %i ORDER BY RANDOM()"):format(now - Peer.inactivity_timeout, now - Peer.keepalive_interval)
    local peers = {}
    for row in db:nrows(select) do
      table.insert(peers, Peer.new(row))
    end
    return peers
  end,
  
  update_timestamp_field = function(peer, timestamp_field)
    local query = ("UPDATE peers SET %s=%f WHERE address=\"%s\" AND port=%d"):format(
      timestamp_field,
      peer[timestamp_field],
      peer.address,
      peer.port
    )
    db:exec(query)
  end
}}

return {
  initialize = function(db_ref)
    Peer = require "prailude.peer"
    db = db_ref
    if db:exec(schema) ~= sqlite3.OK then
      error(db:errmsg())
    end
    setmetatable(Peer, PeerDB_meta)
  end
}

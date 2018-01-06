local bus = require "prailude.bus"
local mm = require "mm"
local log = require "prailude.log"
local server = require "prailude.server"


local schema = [[
  CREATE TABLE IF NOT EXISTS peers (
    address         TEXT,
    port            INTEGER,
    last_received   INTEGER,
    last_sent       INTEGER,
    last_keepalive  INTEGER,
    PRIMARY KEY(address, port)
  ) WITHOUT ROWID;
  CREATE INDEX IF NOT EXISTS peer_last_received_idx  ON peers (last_received);
  CREATE INDEX IF NOT EXISTS peer_last_sent_idx      ON peers (last_sent);
  CREATE INDEX IF NOT EXISTS peer_last_keepalive_idx ON peers (last_keepalive);
]]

local known_peers = {}
local db

local Peer_instance = {
  send = function(self, message)
    return server.send(message, self)
  end,
  
  update_timestamp = function(self, what)
    local field = "last_"..what
    local current_val = self[field]
    local now = os.time() --yummy slowy syscall
    if not current_val or current_val < now - 60 then --every minute update
      local query = ("UPDATE peers SET %s=%d WHERE address=\"%s\" AND port=%d"):format(
        field,
        os.time(),
        self.address,
        self.port
      )
      db:exec(query)
      self[field] = now
    end
    return self
  end
}
local peer_meta = {
  __index=Peer_instance,
  __tostring=function(t)
    return t.id
  end
}

local function new_peer(peer_addr, peer_port)
  local peer
  if type(peer_addr) == "table" and peer_port == nil then
    peer = peer_addr
    assert(peer.address, "peer address is required")
    assert(peer.port, "peer port is required")
    peer.id = ("%s:%d"):format(peer.address, peer.port)
  else
    if not peer_port then
      local m1, m2 = peer_addr:match("(.*[^:]):(%d+)$")
      if not m1 then
        peer_port = 7075
      else
        peer_addr, peer_port = m1, m2
      end
    end
    peer = {
      address = peer_addr,
      port = peer_port,
      id = ("%s:%.0f"):format(peer_addr, peer_port)
    }
  end
  
  setmetatable(peer, peer_meta)
  return peer
end

local function ensure_ipv6_if_ipv4(peer_addr)
  local addr = { peer_addr:match("^(%d%d?%d?)%.(%d%d?%d?)%.(%d%d?%d?)%.(%d%d?%d?)") }
  if #addr == 0 then
    return peer_addr
  else
    return ("::ffff:%i.%i.%i.%i"):format(addr[1], addr[2], addr[3], addr[4])
  end
end

local Peer = {
  new = new_peer,
  
  --find existing peer or make a new one
  get = function(peer_addr, peer_port)
    local id, not_recently_seen
    if type(peer_addr) == "table" then
      peer_port = peer_addr.port
      peer_addr = peer_addr.address
      id = ("%s:%.0f"):format(peer_addr, peer_port)
    elseif peer_addr and not peer_port then --maybe we were passed the peer id (addr:port)
      id = peer_addr
      peer_addr, peer_port = id:match("^(.*[^:]):(%d+)$")
    else
      peer_addr = ensure_ipv6_if_ipv4(peer_addr)
      id = ("%s:%.0f"):format(peer_addr, peer_port)
    end
    local peer = rawget(known_peers, id)
    if not peer then
      local select = ("SELECT * FROM peers WHERE address=\"%s\" AND port=%d LIMIT 1"):format(peer_addr, peer_port)
      for row in db:nrows(select) do
        peer = new_peer(row)
        break
      end
    end
    if not peer then
      not_recently_seen = true
      peer = new_peer(peer_addr, peer_port)
      db:exec(("INSERT OR IGNORE INTO peers (address, port) VALUES( \"%s\", %i)"):format(peer_addr, peer_port))
    end
    rawset(known_peers, id, peer)
    return peer, not_recently_seen
  end,
  
  get8 = function(except_peer)
    local select = ("SELECT * FROM peers WHERE last_keepalive > datetime('now') - 120 AND address !=\"%s\" AND port != %d ORDER BY RANDOM() LIMIT 8"):format(except_peer.address, except_peer.port)
    local peers = {}
    for row in db:nrows(select) do
      table.insert(peers, new_peer(row))
    end
    return peers
  end,
  
  initialize = function(db_ref)
    db = db_ref
    db:exec(schema)
  end
}

return Peer

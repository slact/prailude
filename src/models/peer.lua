local bus = require "prailude.bus"
local mm = require "mm"
local log = require "prailude.log"
local server = require "prailude.server"
local gettime = require "prailude.util.lowlevel".gettime
local TCPSession = require "prailude.TCPsession"
local NilDB = require "prailude.db.nil" -- no database
local coroutine = require "prailude.util.coroutine"
local Peer

local known_peers = {}

local Peer_instance = {
  send = function(self, message)
    local ret, err = server.send(message, self)
    if ret then
      self:update_timestamp("keepalive_sent")
    end
    return ret, err
  end,
  
  tcp_session = function(self, session_name, session, heartbeat)
    local coro = coroutine.running()
    assert(coro, "tcp_session expects to be called in a coroutine")
    return TCPSession.get(self):run(session_name, session, heartbeat)
  end,
  
  update_timestamp = function(self, what)
    local field = "last_"..what
    local prev_val = rawget(self, field)
    rawset(self, field, gettime())
    Peer.update_timestamp_field(self, field, prev_val)
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

Peer = {
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
      peer = Peer.find(peer_addr, peer_port)
    end
    if not peer then
      not_recently_seen = true
      peer = new_peer(peer_addr, peer_port)
      Peer.store(peer)
    end
    rawset(known_peers, id, peer)
    return peer, not_recently_seen
  end,
  
  inactivity_timeout = 60*7,
  keepalive_interval = 60*2,
  
  ------------
  -- database stuff is in db/[db_type]/peerdb.lua
  ------------
}

return setmetatable(Peer, NilDB.peer)

local bus = require "prailude.bus"
local mm = require "mm"
local log = require "prailude.log"
local sqlite3 = require "lsqlite3"
local server = require "prailude.server"

local Peer_instance = {
  send = function(self, message)
    server.send(message, self)
  end
}
local peer_meta = {__index=Peer_instance}

local known_peers = {}

local function new_peer(peer_addr, peer_port)
  if not peer_port then
    local m1, m2 = peer_addr:match("(.*[^:]):(%d+)$")
    if not m1 then
      peer_port = 7075
    else
      peer_addr, peer_port = m1, m2
    end
  end
  
  local peer = {
    address = peer_addr,
    port = peer_port,
    id = ("%s:%.0f"):format(peer_addr, peer_port)
  }
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
  --find existing peer or make a new one
  get = function(peer_addr, peer_port)
    local id    
    if peer_addr and not peer_port then --maybe we were passed the peer id (addr:port)
      id = peer_addr
      peer_addr, peer_port = id:match("^(.*[^:]):(%d+)$")
    else
      peer_addr = ensure_ipv6_if_ipv4(peer_addr)
      id = ("%s:%.0f"):format(peer_addr, peer_port)
    end
    local peer = rawget(known_peers, id)
    if not peer then
      peer = new_peer(peer_addr, peer_port)
    end
    rawset(known_peers, id, peer)
    return peer
  end,
  
  forget = function(peer)
    known_peers[peer] = nil
    return peer
  end
}

return Peer

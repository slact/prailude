local bus = require "prailude.bus"

local Peer_instance = {
  send = function(self, message)
    local msg = assert(message:pack())
    bus.pub("message:send", message, self)
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

local Peer = {
  --find existing peer or make a new one
  get = function(peer_addr, peer_port)
    local id = ("%s:%.0f"):format(peer_addr, peer_port)
    local peer = rawget(known_peers, id)
    if not peer then
      peer = new_peer(peer_addr, peer_port)
    end
    know_peers[id] = peer
    return peer
  end
}

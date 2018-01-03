local uv =  require "luv"
local Message --require it later
local bus = require "prailude.bus"
local Rainet -- require it later
local Peer -- require it later
local util = require "prailude.util"
local logger = require "prailude.log"
local config = require "prailude.config"

local mm = require "mm"

--local log = require "prailude.log"
local Server = {}
function Server.initialize()
  Message = require "prailude.message"
  Rainet = require "prailude.rainet"
  Peer = require "prailude.peer"
  
  local port = config.node.peering_port or 7075
  
  local tcp_server = uv.new_tcp()
  assert(tcp_server:bind("::", port))
  assert(tcp_server:listen(128 --[[connection backlog size]], function(err, addr)
    assert(not err, err)
    local client = uv.new_tcp()
    assert(tcp:accept(client))
    client:read_start(function(err, chunk, addr)
      --print("TCP", err, chunk, etc)
      -- Crash hard on errors
      local data, leftovers_or_err = Message.unpack(chunk)
      if data then
        bus.pub("message:receive", data, addr, "tcp")
      else
        bus.pub("message:receive:fail", leftovers_or_err, addr, "tcp")
      end
    end)
    --do anything on TCP connection start?...
  end))
  
  Server.tcp = tcp_server
  
  local udp_server = uv.new_udp()
  assert(udp_server:bind("::", port))
  udp_server:recv_start(function(err, chunk, addr)
    if chunk == nil and not err then --EAGAIN or something
      return
    end
    
    --print(err, #chunk, addr)
    
    local peer = Peer.get(addr.ip, addr.port)
    
    local msg, leftovers_or_err = Message.unpack(chunk)
    if msg then
      logger:debug("server: got message %s from peer %s", msg.type, tostring(peer))
      bus.pub( "message:receive", msg, peer, "udp")
      bus.pub(("message:receive:%s"):format(msg.type), msg, peer, "udp")
    else
      logger:warn("server: bad message from peer %s", tostring(peer))
      bus.pub_fail("message:receive", leftovers_or_err, peer, "udp")
    end
  end)
  
  Server.udp = udp_server
  return Server
end

function Server.send(msg, peer)
  if msg.protocol =="tcp" then
    error("tcp messaging not yet implemented")
  else --udp by default
    local msg_packed = assert(msg:pack())
    assert(peer.address, "peer address missing")
    assert(peer.port, "peer port missing")
    uv.udp_send(Server.udp, msg_packed, peer.address, peer.port)
    logger:debug("server: sent message %s to peer %s", msg.type, tostring(peer))
  end
  return Server
end

return Server

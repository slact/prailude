local uv =  require "luv"
local Message --require it later
local bus = require "prailude.bus"
local Peer -- require it later
local util = require "prailude.util"

local mm = require "mm"

--local log = require "prailude.log"
local Server = {}
function Server.initialize(port)
  Message = require "prailude.message"
  Peer = require "prailude.peer"
  
  port = port or 7075
  
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
    
    mm(peer)
    local msg, leftovers_or_err = Message.unpack(chunk)
    if msg then
      bus.pub( "message:receive", msg, peer, "udp")
      bus.pub(("message:receive:%s"):format(msg.type), msg, peer, "udp")
    else
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
    uv.udp_send(udp_server, msg_packed, peer.address, peer.port)
    logger:debug("server: sent message %s to peer %s", msg.type, tostring(peer))
  end
  return Server
end

return Server

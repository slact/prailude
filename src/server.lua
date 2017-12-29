local uv =  require "luv"
local Message = require "prailude.message"
local bus = require "prailude.bus"
local peers = require "prailude.peer"

local mm = require "mm"

--local log = require "prailude.log"
local server = {}
function server.initialize(port)
  
  port = 7075
  
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
  
  server.tcp = tcp_server
  
  local udp_server = uv.new_udp()
  assert(udp_server:bind("::", port))
  udp_server:recv_start(function(err, chunk, addr)
    if chunk == nil and not err then --EAGAIN or something
      return
    end
    local peer = Peers.get(addr.ip, addr.port)
    local msg, leftovers_or_err = Message.unpack(chunk)
    if msg then
      bus.pub("message:receive", msg, peer, "udp")
    else
      bus.pub_fail("message:receive", leftovers_or_err, peer, "udp")
    end
  end)
  
  bus.sub("message:send", function(ok, msg, peer, protocol)
    mm(msg)
    mm(peer)
    mm(protocol)
    if protocol =="tcp" then
      error("tcp messaging not yet implemented")
    else --udp by default
      udp_server:send(msg:pack(), peer.address, peer.port)
    end
  end)
  
  return self
end

return server

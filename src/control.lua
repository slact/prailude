local Message = require "prailude.message"
local Server = require "prailude.server"

local Peer = require "prailude.peer"
local uv = require "luv"
local bus = require "prailude.bus"
local Control = {}
local mm = require "mm"
local log = require "prailude.log" "control"


local bootstrapping_timer = nil
function Control.bootstrap(bootstrap_peers)
  assert(not bootstrapping_timer, "boostrap already started")
  
  --queue up bootstrapping as soon as the event loop starts
  bootstrapping_timer = uv.new_timer()
  local coro = coroutine.wrap(function()
    local random_peer = bootstrap_peers[math.random(1,#bootstrap_peers)]
    --resolve peer name
    local addr = uv.getaddrinfo(random_peer.name, nil, {socktype="dgram", protocol="packet"})
    mm(addr)
    random_peer.address = addr[math.random(1,#addr)].addr
    
    local first_peer = Peer.get(random_peer.address, random_peer.port)
    
    bus.pub("message:send", Message.new("keepalive"), first_peer)
    --TODO: wait for response from peer
    
  end)
  bootstrapping_timer:start(0, 0, function()

  end)

end

function Control.run(port, bootstrap_peers)
  Server.initialize(port)
  Rainet.initialize(bootstrap_peers)
  --do some other stuff maybe
  uv.run()
end

return Control

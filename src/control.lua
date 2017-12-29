local Message = require "prailude.message"
local Server = require "prailude.server"
local uv = require "luv"
local bus = require "prailude.bus"
local Control = {}

local default_port = 7075

local function parse_bootstrap_peer(peer_string)
  local peer_name, peer_port
  if type(peer_string) == "string" then
    peer_name, peer_port = peer_string:match("(.*):(%d+)$")
    if not peer_name then
      peer_name, peer_port = peer_string, 7075
    else
      peer_port = tonumber(peer_port)
    end
  else
    peer_name, peer_port = peer_string[1] or peer_string.name or peer_string.host, peer_string[2] or peer_string.port
  end
  return peer_name, peer_port
end

local bootstrapping_timer = nil
function Control.bootstrap(bootstrap_peers)
  assert(not bootstrapping_timer, "boostrap already started")
  if type(bootstrap_peers) == "string" then
    bootstrap_peers = {bootstrap_peers}
  end
  if #bootstrap_peers == 0 then
    error("no peers to bootstrap from")
  end
  local bootstrap_keepalive = Message.new("keepalive")
  for k, peer in ipairs(bootstrap_peers) do
    local peer_name, peer_port = parse_bootstrap_peer(peer)    
    bootstrap_peers[k]={name = peer_name, port = peer_port}
  end
  bootstrapping_timer = uv.new_timer()
  bootstrapping_timer:start(10, 0, function()
    local random_peer = bootstrap_peers[math.random(1,#bootstrap_peers)]
    local addr = uv.getaddrinfo(random_peer.name, nil, {family="inet6", socktype="dgram", protocol="packet"})
    random_peer.address = addr[math.random(1,#addr)].addr
    
    bus.pub("message:send", m, random_peer)

  end)

end

function Control.run(port, bootstrap_peers)
  Control.bootstrap(bootstrap_peers)
  Server.initialize(port)
  --do some other stuff maybe
  uv.run()
end

return Control

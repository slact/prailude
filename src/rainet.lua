local Peer = require "prailude.peer"
local logger = require "prailude.log"
local bus = require "prailude.bus"
local config = require "prailude.config"
local Message = require "prailude.message"
local Block = require "prailude.block"
local Timer = require "prailude.util.timer"

local uv = require "luv"

local mm = require "mm"

local db

local Rainet = {}

local function keepalive()
  --bootstrap
  
  local function parse_bootstrap_peer(peer_string)
    local peer_name, peer_port
    if type(peer_string) == "string" then
      peer_name, peer_port = peer_string:match("^(.*):(%d+)$")
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
  
  bus.sub("run", function()
    local keepalive_msg = Message.new("keepalive", {peers = {}})
    for _, preconfd_peer in pairs(config.node.preconfigured_peers) do
      local peer_name, peer_port = parse_bootstrap_peer(preconfd_peer)
      local addrinfo = uv.getaddrinfo(peer_name, nil, {socktype="dgram", protocol="packet"})
      for _, addrinfo_entry in ipairs(addrinfo) do
        local peer = Peer.get(addrinfo_entry.addr, peer_port)
        peer:send(keepalive_msg)
      end
    end
  end)
  
  --keepalive parsing and response
  bus.sub("message:receive:keepalive", function(ok, msg, peer)
    if not ok then
      return logger:warning("rainet: message:receive:keepalive failed from peer %s: %s", peer, msg)
    end
    peer:update_timestamp("keepalive_received")
    local inpeer
    local now = os.time()
    local keepalive_cutoff = now - Peer.keepalive_interval
    if (peer.last_keepalive_sent or 0) < keepalive_cutoff then
      peer:send(Message.new("keepalive", {peers = Peer.get8(peer)}))
    end
    for _, peer_data in ipairs(msg.peers) do
      inpeer = Peer.get(peer_data)
      if (inpeer.last_keepalive_sent or 0) < keepalive_cutoff then
        inpeer:send(Message.new("keepalive", {peers = Peer.get8(inpeer)}))
      end
    end
  end)
  
  --periodic keepalive checks
  Timer.interval(Peer.keepalive_interval * 1000, function()
    local ping_these_peers = Peer.get_active_needing_keepalive()
    for _, peer in pairs(ping_these_peers) do
      peer:send(Message.new("keepalive", {peers = Peer.get8(peer)}))
    end
  end)
end


local function handle_blocks()
  local function check_block(block, peer)
    local ok, err = block:verify()
    if not ok then
      logger:debug("server: got block that failed verification (%s) from %s", err, tostring(peer))
    else
      return true
    end
  end
  bus.sub("message:receive:publish", function(ok, msg, peer)
    if not ok then
      return logger:warning("rainet: message:receive:publish from %s failed: %s", peer, msg)
    end
    local block = Block.new(msg.block_type, msg.block)
    check_block(block, peer)
  end)
  bus.sub("message:receive:confirm_req", function(ok, msg, peer)
    if not ok then
      return logger:warning("rainet: message:receive:confirm_req from %s failed: %s", peer, msg)
    end
    local block = Block.new(msg.block_type, msg.block)
    check_block(block, peer)
  end)
  bus.sub("message:receive:confirm_ack", function(ok, msg, peer)
    if not ok then
      return logger:warning("rainet: message:receive:confirm_ack from %s failed: %s", peer, msg)
    end
    local block = Block.new(msg.block_type, msg.block)
    check_block(block, peer)
  end)
end

function Rainet.initialize(db_ref)
  --local initial_peers = {}
  
  db = db_ref
  Peer.initialize(db)
  
  keepalive()
  handle_blocks()
end

function Rainet.bootstrap()
  local fastpeers = Peer.get_fastest_ping(100)
  local frontier_req = Message.new("frontier_req")
  
  local function frontier_fetcher()
    for _, peer in ipairs(fastpeers) do
      local frontiers, ok, err
      logger:debug("bootstrap: starting frontier pull from %s", peer)
      --do we have a tcp connection to the peer?
      ok, err = peer:open_tcp()
      if not ok then
        logger:debug("bootstrap: can't init tcp connection to %s (%s)", peer, err)
        break
      end
      
      logger:debug("bootstrap: opened tcp connection to %s", peer)
      
      ok, err = peer:send_tcp(frontier_req)
      if not ok then
        peer:close_tcp()
        logger:debug("bootstrap: failed to send frontier req to %s (%s)", tostring(peer), err)
        break
      end
      
      frontiers, err = peer:read_frontiers() -- TODO: set min frontier-reading speed
      if not frontiers then
        peer:close_tcp()
        logger:debug("bootstrap: failed to read frontiers list from peer %s (%s)", tostring(peer), err)
        break
      end
      
      
    end
  end
  coroutine.wrap(frontier_fetcher)()
end

return Rainet

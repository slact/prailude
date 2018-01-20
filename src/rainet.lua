local Peer = require "prailude.peer"
local log = require "prailude.log"
local bus = require "prailude.bus"
local config = require "prailude.config"
local Message = require "prailude.message"
local Block = require "prailude.block"
local Timer = require "prailude.util.timer"
local DB = require "prailude.db"
local Frontier = require "prailude.frontier"

local uv = require "luv"
local mm = require "mm"

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
      return log:warning("rainet: message:receive:keepalive failed from peer %s: %s", peer, msg)
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
      log:debug("server: got block that failed verification (%s) from %s", err, tostring(peer))
    else
      return true
    end
  end
  bus.sub("message:receive:publish", function(ok, msg, peer)
    if not ok then
      return log:warning("rainet: message:receive:publish from %s failed: %s", peer, msg)
    end
    local block = Block.new(msg.block_type, msg.block)
    check_block(block, peer)
  end)
  bus.sub("message:receive:confirm_req", function(ok, msg, peer)
    if not ok then
      return log:warning("rainet: message:receive:confirm_req from %s failed: %s", peer, msg)
    end
    local block = Block.new(msg.block_type, msg.block)
    check_block(block, peer)
  end)
  bus.sub("message:receive:confirm_ack", function(ok, msg, peer)
    if not ok then
      return log:warning("rainet: message:receive:confirm_ack from %s failed: %s", peer, msg)
    end
    local block = Block.new(msg.block_type, msg.block)
    check_block(block, peer)
  end)
end

function Rainet.initialize()
  --local initial_peers = {}
  keepalive()
  handle_blocks()
end

function Rainet.bootstrap()
  local min_frontiers_per_sec = 100
  local fastpeers = Peer.get_fastest_ping(100)
  
  local function frontier_fetcher()
    for _, peer in ipairs(fastpeers) do
      local frontiers, err
      log:debug("bootstrap: starting frontier pull from %s", peer)
      --do we have a tcp connection to the peer?
      
      local largest_frontier_pull_size = tonumber(DB.kv.get("largest_frontier_pull") or 0)
      local prev_frontiers_count, intervals_checked, times_below_min_rate = 0, 0, 0
      local frontier_pull_size_estimated = false
      frontiers, err = Frontier.fetch(peer, function(frontiers_so_far, progress)
        
        --interval-checker for frontier fetch progress
        local frontiers_per_sec = #frontiers_so_far - prev_frontiers_count
        prev_frontiers_count = #frontiers_so_far
        if frontiers_per_sec < min_frontiers_per_sec then
          --too slow
          times_below_min_rate = times_below_min_rate + 1
          if times_below_min_rate > 2 then
            return false, ("too slow (%i frontiers/sec)"):format(frontiers_per_sec)
          end
        elseif progress > 0.2 and not frontier_pull_size_estimated then
          --is this pull going to be too small (i.e. from an unsynced node)?
          frontier_pull_size_estimated = true
          if #frontiers_so_far * 1/progress < 0.8 * largest_frontier_pull_size then
            --too small
            return false, ("node seems desynchronized (small estimated frontier %.0f , expected around %.0f)"):format(#frontiers_so_far * 1/progress, largest_frontier_pull_size)
          end
        end
        log:debug("bootstrap: got %5d frontiers (%7d total) [%4.3f%%] from %s", frontiers_per_sec, #frontiers_so_far, (progress or 0) * 100, peer)
        intervals_checked = intervals_checked + 1
        return true
      end)
      
      if frontiers and #frontiers < 0.8 * largest_frontier_pull_size then
        err = ("node seems desynchronized (small frontier %.0f , expected around %.0f)"):format(#frontiers, largest_frontier_pull_size)
        frontiers = nil
      end
      
      if not frontiers then
        log:debug("bootstrap: failed to get frontiers from %s : %s", peer, err)
      else
        log:debug("bootstrap: got %i frontiers from %s", #frontiers, peer)
        if #frontiers > largest_frontier_pull_size then
          DB.kv.set("largest_frontier_pull", #frontiers)
        end
        --we got a good one, now process that shit
        print("we got one!!")
        --return Account.bulk_pull(frontiers)
      end
    end
  end
  
  coroutine.wrap(frontier_fetcher)()
end

return Rainet

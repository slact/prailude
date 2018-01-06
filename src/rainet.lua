local Peer = require "prailude.peer"
local log = require "prailude.log"
local sqlite3 = require "lsqlite3"
local bus = require "prailude.bus"
local config = require "prailude.config"
local Message = require "prailude.message"

local Timer = require "prailude.util.timer"

local uv = require "luv"

local mm = require "mm"




local db

local Rainet = {}

function Rainet.initialize()
  local initial_peers = {}
  
  Rainet.db = sqlite3.open("network.db")
  db = Rainet.db
  db:exec("PRAGMA synchronous = OFF") --don't really care of peer db is corrupted
  Peer.initialize(db)
  
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
    local keepalive = Message.new("keepalive", {peers = {}})
    
    for _, preconfd_peer in pairs(config.node.preconfigured_peers) do
      local peer_name, peer_port = parse_bootstrap_peer(preconfd_peer)
      local addrinfo = uv.getaddrinfo(peer_name, nil, {socktype="dgram", protocol="packet"})
      for _, addrinfo_entry in ipairs(addrinfo) do
        local peer = Peer.get(addrinfo_entry.addr, peer_port)
        peer:send(keepalive)
      end
    end
  end)
  
  bus.sub("message:receive:keepalive", function(ok, msg, peer)
    peer:update_timestamp("keepalive")
    local inpeer
    local now = os.time()
    for _, peer_data in ipairs(msg.peers) do
      inpeer = Peer.get(peer_data)
      if (inpeer.last_keepalive or 0) < now - 120 then --2 minutes
        inpeer:send(Message.new("keepalive", {peers = Peer.get8(inpeer)}))
      end
    end
  end)
end

return Rainet

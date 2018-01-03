local Peer = require "prailude.peer"
local log = require "prailude.log"
local sqlite3 = require "lsqlite3"
local bus = require "prailude.bus"
local config = require "prailude.config"
local Message = require "prailude.message"

local uv = require "luv"

local mm = require "mm"


local db

local Rainet = {}

function Rainet.initialize()
  local initial_peers = {}
  
  Rainet.db = sqlite3.open("network.db")
  db = Rainet.db
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
    --parse it and stuff
    mm("YEAH UPDATE THAT KEEPALIVE TIMESTAMP")
    mm(msg)
    peer:update_timestamp("keepalive")
  end)
end

return Rainet

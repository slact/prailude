local Peer = require "prailude.peer"
local log = require "prailude.log"
local mm = require "mm"

local peerdb = setmetatable({}, {__mode="k"})
local peer_lru = setmetatable({}, {__move="v"})


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

local Rainet = {}

return Rainet

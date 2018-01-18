local bus = require "prailude.bus"
local mm = require "mm"
local log = require "prailude.log"
local coroutine = require "prailude.util.coroutine"
local Peer = require "prailude.peer"
local Message = require "prailude.message"
local Parser = require "prailude.message.parser"
local NilDB = require "prailude.db.nil" -- no database

local Frontier_meta = {
  __index={
    get_peer = function(self)
      if not self.source_peer then
        return nil
      end
      local peer_addr, peer_port = self.source_peer:match("^(.*[^:]):(%d+)$")
      return Peer.find(peer_addr, peer_port)
    end
  }
}

local Frontier = {}

function Frontier.new(data, peer)
  if peer then
    data.source_peer = peer.id
  end
  return setmetatable((data or {}), Frontier_meta)
end
  
function Frontier.fetch(peer, heartbeat_callback)
  assert(coroutine.running(), "Frontier.fetch must be called in a coroutine")
  local frontier_req = Message.new("frontier_req")
  
  local progress, frontiers_so_far = 0, {}
  local tcp_heartbeat_callback = function()
    local ret, err = heartbeat_callback(frontiers_so_far, progress)
    if ret == false or (not ret and err) then
      return false, err
    end
  end
  
  local ok, err
  
  ok, err = peer:open_tcp(tcp_heartbeat_callback)
  if not ok then
    return nil, err and "can't open tcp connection: " .. tostring(err) or "can't open tcp connection"
  end
  
  ok, err = peer:send_tcp(frontier_req)
  if not ok then
    peer:close_tcp()
    return nil, err and "failed to send frontier req: " .. tostring(err) or "failed to send frontier req"
  end
  
  local buf, chunk
  local function read_peer()
    chunk, err = peer:read_tcp()
    if chunk then
      assert(type(chunk)=="string", err or "chunk isn't a string : " .. type(chunk) .. " " .. tostring(chunk))
    end
    return chunk
  end
  
  local fresh_frontiers, done, current_progress
  
  while read_peer() do
    fresh_frontiers, buf, done, current_progress = Parser.unpack_frontiers(buf and buf..chunk or chunk)
    if not fresh_frontiers then -- there was an error
      err = buf
      peer:close_tcp()
      print("error unpacking frontiers")
      return nil, "error unpacking frontiers: " .. err
    elseif not done then
      if current_progress then
        progress = current_progress
      end
      
      --got some new frontiers
      for _, frontier in pairs(fresh_frontiers) do
        table.insert(frontiers_so_far, Frontier.new(frontier))
      end
      --print("got " .. #fresh_frontiers .. " frontiers")
    else
      log:debug("finished getting frontiers (%7d total) from %s", #frontiers_so_far, peer)
      -- no more frontiers here
      peer:close_tcp()
      return frontiers_so_far
    end
  end
  peer:close_tcp(err)
  return nil, err
end

------------
-- database stuff is in db/[db_type]/frontierdb.lua
------------

return setmetatable(Frontier, NilDB.frontier)

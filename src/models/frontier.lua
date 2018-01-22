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
  
function Frontier.fetch(peer, watchdog_callback)
  assert(coroutine.running(), "Frontier.fetch must be called in a coroutine")
  local frontier_req = Message.new("frontier_req")
  
  local progress, frontiers_so_far = 0, {}
  local function watchdog_wrapper()
    return watchdog_callback(frontiers_so_far, progress)
  end
  
  return peer:tcp_session("frontier pull", function(tcp)
    tcp:write(frontier_req:pack())
    local fresh_frontiers, leftovers_or_err, done, current_progress
    while tcp:read() do
      fresh_frontiers, leftovers_or_err, done, current_progress = Parser.unpack_frontiers(tcp.buf:flush())
      if not fresh_frontiers then -- there was an error
        return nil, "error unpacking frontiers: " .. tostring(leftovers_or_err)
      elseif not done then
        if current_progress then
          progress = current_progress
        end
        --got some new frontiers
        for _, frontier in pairs(fresh_frontiers) do
          table.insert(frontiers_so_far, Frontier.new(frontier))
        end
        if leftovers_or_err and #leftovers_or_err > 0 then
          tcp.buf:push(leftovers_or_err)
        end
      else
        log:debug("finished getting frontiers (%7d total) from %s", #frontiers_so_far, peer)
        -- no more frontiers here
        return frontiers_so_far
      end
      
    end
  end, watchdog_wrapper)
end

------------
-- database stuff is in db/[db_type]/frontierdb.lua
------------

return setmetatable(Frontier, NilDB.frontier)

local bus = require "prailude.bus"
local mm = require "mm"
local log = require "prailude.log"
local coroutine = require "prailude.util.coroutine"
local Peer = require "prailude.peer"
local Message = require "prailude.message"
local Account = require "prailude.account"
local Parser = require "prailude.util.parser"
local NilDB = require "prailude.db.nil" -- no database
local BatchSink = require "prailude.util".BatchSink

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

do
  local frontier_pulls = {}
  
  function Frontier.new_pull_id(peer)
    table.insert(frontier_pulls, peer)
    return #frontier_pulls
  end
  
  function Frontier.get_peer_from_pull_id(id)
    return frontier_pulls[id]
  end
  
  function Frontier.clear_pulls()
    frontier_pulls={}
  end
  
  function Frontier.delete_pull_id(id)
    frontier_pulls[id]=nil
  end
  
  function Frontier.delete_pull(id)
    Frontier.delete_pull_id(id)
    Frontier.delete_pull_data(id)
  end
end

function Frontier.new(data, peer)
  if peer then
    data.source_peer = tostring(peer)
  end
  return setmetatable((data or {}), Frontier_meta)
end
  
function Frontier.fetch(peer, watchdog_callback)
  assert(coroutine.running(), "Frontier.fetch must be called in a coroutine")
  local frontier_req = Message.new("frontier_req")
  
  local progress, frontiers_count_so_far = 0, 0
  local function watchdog_wrapper()
    return watchdog_callback(frontiers_count_so_far, progress)
  end
  
  local pull_id = Frontier.new_pull_id(peer)
  local sink = BatchSink {
    batch_size = 10000,
    consume = Frontier.batch_store
  }
  
  local res, err = peer:tcp_session("frontier pull", function(tcp)
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
        for _, frontier in ipairs(fresh_frontiers) do
          frontier.pull_id = pull_id
          frontier.stored_frontier = Account.get_frontier(frontier.account)
          sink:add(Frontier.new(frontier))
        end
        frontiers_count_so_far = frontiers_count_so_far + #fresh_frontiers
        if leftovers_or_err and #leftovers_or_err > 0 then
          tcp.buf:push(leftovers_or_err)
        end
      else
        sink:finish()
        log:debug("finished getting frontiers (%7d total) from %s", frontiers_count_so_far, peer)
        -- no more frontiers here
        return frontiers_count_so_far
      end
    end
  end, watchdog_wrapper)
  
  if not res then
    Frontier.delete_pull_id(pull_id)
    return nil, err
  else
    return pull_id, err
  end
end

------------
-- database stuff is in db/[db_type]/frontierdb.lua
------------

return setmetatable(Frontier, NilDB.frontier)

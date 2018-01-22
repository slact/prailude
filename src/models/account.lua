local bus = require "prailude.bus"
local mm = require "mm"
local log = require "prailude.log"
--local coroutine = require "prailude.util.coroutine"
local Peer = require "prailude.peer"
local Message = require "prailude.message"
--local Parser = require "prailude.message.parser"
local NilDB = require "prailude.db.nil" -- no database
local util = require "prailude.util"

local Account_meta = {
  __index = {
    
  }
}

local Account = {}

function Account.new(id, last_known_hash)
  local data = {
    id = id,
    last_known_hash = last_known_hash
  }
  return setmetatable(data, Account_meta)
end

function Account.to_bytes(str)
  return util.pack_account(str)
end

function Account.to_readable(raw)
  return util.unpack_account(raw)
end

--[[
function Account.bulk_pull(frontier, peer, heartbeat_callback)
  assert(coroutine.running(), "Account.bulk_pull must be called in a coroutine")
  local bulk_pull_message = Message.new("bulk_pull", {
    account = frontier.account,
    block_hash = frontier.last_block_hash
  })
  
  local blocks_so_far = 0, {}
  local ok, err
  
  ok, err = peer:tcp_stream("bulk pull", function(tcp)
    tcp:send(bulk_pull_message)
    
    while tcp:read() do
      local buf = tcp:buf()
    end
    
  end, heartbeat)
  
  local buf, chunk
  local function read_peer()
    chunk, err = peer:read_tcp()
    if chunk then
      assert(type(chunk)=="string", err or "chunk isn't a string : " .. type(chunk) .. " " .. tostring(chunk))
    end
    return chunk
  end
  
  local fresh_blocks, done
  
  while read_peer() do
    fresh_blocks, buf, done = Parser.unpack_frontiers(buf and buf..chunk or chunk)
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
]]

------------
-- database stuff is in db/[db_type]/frontierdb.lua
------------

return setmetatable(Account, NilDB.account)

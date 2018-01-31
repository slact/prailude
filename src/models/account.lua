local bus = require "prailude.bus"
local mm = require "mm"
local log = require "prailude.log"
local coroutine = require "prailude.util.coroutine"
local Peer = require "prailude.peer"
local Message = require "prailude.message"
local Parser = require "prailude.message.parser"
local NilDB = require "prailude.db.nil" -- no database
local util = require "prailude.util"
local Block

local Account = {}

local Account_meta = { __index = {
  update_frontier = function(self)
    Account.update(self, "frontier")
    return self
  end,
  update_balance = function(self)
    Account.update(self, "balance")
    return self
  end
}}

function Account.new(id, frontier)
  local data
  if type(id) == "table" then
    data = id
  else
    data = {
      id = id,
      frontier = frontier
    }
  end
  return setmetatable(data, Account_meta)
end

function Account.to_bytes(str)
  return util.pack_account(str)
end

function Account.to_readable(raw)
  return util.unpack_account(raw)
end

function Account.bulk_pull(frontier, peer, watchdog)
  Block = require "prailude.block" --late require
  
  assert(coroutine.running(), "Account.bulk_pull must be called in a coroutine")
  local bulk_pull_message = Message.new("bulk_pull", {
    account = frontier.account,
    block_hash = frontier.last_block_hash
  })
  
  local blocks_so_far = {}
  
  local function watchdog_wrapper()
    return watchdog(blocks_so_far)
  end
  
  return peer:tcp_session("bulk pull", function(tcp)
    tcp:write(bulk_pull_message:pack())
    local fresh_blocks, leftovers_or_err, done
    while tcp:read() do
      fresh_blocks, leftovers_or_err, done = Parser.unpack_bulk_pull(tcp.buf:flush())
      if not fresh_blocks then -- there was an error
        return nil, "error unpacking bulk blocks: " .. tostring(leftovers_or_err)
      elseif not done then
        --got some new blocks
        for _, block in pairs(fresh_blocks) do
          table.insert(blocks_so_far, Block.get(block))
        end
        if leftovers_or_err and #leftovers_or_err > 0 then
          tcp.buf:push(leftovers_or_err)
        end
      else
        log:debug("finished getting blocks for %s (%7d total) from %s", blocks_so_far, Account.to_readable(frontier.account), peer)
        -- no more frontiers here
        return blocks_so_far
      end
      
    end
  end, watchdog_wrapper)
end


------------
-- database stuff is in db/[db_type]/frontierdb.lua
------------

return setmetatable(Account, NilDB.account)

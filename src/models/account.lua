local bus = require "prailude.bus"
local mm = require "mm"
local log = require "prailude.log"
local coroutine = require "prailude.util.coroutine"
local Peer = require "prailude.peer"
local Message = require "prailude.message"
local Parser = require "prailude.util.parser"
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
},
__tostring = function(self)
  return Account.to_readable(self.id)
end
}

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

function Account.get(id) --always returns the account, regardless if it exists in db or not
  local acct = Account.find(id)
  if not acct then
    acct = Account.new(id)
  end
  return acct
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
  local wanted_frontier = frontier.hash
  local acct = Account.get(frontier.account)
  
  local bulk_pull_message = Message.new("bulk_pull", {
    account = acct.id,
    hash = acct.frontier
  })
  
  local blocks_so_far = {}
  
  local function watchdog_wrapper()
    return watchdog(blocks_so_far)
  end
  local frontier_hash_found = false
  
  return peer:tcp_session("bulk pull", function(tcp)
    tcp:write(bulk_pull_message:pack())
    local fresh_blocks, leftovers_or_err, done
    while tcp:read() do
    local buf = tcp.buf:flush()
      --print("buf", #buf)
      --print(util.bytes_to_hex_debug(buf))
      --print("")
      fresh_blocks, leftovers_or_err, done = Parser.unpack_bulk(buf)
      --print("fresh blocks", fresh_blocks and #fresh_blocks or "none", tostring(peer))
      if not fresh_blocks then -- there was an error
        --print("ERROR!", leftovers_or_err)
        return nil, "error unpacking bulk blocks: " .. tostring(leftovers_or_err)
      else
        --got some new blocks?
        if not done and #fresh_blocks == 0 and not leftovers_or_err then
          --nope, no blocks here, and no leftovers either
          --pull failed?
          --print("no blocks here, and no leftovers either?...")
          return nil, "account pull produced 0 blocks"
        else
          --mm(fresh_blocks)
          local block, err
          for _, rawblock in ipairs(fresh_blocks) do
            --mm(rawblock)
            block, err = Block.get(rawblock)
            if block then
              if not frontier_hash_found and block.hash == wanted_frontier then
                frontier_hash_found = true
              end
              table.insert(blocks_so_far, block)
            else
              log:warn("Found bad block from peer %s: %s (%s)", tostring(peer),  Block.new(rawblock):to_json(), err)
            end
          end
          
        end
      end
      if done then
        break
      elseif fresh_blocks and leftovers_or_err  and #leftovers_or_err > 0 then
        tcp.buf:push(leftovers_or_err)
      end
    end
    
    --log:debug("finished getting blocks for %s (%7d total) from %s", Account.to_readable(frontier.account), #blocks_so_far, peer)
    -- no more blocks here
    return blocks_so_far, frontier_hash_found
    
  end, watchdog_wrapper)
end


------------
-- database stuff is in db/[db_type]/frontierdb.lua
------------

return setmetatable(Account, NilDB.account)

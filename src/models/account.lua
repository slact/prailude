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

function Account.bulk_pull(frontier, peer, opt)
  if not Block then
    Block = require "prailude.block" --late require
  end
  opt = opt or {}
  assert(coroutine.running(), "Account.bulk_pull must be called in a coroutine")
  local wanted_frontier = frontier.frontier
  local acct = Account.get(frontier.account)
  
  local bulk_pull_message = Message.new("bulk_pull", {
    account = acct.id,
    frontier = acct.frontier
  })
  local blocks_so_far_count = 0
  
  local watchdog = opt.watchdog
  local function watchdog_wrapper()
    return watchdog(blocks_so_far_count)
  end
  local frontier_hash_found = false
  
  local consume, result
  do
    local pubkey = acct.id
    local function checkbatch(batch)
      for _, block in ipairs(batch) do
        if not block:verify_PoW() then
          log:warn("bootstrap: got bad-PoW block from %s for acct %s: %s", tostring(peer), tostring(acct), block:to_json())
          return nil, "bad PoW in batch_verify_signaturesaccount blocks"
        end
      end
      local all_valid, block_valid = Block.batch_verify_signatures(batch, pubkey)
      if not all_valid then
        for _, v in ipairs(block_valid) do
          if not v then
            log:warn("bootstrap: got bad-sig block from %s for acct %s: %s", tostring(peer), tostring(acct), v.block:to_json())
          end
        end
        return nil, "bad signature in account blocks"
      else
        return true
      end
    end
    if not opt.consume then
      local blocks_so_far = {}
      consume = function(batch)
        local ok, err = checkbatch(batch)
        if not ok then return nil, err end
        for _, b in ipairs(batch) do
          table.insert(blocks_so_far, b)
        end
      end
      result = function()
        return blocks_so_far, frontier_hash_found
      end
    else
      consume = function(batch)
        local ok, err = checkbatch(batch)
        if not ok then return nil, err end
        return opt.consume(batch, frontier, peer)
      end
      result = function()
        return blocks_so_far_count, frontier_hash_found
      end
    end
  end
  
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
          local ok, block, err
          for i, blockdata in ipairs(fresh_blocks) do
            --mm(blockdata)
            block, err = Block.new(blockdata)
            if block then
              if not frontier_hash_found and block.hash == wanted_frontier then
                frontier_hash_found = true
              end
              rawset(fresh_blocks, i, block)
            else
              log:warn("Found bad block from peer %s: %s (%s)", tostring(peer),  Block.new(blockdata):to_json(), err)
              return nil, "bad block: " .. err
            end
          end
          
          blocks_so_far_count = blocks_so_far_count + #fresh_blocks
          ok, err = consume(fresh_blocks)
          if not ok then
            return  nil, err or "consume function returned nil but no error"
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
    return result()
    
  end, watchdog_wrapper)
end


------------
-- database stuff is in db/[db_type]/frontierdb.lua
------------

return setmetatable(Account, NilDB.account)

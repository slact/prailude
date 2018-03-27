local Block = require "prailude.block"
local Util =  require "prailude.util"
local Account = require "prailude.account"
local log = require "prailude.log"
local DB = require "prailude.db"

local uniqueRunID; do
  -- since we're single-threaded and DB-exclusive, and walks are temporary data,
  -- this ID can just be held in memory
  local id = 0
  uniqueRunID = function()
    id = id + 1
    return id
  end
end

local BlockWalker = {}

local BlockWalker_instance = {
  add = function(self, block) --add block to visit
    return self.unvisited:add(block)
  end,
  
  next = function(self)
    local next_block = self.unvisited:next()
    --print("BlockWalker NEXT!!!!!", Util.bytes_to_hex(next_block.hash))
    if next_block then
      return self.visit(next_block)
    end
  end,
  
  each = function(self)
    local unvisited, visit = self.unvisited, self.visit
    return function()
      local next_block = unvisited:next()
      if next_block then
        return visit(next_block)
      end
    end
  end,
  
  walk = function(self)
    for _ in self:each() do
      --just loop
    end
    return self
  end,

  stop = function(self)
    --meh
    return self
  end,
  
  finish = function(self)
    self.unvisited:stop()
    self.stop()
    BlockWalker.delete_walk(self.id)
    return self.id
  end
}

--[[
local ttt = 0
local function trace ()
  local ttt2 = os.clock()
  if ttt2 - ttt > 1 then
    ttt = ttt2
    print(debug.traceback())
  end
end

debug.sethook(trace, "l")
]]

local BlockWalker_meta = {__index = BlockWalker_instance}

function BlockWalker.new(data)
  if data.direction ~= "frontier" then
    error("invalid walk direction " .. tostring(data.direction))
  end
  
  local self = {
    direction = data.direction,
  }
  
  local interrupt = data.interrupt or function() end
  
  self.id = uniqueRunID()
  local unvisited = Util.PageQueue{
    id=self.id,
    pagesize = 5000,
    store_item = function(item, state)
      local itemtype = type(item)
      if state == "active" then
        if itemtype == "string" then -- we have an account id
          return assert(Account.find(item))
        else
          assert(Account.is_instance(item))
          return item
        end
      elseif state == "idle" then
        if itemtype == "string" then --account.id
          return item
        else
          assert(Account.is_instance(item))
          return item.id
        end
      else
        error("weird state: " .. tostring(state))
      end
    end,
    store_page = BlockWalker.store_page,
    load_page = BlockWalker.restore_page,
    stored_page_size = assert(BlockWalker.get_page_size),
    item_tostring = function(item)
      local acct =  type(item) == "string" and Account.find(item) or item
      return ("%s %s %s %s"):format(acct:debug())
    end
  }
  self.unvisited = unvisited
  local stats = setmetatable({}, {__index = function() return 0 end})
  self.stats = stats
  
  do
    local start = data.start
    if start == "genesis" then
      local genesis_block = Block.find(Block.genesis.hash)
      assert(genesis_block, "genesis block not found")
      local acct, is_new = Account.get(genesis_block.account)
      acct.behind = true
      if is_new then
        acct:store()
      else
        acct:update("behind")
      end
      start = acct
    end
    if getmetatable(start) == nil then  --just a regular array of starting blocks
      local n = 0
      for _, obj in ipairs(start) do
        n = n+1
        assert(Account.is_instance(obj), "at least one start position is not a block")
        
        unvisited:add(obj)
      end
      assert(n>=1, "must have at least 1 starting block")
    else  -- probably a single start block?
      assert(Account.is_instance(start), "at least one start position is not a block")
      unvisited:add(start)
    end
  end
  
  local update_acct = function(acct, block)
    assert(acct.id == block.account)
    assert(acct.frontier == block.previous)
    local balance_change = tonumber(tostring(block:get_balance())) - tonumber(tostring(block:get_prev_balance()))
    
    if block.representative then
      --rep was created or changed
      if acct.representative and acct.representative ~= block.representative then --rep was changed
        local prev_rep = Account.find(acct.representative)
        if prev_rep then
          prev_rep.delegated_balance = tonumber(prev_rep.delegated_balance or 0) + balance_change
          prev_rep:save_later("delegated_balance")
          self.sink:add(prev_rep)
        end
      end
      acct.representative = block.representative
      acct:save_later("representative")
    end
    
    if balance_change > 0 then
      if not acct.representative then
        error("acct representative is missing?... acct: ".. acct:debug())
      end
      local rep, is_new = Account.get(acct.representative)
      if is_new then
        rep:create_later()
      end
      rep.delegated_balance = tonumber(rep.delegated_balance or 0) + balance_change
      rep:save_later("delegated_balance")
      self.sink:add(rep)
    end
    
    acct.behind = true
    acct.frontier = block.hash
    acct:save_later("frontier")
    acct:save_later("behind")
    self.sink:add(acct)
  end

  
  local function find_next_block_for_acct(acct)
    assert(Account.is_instance(acct))
    if acct.frontier then
      return Block.find_block_by("previous", acct.frontier)
    else
      return Block.find_open_for_account(acct.id)
    end
  end
  
  local function fast_forward(acct, n) --also verify up to n blocks in the same account with no unverified dependencies
    --print("ffwd", n, acct:debug())
    
    if n == 0 or not n then
      return false, "retry_right_away"
    end
    local block = find_next_block_for_acct(acct)
    if not block then --we're up to date now
      acct.behind = false
      acct:save_later("behind")
      self.sink:add(acct)
      return true
    end
    
    assert(not block:is_valid("ledger"))
    local ok, err, err_details = block:verify_ledger()
    
    if ok then
      --print("VERIFIED", block:debug())
      self.sink:add(block)
      stats.verified = stats.verified + 1
      update_acct(acct, block)
      if block.type == "send" then
        local dst = block:get_destination()
        if dst and not dst:is_valid("ledger") then
          local dst_acct, is_new = Account.get(dst.account)
          if is_new then
            dst_acct:create_later()
            self.sink:add(dst_acct)
          end
          if not dst_acct.behind then
            dst_acct.behind = true
          end
          unvisited:add(dst_acct)
        end
      end
      
      local child = block:get_next()
      if child then
        --print("with child")
        return fast_forward(acct, n-1)
      else
        return true, nil -- we're done here
      end
    elseif err == "retry" then
      --print("RETRY")
      stats.retry = stats.retry + 1
      --unvisited:prepend(block)
      if block.source then
        local source = block:get_source()
        if source and not source:is_valid("ledger") then
          local src_acct, is_new = Account.get(source.account)
          if is_new then
            src_acct:create_later()
            self.sink:add(src_acct)
          end
          unvisited:prepend(src_acct)
        end
      end
      if block.previous then
        local prev = block:get_previous()
        if prev and not prev:is_valid("ledger") then
          print("block:", block:debug())
          print("prev:", prev:debug())
          error("should not have happened")
        elseif not prev then
          error("y u no previous?")
        end
      end
      return false, "retry"
    elseif err == "gap" then
      print("GAP", err_details or "", block and block:debug())
      local gap_hash
      if err_details == "source" then
        gap_hash = block.source
      elseif err_details == "previous" then
        gap_hash = block.previous
      else
        error("unexpected block gap", tostring(err_details), block:debug())
      end
      local Nanonet = require "prailude.nanonet"
      local gap_block, gap_block_err = coroutine.wrap(function()
        local blocks, pull_err = Nanonet.bulk_block_pull {
          min_hash = gap_hash,
          max_mash = gap_hash,
          mode = "list",
          max_count = 1
        }
        if blocks then
          return blocks[1]
        else
          return nil, pull_err
        end
      end)()
      print(gap_block and gap_block:debug(), gap_block_err)
      
      
      
      
    else
      print("fail:", err, err_details or "", block and block:debug())
      return false, err, block
    end
  end
  
  
  self.visit = function(acct)
    interrupt()
    --print("VISIT", block:debug())
    local ok, err = fast_forward(acct, 400)
    if not ok then
      if err == "retry" then --retry it again later maybe?
        unvisited:add(acct)
      elseif err == "retry_right_away" then
        unvisited:prepend(acct)
      else
        --log:error(("validation failed on block %s: %s"):format(block and block:debug() or "unknown-block", err or "unknown-error"))
        stats.failed = stats.failed + 1
      end
    end
    return true
  end
  
  --local batchnum = 0
  self.sink = Util.BatchSink{
    batch_size = 5000,
    consume = function(batch)
      DB.transaction(function()
        --batchnum = batchnum + 1
        --print("SAVE BATCH", batchnum, "#batch", #batch)
        for _, val in ipairs(batch) do
          if Account.is_instance(val) then
            val.in_sink = nil
            val:save()
          else
            val:update_ledger_validation()
          end
        end
      end)
      return true
    end,
    add_filter = function(val)
      if Account.is_instance(val) then
        --accounts only need to be added once per batch
        if val.in_sink then
          return nil
        else
          val.in_sink = true
        end
      else
        assert(Block.is_instance(val))
        val:store_later()
      end
      return val
    end
  }
  
  return setmetatable(self, BlockWalker_meta)
end

return BlockWalker

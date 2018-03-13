local Block = require "prailude.block"
local Util =  require "prailude.util"
local Account = require "prailude.account"

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

local BlockWalker_meta = {__index = BlockWalker_instance}

function BlockWalker.new(data)
  if data.direction ~= "frontier" and data.direction ~= "genesis" then
    error("invalid walk direction " .. tostring(data.direction))
  end
  
  local self = {
    direction = data.direction,
  }
  
  self.id = uniqueRunID()
  
  local unvisited = Util.PageQueue{
    id=self.id,
    pagesize = 5000,
    store_item = function(item, state)
      local itemtype = type(item)
      if state == "active" then
        if itemtype == "string" then -- we have a block hash
          return assert(Block.find(item))
        else
          assert(Block.is_instance(item))
          return item
        end
      elseif state == "idle" then
        if itemtype == "string" then --block hash
          return item
        else
          assert(Block.is_instance(item))
          return item.hash
        end
      else
        error("weird state: " .. tostring(state))
      end
    end,
    store_page = BlockWalker.store_page,
    load_page = BlockWalker.restore_page,
    stored_page_size = BlockWalker.get_page_size,
    item_tostring = function(item)
      local block =  type(item) == "string" and Block.find(item) or item
      return ("%s %s %s %s"):format(Util.bytes_to_hex(block.hash), Account.to_readable(block.account), block.type, block.valid)
    end
  }
  self.unvisited = unvisited
  
  
  do
    local start = data.start
    if getmetatable(start) == nil then  --just a regular array of starting blocks
      local n = 0
      for _, obj in ipairs(start) do
        n = n+1
        assert(Block.is_instance(obj), "at least one start position is not a block")
        
        unvisited:add(obj)
      end
      assert(n>=1, "must have at least 1 starting block")
    else  -- probably a single start block?
      assert(Block.is_instance(start), "at least one start position is not a block")
      unvisited:add(start)
    end
  end
  
  self.visit = data.visit
    
  return setmetatable(self, BlockWalker_meta)
end

return BlockWalker

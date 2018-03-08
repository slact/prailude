local Block = require "prailude.block"
local Util =  require "prailude.util"

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
    return self.unvisited.sink:add(block)
  end,
  
  next = function(self)
    print("NEXT!!!!!", require "mm"(self.unvisited))
    
    local next_block_id = self.unvisited:next()
    if next_block_id then
      return self.visit(next_block_id)
    end
  end,
  
  each = function(self)
    local unvisited, visit = self.unvisited, self.visit
    return function()
      print("nexyt!")
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
  
  local start = data.start
  if getmetatable(start) == nil then  --just a regular array of starting blocks
    local n = 0
    for _, obj in ipairs(start) do
      n = n+1
      assert(Block.is_instance(obj), "at least one start position is not a block")
    end
    assert(n>=1, "must have at least 1 starting block")
  else  -- probably a single start block?
    assert(Block.is_instance(data.start), "at least one start position is not a block")
    data.start = { data.start }
  end
  
  local id = uniqueRunID()
  data.id = id
  
  local unvisited = Util.PageQueue{
    id=id,
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
    stored_page_size = BlockWalker.get_page_size
  }
  
  --for _, start_block in ipairs(data.start) do
--    unvisited:add(start_block)
--  end
    
  local visit = data.visit
  data.visit = function(block)
    assert(type(block)=="table")
    
    local kids = block:get_child_hashes()
    
    if block.type == "send" then
      assert(#kids == 2)
    else
      assert(#kids == 1)
    end
    
    for _, child_hash in ipairs(kids) do
      unvisited:add(child_hash)
    end
    
        
    print("RARRRRRG", unvisited:count())
    
    return visit(block)
  end
  
  data.unvisited = unvisited
  
  return setmetatable(data, BlockWalker_meta)
end

return BlockWalker

--eh...
local Block_instance = {
  
}
local block_meta = {__index = Block_instance}

local Block = {}

function Block.new(block_type, data)
  if data then
    data.type = block_type
  elseif type(block_type) == "table" then
    data = block_type
    assert(data.type, "block type is required")
  else
    data = {type = block_type}
  end
  return setmetatable(data, block_meta)
end

return Block

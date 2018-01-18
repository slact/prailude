--eh...

local Util = require "prailude.util"

local verify_block_PoW = Util.work.verify
local verify_block_PoW_test = Util.work.verify_test
local blake2b_hash = Util.blake2b.hash
local mm = require "mm"

local NilDB = require "prailude.db.nil" -- no database

local Block_instance = {
  hashable = function(self)
    local blocktype = self.type
    if blocktype == "open" then
      return self.account
    else
      return self.previous
    end
  end,
  hash = function(self)
    return blake2b_hash(self.raw, 32)
  end,
  
  verify = function(self)
    if not self:verify_PoW() then
      return nil, "insufficient proof of work"
    elseif not self:verify_signature() then
      return nil, "bad signature"
    elseif not self:verify_consistency() then
      return nil, "block inconsistent with ledger"
    else
      return true
    end
  end,
  verify_PoW = function(self)
    return verify_block_PoW(self:hashable(), self.work)
  end,
  verify_test_PoW = function(self)
    return verify_block_PoW_test(self:hashable(), self.work)
  end,
  verify_signature = function(self, account)
    if self or account then
      return true
    end
    return true --not implemented yet
  end,
  verify_consistency = function(self)
    if self then return true end
    return true -- not implemented yet
  end
}


local block_meta = {__index = Block_instance}
local Block = {}

function Block.new(block_type, data)
  if data then
    assert(type(block_type) == "string")
    data.type = block_type
  elseif type(block_type) == "table" then
    data = block_type
    assert(data.type, "block type is required")
  else
    assert(type(block_type) == "string")
    data = {type = block_type}
  end
  return setmetatable(data, block_meta)
end

function Block.get(data)
  local block = Block.new(data)
  if not block:verify_PoW() then
    return nil, "PoW check failed"
  elseif not block:verify_signature() then
    return nil, "Signature check failed"
  else
    return block
  end
end

return setmetatable(Block, NilDB.block)

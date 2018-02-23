local mm = require "mm"
local log = require "prailude.log"
local Block = require "prailude.block"
local Account = require "prailude.account"
local verify_sig = require "prailude.util".ed25519.verify
local batch_verify_sig = require "prailude.util".ed25519.delayed_batch_verify
local blake2b_hash = require "prailude.util".blake2b.hash

local Vote = {}

local Vote_meta = { __index = {
  verify = function(self, opt)
    local valid = self.valid
    if valid ~= nil then
      return valid
    else
      local verify = (opt == "batch") and batch_verify_sig or verify_sig
      valid = verify(self.hash, self.signature, self.account.id)
      self.valid = valid or false
      return valid
    end
  end
}}

--local vcache = setmetatable({}, {__mode="kv"})

function Vote.new(data)
  local self = nil --vcache[data.signature]
  if self then
    return self
  else
    local block = data.block
    if type(block) == "string" then
      assert(data.block_type)
      block = Block.unpack(data.block_type, data.block)
    end
    
    local account = data.account
    if type(account) == "string" then
      account = Account.new(data.account)
    end
    
    self = {
      block = block,
      account = account,
      sequence = data.sequence,
      signature = data.signature,
      hash = blake2b_hash(block.hash, data.sequence),
    }
    setmetatable(self, Vote_meta)
    
    return self
  end
end

return Vote

--eh...

local Util = require "prailude.util"
local Balance = require "prailude.util.balance"
local verify_block_PoW = Util.work.verify
local generate_block_PoW = Util.work.generate
local verify_block_PoW_test = Util.work.verify_test
local blake2b_hash = Util.blake2b.hash
local verify_edDSA_blake2b_signature = Util.ed25519.verify

local Account = require "prailude.account"
local mm = require "mm"
local Parser = require "prailude.util.parser"

local NilDB = require "prailude.db.nil" -- no database
local CJSON = require "cjson.safe"

local Block = {}

local block_typecode = {
  send =          2,
  receive =       3,
  open =          4,
  change =        5
}

local Block_instance = {
  rehash = function(self)
    local btype = rawget(self, "type")
    local hash, err
    if     btype == "open" then
      hash, err = blake2b_hash(self.source, self.representative, self.account)
    elseif btype == "change" then
      hash, err = blake2b_hash(self.previous, self.representative)
    elseif btype == "receive" then
      hash, err = blake2b_hash(self.previous, self.source)
    elseif btype == "send" then
      hash, err = blake2b_hash(self.previous, self.destination, self.balance:pack())
    else
      error("unknown block type " .. tostring(btype))
    end
    assert(hash, err)
    rawset(self, "hash", hash)
    return hash
  end,
  pack = function(self)
    local packed = Parser.pack_block(self)
    rawset(self, "raw", packed)
    return packed
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
  
  PoW_hashable = function(self)
    local blocktype = self.type
    if blocktype == "open" then
      return self.account
    else
      return self.previous
    end
  end,
  verify_PoW = function(self)
    return verify_block_PoW(self:PoW_hashable(), self.work)
  end,
  verify_test_PoW = function(self)
    return verify_block_PoW_test(self:PoW_hashable(), self.work)
  end,
  generate_PoW = function(self)
    local pow = generate_block_PoW(self:PoW_hashable())
    self.work = pow
    return pow
  end,
  verify_signature = function(self, account_raw)
    if self.type == "open" then
      return verify_edDSA_blake2b_signature(self.hash, self.signature, account_raw or self.account)
    elseif account_raw then
      return verify_edDSA_blake2b_signature(self.hash, self.signature, account_raw)
    else
      return true -- for now
      --error("not implemented yet")
    end
  end,
  verify_consistency = function(self)
    if self then return true end
    return true -- not implemented yet
  end,
  typecode = function(self)
    return block_typecode[self.type]
  end,
  
  to_json = function(self)
    local function b2h(b, reverse)
      if b then
        return Util.bytes_to_hex(reverse and b:reverse() or b)
      end
    end
    local function acct2r(raw)
      if raw then return Account.to_readable(raw) end
    end
    local data = {
      previous = b2h(self.previous),
      source = b2h(self.source),
      balance = b2h(type(self.balance) == "userdata" and self.balance:pack() or self.balance),
      work = b2h((self.work or ""):reverse()),
      signature = b2h(self.signature),
      representative = acct2r(self.representative),
      account = acct2r(self.account),
      destination = acct2r(self.destination),
      hash = b2h(self.hash)
    }
    return CJSON.encode(data)
  end,
  
  store = function(self)
    return Block.store(self)
  end
  
}
local block_meta = {__index = function(self, k)
  local fn = rawget(Block_instance, k)
  if fn then
    return fn
  elseif k == "hash" then
    return Block_instance.rehash(self)
  end
end}

function Block.new(block_type, data)
  if data then
    assert(type(block_type) == "string")
    if type(data) == "string" then --in the raw
      data = assert(Block.unpack(block_type, data))
    end
    data.type = block_type
  elseif type(block_type) == "table" then
    data = block_type
    assert(data.type, "block type is required")
  else
    assert(type(block_type) == "string")
    data = {type = block_type}
  end
  if data.balance then
    data.balance = Balance.unpack(data.balance)
  end
  if not block_typecode[data.type] then
    mm(data)
    error("invalid block type " .. tostring(data.type))
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

local block_cache = {}
for _, v in pairs{"open", "change", "receive", "send"} do
  block_cache[v]=setmetatable({}, {__mode="v"})
end
local cache_enabled = true

function Block.unpack_cache_enabled(val)
  if val ~= nil then
    cache_enabled = val
  end
  return cache_enabled
end

function Block.unpack(block_type, raw)
  local block, bcache
  if cache_enabled then
    bcache = rawget(block_cache, block_type)
    block = rawget(bcache, raw)
  else
    block = nil
  end
  if not block then
    local data, err = Parser.unpack_block(rawget(block_typecode, block_type), raw)
    if not data then
      return nil, err
    end
    block = Block.new(data)
    --TODO: make block immutable
    if cache_enabled then
      rawset(bcache, raw, block)
    end
  end
  return block
end

function Block.from_json(json_string)
  local data, err = CJSON.decode(json_string)
  if not data then
    return nil, err
  elseif type(data) ~= "table" then
    return nil, "json string doesn't represent an object"
  elseif not data.type then
    return nil, "block type missing"
  end
  local h2b, acct2b = Util.hex_to_bytes, Account.to_bytes
  for k, transform in pairs {previous=h2b, source=h2b, balance=h2b, work=h2b, signature=h2b, representative = acct2b, account=acct2b, destination=acct2b} do
    if data[k] then
      data[k]=transform(data[k])
    end
  end
  
  local block
  block, err = Block.new(data)
  if not block then return
    nil, err
  end
  
  if block.work then
    block.work = block.work:reverse()
    if not block:verify_PoW() then
      return nil, "PoW verification failed"
    end
  end
  
  return block
end

function Block.batch_verify_signatures(blocks, account_pubkey)
  local batch = {}
  for i, block in ipairs(blocks) do
    rawset(batch, i, {block.hash, block.signature, account_pubkey})
  end
  local all_valid = Util.ed25519.batch_verify(batch)
  if all_valid then
    return true
  else
    local valid = {}
    for i, v in ipairs(batch) do
      rawset(valid, i, rawget(v, "valid"))
    end
    return false, valid
  end
end

local main_net = true
if main_net then
  Block.genesis = Block.new("open", {
    source =         Util.hex_to_bytes("E89208DD038FBB269987689621D52292AE9C35941A7484756ECCED92A65093BA"),
    representative = Account.to_bytes("xrb_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3"),
    account =        Account.to_bytes("xrb_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3"),
    work =           Util.hex_to_bytes("62f05417dd3fb691"):reverse(), --little endian
    signature =      Util.hex_to_bytes("9F0C933C8ADE004D808EA1985FA746A7" ..
                                       "E95BA2A38F867640F53EC8F180BDFE9E" ..
                                       "2C1268DEAD7C2664F356E37ABA362BC5" ..
                                       "8E46DBA03E523A7B5A19E4B6EB12BB02")
  })
  assert(Block.genesis:pack())
  assert(Block.genesis.hash == Util.hex_to_bytes("991CF190094C00F0B68E2E5F75F6BEE9"..
                                                 "5A2E0BD93CEAA4A6734DB9F19B728948"),
        "Genesis block hash doesn't match expected value")
else
  Block.genesis = Block.new("open", {
    source =         Util.hex_to_bytes("B0311EA55708D6A53C75CDBF88300259C6D018522FE3D4D0A242E431F9E8B6D0"),
    representative = Account.to_bytes("xrb_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo"),
    account =        Account.to_bytes("xrb_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo"),
    work =           Util.hex_to_bytes("9680625b39d3363d"):reverse(), --little endian
    signature =      Util.hex_to_bytes("ECDA914373A2F0CA1296475BAEE40500"..
                                       "A7F0A7AD72A5A80C81D7FAB7F6C802B2"..
                                       "CC7DB50F5DD0FB25B2EF11761FA7344A"..
                                       "158DD5A700B21BD47DE5BD0F63153A02")
  })
  assert(Block.genesis:pack())
end
assert(Block.genesis:verify())

return setmetatable(Block, NilDB.block)

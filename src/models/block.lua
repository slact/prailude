local mainnet = true

local Util = require "prailude.util"
local Balance = require "prailude.util.balance"
local verify_block_PoW = mainnet and Util.work.verify or Util.work.verify_test
local generate_block_PoW = Util.work.generate
local blake2b_hash = Util.blake2b.hash
local verify_edDSA_blake2b_signature = Util.ed25519.verify
local tinsert = table.insert

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

local required_fields = {
  send = {    [true] = {"previous", "destination", "balance"},  [false] = {"representative", "source"}},
  receive = { [true] = {"previous", "source"},                  [false] = {"representative", "destination"}},
  open = {    [true] = {"source", "representative", "account"}, [false] = {"destination"}},
  change = {  [true] = {"previous", "representative"},          [false] = {"source", "destination"}}
}

local GENESIS_HASH

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
  
  is_valid = function(self, lvl)
    local valid = self.valid
    if lvl == "PoW" then
      return valid == "PoW" or valid == "signature" or valid == "ledger" or valid == "confirmed"
    elseif lvl == "signature" then
      return valid == "signature" or valid == "ledger" or valid == "confirmed"
    elseif lvl == "ledger" then
      return valid == "ledger" or valid == "confirmed"
    elseif lvl == "confirmed" then
      return valid == "confirmed"
    else
      error("validation level invalid:" .. tostring(lvl))
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
    if self:is_valid("PoW") then
      --already checked
      return true
    else
      local ok, err = verify_block_PoW(self:PoW_hashable(), self.work)
      if ok then
        self.valid = "PoW"
      end
      return ok, err
    end
  end,
  generate_PoW = function(self)
    local pow = generate_block_PoW(self:PoW_hashable())
    self.work = pow
    return pow
  end,
  verify_signature = function(self, account_raw)
    if self:is_valid("signature") then
      --alredy checked
      return true
    else
      local ok, err = verify_edDSA_blake2b_signature(self.hash, self.signature, account_raw or self.account)
      if ok then
        self.valid = "signature"
      end
      return ok, err
    end
  end,
  
  verify_ledger = function(self)
    --print("VERIFY_LEDGER " .. Account.to_readable(self.account) .. " hash:", Util.bytes_to_hex(self.hash), " type:", self.type, "gd:", self.genesis_distance, " valid:", self.valid)
    if self:is_valid("ledger") then
      --print("already validated")
      return true
    elseif self.hash == GENESIS_HASH then
      --print("genesis block")
      self.genesis_distance = 0
      self.valid = "confirmed" --for sure
      return true
    end
    
    local btype = self.type
    --check that the block types have the expected fields
    for must_exist, fset in ipairs(required_fields[btype]) do
      for _, field in ipairs(fset) do
        if must_exist and not self[field] then
          return nil, ("%s block is missing %s field"):format(btype, field)
        elseif not must_exist and self[field] then
          return nil, ("%s block has a %s field -- and it shouldn't"):format(btype, field)
        end
      end
    end
    
    --print("prev:", Util.bytes_to_hex(prev_hash), " valid:", prev.valid)
    local prev_block
    if not self.previous then --open block
      assert(btype == "open")
      if self.account == Account.burn.id then
        return nil, "someone did the impossible -- find a privkey for the burn account"
      end
    else
      --verify previous block
      prev_block = self:get_previous()
      --print("VERIFY: PREVIOUS IS", prev_block:debug())
      if not prev_block then
        return nil, "gap", "previous"
      elseif not prev_block:is_valid("ledger") then
        return nil, "retry"
      end
    end
      
    local source_block
    if self.source then --`receive` or `open` block
      assert(btype == "receive" or btype == "open")
      --was the send directed to this account?
      source_block = self:get_source()
      if not source_block then
        return nil, "gap", "source"
      elseif not source_block:is_valid("ledger") then
        return nil, "retry"
      end
      if self.account ~= source_block.destination then
        return nil, ("send block %s's destination doesn't match"):format(Util.bytes_to_hex(self.source))
      end
    end
    
    if not self.balance then
      self:get_balance() -- fill in the blanks. auto-sets self.balance
    elseif self.source and self.destination then
      error("this is something ublocks might do. this is not supported yet")
    elseif self.destination then
      --TODO: u-block compatibility
      assert(btype == "send")
      local prev_balance = prev_block:get_balance()
      if self.balance > prev_balance then
        return nil, "send block wrongly credited balance to own account"
      elseif self.balance < Balance.zero then --should be impossible, balance is unsigned
        return nil, "balance cannot be < 0"
      end
    elseif self.source then
      local source_amount = source_block and source_block:get_send_amount() or Balance.zero
      local prev_balance = prev_block and prev_block:get_balance() or Balance.zero
      if(self.balance ~= source_amount + prev_balance) then
        return nil, "balance doesn't match send + previous"
      end
    end
  
    self.genesis_distance = (prev_block or source_block).genesis_distance + 1
    self.valid = "ledger"
    return true
  end,
  
  update_ledger_validation = function(self)
    return Block.update_ledger_validation(self)
  end,
  
  --verify_ledger --defined further down
  typecode = function(self)
    return block_typecode[self.type]
  end,
  
  debug = function(self)
    local function b2h(b, reverse)
      if b then
        return Util.bytes_to_hex(reverse and b:reverse() or b)
      end
    end
    local function acct2r(raw)
      if raw then return Account.to_readable(raw) end
    end
    local mt = getmetatable(self)
    setmetatable(self, nil)
    local tid = tostring(self)
    setmetatable(self, mt)
    
    local out = {tid, "hash: "..b2h(self.hash)}
    table.insert(out, "type: ".. self.type)
    table.insert(out, "valid: "..(self.valid or "none"))
    if self.previous then
      table.insert(out, "prev: "..b2h(self.previous))
    end
    if self.source then
      table.insert(out, "src: "..b2h(self.source))
    end
    if self.account then
      table.insert(out, "acct: "..acct2r(self.account))
    end
    
    if self.destination then
      table.insert(out, "dst: "..acct2r(self.destination))
    end
    if self.balance then
      table.insert(out, "bal: ".. tostring(self.balance))
    end
    return table.concat(out, ", ")
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
  
  store = function(self, opt)
    assert(self.account, "gotta have an account")
    return Block.store(self, opt)
  end,
  store_later = function(self)
    return Block.store_later(self)
  end,
  
  get_account = function(self)
    return Account.find(assert(self.account, "gotta have an account"))
  end,
  
  get_send_amount = function(self)
    assert(self.type == "send")
    local own_balance, prev_balance = self:get_balance(), Block.find(self.previous):get_balance()
    --print(debug.traceback())
    --print("GET_SEND_AMOUNT", prev_balance > own_balance, Util.bytes_to_hex(self.hash), Account.to_readable(self.account), prev_balance, own_balance)
    return prev_balance - own_balance
  end,
  
  get_prev_balance = function(self)
    if self.type == "open" then
      return Balance.zero
    else
      return self:get_previous():get_balance()
    end
  end,
  
  get_balance = function(self)
    local blocktype = self.type
    if self.hash == GENESIS_HASH then
      --print("GEMESIS BALANCE ", tostring(Balance.genesis))
      return Balance.genesis
    elseif self.balance then
      return self.balance
    elseif blocktype == "open" then
      if self.balance then
        return self.balance
      else
        local source = Block.find(self.source)
        if not source then
          error(("source for open block not found. block: %s"):format(self:debug()))
        end
        local sent_balance = source:get_send_amount()
        self.balance = sent_balance
        return sent_balance
      end
    elseif blocktype == "receive" then
      local balance, parent, source
      parent = Block.find(self.previous)
      source = Block.find(self.source)
      assert(parent, "parent for receive block missing when trying to get balance")
      assert(source, "source for receive block missing when trying to get balance")
      local parent_balance, source_balance = (parent:get_balance()), (source:get_balance())
      if not parent_balance then
        error(("parent of block has no balance. block: %s. parent: %s"):format(self:debug(), parent:debug()))
      elseif not source_balance then
        error(("source of block has no balance. block: %s. parent: %s"):format(self:debug(), source:debug()))
      end
      balance = assert(parent:get_balance()) + assert(source:get_send_amount(), "send block balance missing")
      self.balance = balance
      return balance
    elseif blocktype == "change" or blocktype == "send" then
      local parent = Block.find(self.previous)
      assert(parent, "parent for block missing when trying to get balance")
      local parent_balance = parent:get_balance()
      if not parent_balance then
        error(("parent of block has no balance. block: %s. parent: %s"):format(self:debug(), parent:debug()))
      end
      return parent_balance
    else
      error("unknown blocktype " .. tostring(blocktype) .. " for block " .. self:debug())
    end
  end,
  
  get_next = function(self)
    return Block.find_block_by("previous", self.hash)
  end,
  
  get_previous = function(self)
    local b =  Block.find(self.previous)
    return b
  end,
  get_source = function(self)
    return Block.find(self.source)
  end,
  
  get_destination = function(self)
    return Block.find_block_by("source", self.hash)
  end,
  
}
local Block_meta = { __index = Block_instance }

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
  
  local valid = data.valid
  if valid == 0 then
    data.valid = nil
  elseif valid == 1 then
    data.valid = "PoW"
  elseif valid == 2 then
    data.valid = "signature"
  elseif valid == 3 then
    data.valid = "ledger"
  elseif valid == 4 then
    data.valid = "confirmed"
  end
  local block = setmetatable(data, Block_meta)
  if not data.defer_hash and not data.hash then
    block:rehash()
  end
  return block
end

function Block.is_instance(obj)
  return type(obj) == "table" and getmetatable(obj) == Block_meta
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
local cache_enabled = false

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
  local valid
  for i, block in ipairs(blocks) do
    valid = block.valid
    if not valid or (valid ~= "signature" and valid ~= "ledger" and valid ~= "confirmed") then
      tinsert(batch, {block.hash, block.signature, account_pubkey, i=i, block=block})
    end
  end
  local all_valid = Util.ed25519.batch_verify(batch)
  if all_valid then
    if #batch == #blocks then --a little optimization
      for _, block in ipairs(blocks) do
        block.valid = "signature"
      end
    else
      
      for _, b in ipairs(batch) do
        blocks[b.i].valid = "signature"
      end
    end
    return true
  else
    valid = {}
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
    work =           Util.hex_to_bytes("62f05417dd3fb691"):reverse(), --little_endian
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
    work =           Util.hex_to_bytes("9680625b39d3363d"):reverse(), --little_endian
    signature =      Util.hex_to_bytes("ECDA914373A2F0CA1296475BAEE40500"..
                                       "A7F0A7AD72A5A80C81D7FAB7F6C802B2"..
                                       "CC7DB50F5DD0FB25B2EF11761FA7344A"..
                                       "158DD5A700B21BD47DE5BD0F63153A02")
  })
  assert(Block.genesis:pack())
end
GENESIS_HASH = Block.genesis.hash

assert(Block.genesis:verify_PoW())
assert(Block.genesis:verify_signature())
assert(Block.genesis:verify_ledger())

return setmetatable(Block, NilDB.block)

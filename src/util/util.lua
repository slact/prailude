local cutil = require "prailude.util.lowlevel"
local crypto = require "prailude.util.crypto"
local timer = require "prailude.util.timer"
local coroutine_util = require "prailude.util.coroutine"
local parser = require "prailude.util.parser"
local balance = require "prailude.util.balance"

local blake2b_init, blake2b_update, blake2b_final = crypto.blake2b_init, crypto.blake2b_update, crypto.blake2b_finalize
local blake2b_hash = crypto.blake2b_hash

local unpack_account_with_checksum = cutil.unpack_account_with_checksum
local pack_account_with_checksum = cutil.pack_account_with_checksum


local util = {}

do
  local batchsink_mt = {__index={
    add = function(self, data)
      table.insert(self.batch, data)
      if self:full() then
        return self:consume()
      end
      return self
    end,
    full = function(self)
      return #(rawget(self, "batch")) >= self.batch_size
    end,
    finish = function(self)
      return self:consume()
    end,
    consume = function(self)
      self.__consume(self.batch)
      self:clear()
      return self
    end,
    clear = function(self)
      rawset(self, "batch", {})
    end,
    batch_size = 10000
  }}
  
  function util.BatchSink(opt, fn)
    if type(fn)=="function" and type(opt)=="number" then
      opt={consume=fn, batch_size=opt}
    end
    return setmetatable({
      __consume = opt.consume,
      batch_size  = opt.batch_size,
      batch = {},
    }, batchsink_mt)
  end
end

do
  local batchsource_mt = {__index = {
    next = function(self)
      local batch = self.batch
      local n = rawget(self, "__batch_n")
      if n >= #batch then
        local total_n = rawget(self, "__total_n") + n
        batch = self.__produce(total_n) or {}
        rawset(self, "__total_n", total_n)
        n = 1
        self.batch = batch
      else
        n = n+1
      end
      rawset(self, "__batch_n", n)
      return rawget(batch, n)
    end,
    
    each = function(self)
      return function()
        return self:next()
      end
    end
  }}
  
  function util.BatchSource(opt)
    if type(opt) == "function" then
      opt = {produce = opt}
    end
    return setmetatable( {
      __produce = opt.produce,
      __batch_n = 0,
      __total_n = 0,
      batch = {},
    }, batchsource_mt)
  end
end

local PageQueue; do

  -- load_page(queue_id, page_id); BlockWalker.pop_page(self.walk_id, self.page_id)
  -- store_page(queue_id, page_id, data)
  -- store_item(item, page_state)
  -- stored_page_size(queue_id, page_id)
  -- page_id
  
  local Page_meta = {__index = {
    change_state = function(self, new_state)
      local state = self.state
      if state == new_state then
        return self --nothing to do
      end
      
      if state == "stored" then -- must at least load from storage
        local data = self.load_page(self.queue_id, self.id)
        assert(type(data)=="table", "load_page gave a bad result")
        self.data = data
      end
      
      if new_state == "idle" and state == "stored" then
        self.state = new_state
        return self -- we're done here
      elseif new_state == "active" and (state == "idle" or state == "stored") then
        local store_item, data = self.store_item, self.data
        for i, item in ipairs(data) do
          item = store_item(item, "active")
          assert(item ~= nil, "store_item gave a nil result")
          rawset(data, i, item)
        end
        self.first, self.last = 1, #self.data
      end
      
      if (new_state == "idle" or new_state == "stored") and state == "active" then
        --idle pages only store compressed data
        local store_item, data, newdata = self.store_item, self.data, {}
        for i=self.first, self.last do
          local item = store_item(data[i], "idle")
          assert(item ~= nil, "store_item gave a nil result")
          table.insert(newdata, item)
        end
        self.data, self.first, self.last = newdata, nil, nil
      end
      if new_state == "stored" then
        self.store_page(self.queue_id, self.id, self.data)
        self.data = nil
      end
      self.state = new_state
      return self
    end,
    
    add = function(self, item)
      local state = self.state
      item = self.store_item(item, state)
      local data = self.data
      if not data then
        data = {}
        self.data = data
      end
      if state == "active" then
        local last = (rawget(self,"last") or 0) + 1
        rawset(data, last, item)
        rawset(self, "last", last)
        if not self.first then
          self.first = last
        end
        return true
      elseif state == "idle" then
        table.insert(data, item)
      else
        error("can't add block to " .. tostring(state) .. " page")
      end
      return true
    end,
    
    next = function(self)
      assert(self.state == "active")
      local data = self.data
      local first, last = self.first, self.last
      if not data or not first or not last or first > last then
        return nil, "empty"
      end
      local ret = data[first]
      data[first] = nil
      self.first = first + 1
      return ret
    end,
    
    debug_each = function(self, callback)
      if self.state == "active" then
        for i=self.first, self.last do
          callback(self.data[i], "active")
        end
      elseif self.state == "idle" then
        for _, item in ipairs(self.data) do
          callback(item, "idle")
        end
      elseif self.state == "stored" then
        self:change_state("idle")
        self:debug_each(callback)
        self:change_state("stored")
      else
        error("unexpected page state " .. tostring(self.state))
      end
    end,
    
    count = function(self)
      if self.state == "active" then
        if self.data then
          return self.last - self.first + 1
        else
          return 0
        end
      elseif self.state == "idle" then
        local data = self.data
        return data and #data or 0
      else
        return self.stored_page_size(self.queue_id, self.id)
      end
    end,
  }}
  
  local function Page(queue_id, page_id, data_handlers, state)
    return setmetatable({
      queue_id = queue_id,
      id = page_id,
      
      store_item = data_handlers.store_item,
      store_page = data_handlers.store_page,
      load_page = data_handlers.load_page,
      
      state=state
    }, Page_meta)
  end

  
  local PageQueue_meta = {__index = {
    pagesize = 5000,
    add = function(self, item)
      local lastpage = self.last_page
      if not lastpage then
        lastpage = self:add_page()
      end
      if lastpage:count() < self.pagesize then
        return lastpage:add(item)
      else
        self:add_page()
        return self:add(item)
      end
    end,
    
    add_page = function(self)
      local next_page_id = self.next_page_id or 0
      self.next_page_id = next_page_id + 1
      local page
      if self.first_page == nil then
        page = Page(self.id, next_page_id, self.data_handlers, "active")
        self.first_page = page
      else
        if self.last_page ~= self.first_page then
          self.last_page:change_state("stored")
        end
        page = assert(Page(self.id, next_page_id, self.data_handlers, "idle"))
      end
      table.insert(self.pages, page)
      self.last_page = page
      return page
    end,
    
    next_page = function(self)
      local removed = table.remove(self.pages, 1) -- remove old first active page
      if self.last_page == removed then
        self.last_page = nil
      end
      local new_first_page = self.pages[1]
      self.first_page = new_first_page
      if new_first_page then
        new_first_page:change_state("active")
        return new_first_page
      end
    end,
    
    next = function(self)
      local firstpage = self.first_page
      if firstpage == nil then
        return nil
      else
        local ret = firstpage:next()
        if not ret then
          self:next_page()
          return self:next()
        else
          return ret
        end
      end
    end,
    
    each = function(self)
      return function()
        return self:next()
      end
    end,
    
    count = function(self)
      local n = 0
      for _, page in ipairs(self.pages) do
        n = n + page:count()
      end
      return n
    end,
    
    debug_each = function(self, callback)
      for _, page in ipairs(self.pages) do
        page:debug_each(callback)
      end
    end,
    
    debug_print = function(self)
      local n, item_tostring = 0, self.data_handlers.item_tostring
      local callback = function(item, status)
        n = n + 1
        print(("%4i: %s"):format(n, item_tostring(item, status)))
      end
      return self:debug_each(callback)
    end
    
  }}
  
  function PageQueue(opt)
    local pq = {
      pages = {},
      pagesize = opt.pagesize,
      id = assert(opt.id, "id missing"),
      data_handlers = {
        store_item = assert(opt.store_item, "store_item missing"),
        store_page = assert(opt.store_page, "store_page missing"),
        load_page =  assert(opt.load_page,  "load_page missing"),
        item_tostring = opt.item_tostring or tostring
      }
    }
    assert(pq.id, "PageQueue ID required")
    
    pq = setmetatable(pq, PageQueue_meta)
    pq:add_page()
    return pq
  end

end

util.PageQueue = PageQueue

local MAX_BATCH_SIZE = 64
local Ed25519Batch = {
  interval = 250, --ms
  batch = {},
  timer = nil
}

util.Cache = function(mode)
  if mode == "off" then
    return {
      get = function()
        return nil
      end,
      set = function(_, _, val)
        return val
      end,
      clear = function() end,
      cache = {}
    }
  elseif mode == "weak" then
    --done this way instead of a function lookup index for efficiency
    local cache
    local obj = {
      get = function(_, id)
        return rawget(cache, id)
      end,
      set = function(_, id, value)
        rawset(cache, id, value)
        return value
      end,
      clear = function()
        cache = setmetatable({}, {__mode="v"})
      end,
      cache = cache,
    }
    obj:clear()
    return obj
  elseif not mode then
    error("missing cache mode")
  else
    error("unknown cache mode " .. tostring(mode))
  end
end

function Ed25519Batch.add(msg, sig, pubkey, coro)
  local batch = Ed25519Batch.batch
  assert(#batch < MAX_BATCH_SIZE)
  table.insert(batch, {msg, sig, pubkey, coro})
  if #batch == MAX_BATCH_SIZE then
    print("do a batch right now")
    Ed25519Batch.batch = {}
    local all_valid = crypto.edDSA_blake2b_batch_verify(#batch, batch)
    if all_valid then
      for i=1, MAX_BATCH_SIZE-1 do
        coroutine_util.resume(batch[i][4], true)
      end
      --now the last coroutine
      return true
    else
      local b
      for i=1, MAX_BATCH_SIZE-1 do
        b = rawget(batch, i)
        coroutine_util.resume(b[4], b.valid)
      end
      --now the last coroutine
      return batch[MAX_BATCH_SIZE].valid
    end
  else
    if not Ed25519Batch.timer then
      Ed25519Batch.start_timer()
    end
    return coroutine_util.yield()
  end
end

function Ed25519Batch.start_timer()
  Ed25519Batch.timer = timer.interval(Ed25519Batch.interval, function()
    local batch = Ed25519Batch.batch
    print("do a batch of size", #batch)
    if #batch == 0 then
      Ed25519Batch.timer = nil
      return false --stops timer
    else
      Ed25519Batch.batch = {}
      local all_valid = crypto.edDSA_blake2b_batch_verify(#batch, batch)
      for _, b in ipairs(batch) do
        coroutine_util.resume(b[4], b.valid or all_valid)
      end
    end
  end)
end

util.timer = timer
util.blake2b = {
  init = blake2b_init,
  update = blake2b_update,
  final = blake2b_final,
  hash = blake2b_hash,
}
util.work = {
  verify = crypto.raiblocks_verify_work,
  verify_test = crypto.raiblocks_verify_work,
  generate = crypto.raiblocks_generate_work
}
util.argon2d_hash = crypto.argon2d_raiblocks_hash
util.ed25519 = {
  get_public_key = crypto.edDSA_blake2b_get_public_key,
  sign = crypto.edDSA_blake2b_sign,
  verify = crypto.edDSA_blake2b_verify,
  batch_verify = function(batch)
    local all_valid = crypto.edDSA_blake2b_batch_verify(#batch, batch)
    if all_valid then
      return true
    else
      for _, v in pairs(batch) do
        if rawget(v, "valid") == nil then
          rawset(v, "valid", true)
        end
      end
      return false
    end
  end,
  delayed_batch_verify = function(msg, sig, pubkey)
    assert(#sig == 64, "signature length must be 64")
    assert(#pubkey == 32, "pubkey length must be 32")
    local coro = assert(coroutine_util.running(), "batch_verify must be called in a coroutine")
    return Ed25519Batch.add(msg, sig, pubkey, coro)
  end
}
util.parser = parser
util.unpack_account = function(raw)
  return unpack_account_with_checksum(raw, blake2b_hash(raw, 5))
end
util.pack_account = function(account)
  if #account ~= 64 then
    return nil, "invalid account", "wrong length"
  end
  local raw, checksum_or_error = pack_account_with_checksum(account)
  if not raw then
    return nil, checksum_or_error
  end
  if blake2b_hash(raw, 5) ~= checksum_or_error then
    return nil, "invalid account", "checksum fail"
  end
  return raw
end
  
util.vanitygen = function(str, center)
  local not_alphabet_match = "[^13456789abcdefghijkmnopqrstuwxyz]"
  local map = {
    [" "] = "1",
      l   = "1",
      v   = "w",
    ["2"] = "3",
    ["0"] = "o"
  }
  str = str:lower()
  if #str > 51 then
    return nil, "string too big"
  end
  print("str", str)
  str = str:gsub(".", map)
  print("str", str)
  local badchar = str:match(not_alphabet_match)
  if badchar then
    return nil, "character '" .. badchar .. "' not possible in address"
  end
  
  local fill = 51 - #str
  if not center then
    str = "xrb_1" .. str .. ("1"):rep(fill) .. "bbbbbbbb"
  else
    str = "xrb_1" .. ("1"):rep(math.floor(fill/2)) .. str .. ("1"):rep(math.ceil(fill/2)).."bbbbbbbb"
  end
  local raw, err = pack_account_with_checksum(str)
  if not raw then return nil, err end
  return unpack_account_with_checksum(raw, blake2b_hash(raw, 5))
end

util.bytes_to_hex = cutil.bytes_to_hex
util.bytes_to_hex_debug = function(bytes)
  local hex = cutil.bytes_to_hex(bytes)
  local n = 0
  local out = {""}
  for byte in hex:gmatch("..") do
    table.insert(out, byte)
    n = n+1
    if n%16 == 0 then
      table.insert(out, "\n")
    elseif n%8 == 0 then
      table.insert(out, " ")
    end
  end
  return table.concat(out, " ")
end
util.hex_to_bytes = function(hex)
  return cutil.hex_to_bytes(hex:upper())
end
util.print_hex = cutil.print_hex
util.balance = balance

util.coroutine = coroutine_util

return util

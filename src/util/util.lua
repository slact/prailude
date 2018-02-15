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

local MAX_BATCH_SIZE = 64
local Ed25519Batch = {
  interval = 250, --ms
  batch = {},
  timer = nil
}

function Ed25519Batch.add(msg, sig, pubkey, coro)
  local batch = Ed25519Batch.batch
  assert(#batch < MAX_BATCH_SIZE)
  table.insert(batch, {msg, sig, pubkey, coro})
  if #batch == MAX_BATCH_SIZE then
    print("do a batch right now")
    Ed25519Batch.batch = {}
    local all_valid = crypto.edDSA_blake2b_batch_verify(batch)
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
      local all_valid = crypto.edDSA_blake2b_batch_verify(batch)
      for _, b in ipairs(batch) do
        coroutine_util.resume(b[4], b.valid or all_valid)
      end
    end
  end)
end


local util = {
  timer = timer,
  
  blake2b = {
    init = blake2b_init,
    update = blake2b_update,
    final = blake2b_final,
    hash = blake2b_hash,
  },
  
  work = {
    verify = crypto.raiblocks_verify_work,
    verify_test = crypto.raiblocks_verify_work,
    generate = crypto.raiblocks_generate_work
  },
  
  argon2d_hash = crypto.argon2d_raiblocks_hash,
  
  ed25519 = {
    get_public_key = crypto.edDSA_blake2b_get_public_key,
    sign = crypto.edDSA_blake2b_sign,
    verify = crypto.edDSA_blake2b_verify,
    batch_verify = function(msg, sig, pubkey)
    assert(#sig == 64, "signature length must be 64")
      assert(#pubkey == 32, "pubkey length must be 32")
      local coro = assert(coroutine_util.running(), "batch_verify must be called in a coroutine")
      return Ed25519Batch.add(msg, sig, pubkey, coro)
    end
  },

  parser = parser,
  
  unpack_account = function(raw)
    return unpack_account_with_checksum(raw, blake2b_hash(raw, 5))
  end,

  pack_account = function(account)
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
  end,
  
  vanitygen = function(str, center)
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
  end,
  
  bytes_to_hex = cutil.bytes_to_hex,
  hex_to_bytes = function(hex)
    return cutil.hex_to_bytes(hex:upper())
  end,
  print_hex = cutil.print_hex,
  
  coroutine = coroutine_util,
  
  balance = balance
}

return util

local bc = require "bc"
local cutil = require "prailude.util.lowlevel"
local crypto = require "prailude.util.crypto"
local timer = require "prailude.util.timer"
local coroutine_util = require "prailude.util.coroutine"
local parser = require "prailude.util.parser"

local blake2b_init, blake2b_update, blake2b_final = crypto.blake2b_init, crypto.blake2b_update, crypto.blake2b_finalize
local blake2b_hash = crypto.blake2b_hash

local unpack_account_with_checksum = cutil.unpack_account_with_checksum
local pack_account_with_checksum = cutil.pack_account_with_checksum
local unpack_balance_raw = cutil.unpack_balance_raw

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
  },
  
  argon2d_hash = crypto.argon2d_raiblocks_hash,
  
  ed25519 = {
    get_public_key = crypto.edDSA_blake2b_get_public_key,
    sign = crypto.edDSA_blake2b_sign,
    verify = crypto.dDSA_blake2b_verify
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
  
  unpack_balance = function(raw, unit)
    local prev_bc_scale = bc.digits(20)
    if #raw ~= 16 then
      return nil, "invalid balance"
    end
    local str_balance_raw, err = unpack_balance_raw(raw)
    if not str_balance_raw then
      return nil, err
    end
    local balance = bc.number(str_balance_raw)
    if unit == 'xrb' then
      balance = balance / (10^24)
    elseif unit == 'kxrb' then
      balance = balance / (10^27)
    elseif unit == 'Mxrb' or unit == 'XRB' then
      balance = balance / (10^30)
    elseif unit then
      return nil, "unknown unit " .. tostring(unit)
    end
    bc.digits(prev_bc_scale)
    return balance
  end,
  
  bytes_to_hex = cutil.bytes_to_hex,
  hex_to_bytes = function(hex)
    return cutil.hex_to_bytes(hex:upper())
  end,
  print_hex = cutil.print_hex,
  
  coroutine = coroutine_util
}

return util

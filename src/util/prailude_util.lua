local bc = require "bc"
local cutil = require "prailude.util.lowlevel"
local lz = require "luazen"
local blake2b_init, blake2b_update, blake2b_final = lz.blake2b_init, lz.blake2b_update, lz.blake2b_final
local blake2b_hash = function(data, bytes)
  local ctx = blake2b_init(bytes)
  blake2b_update(ctx, data)
  return blake2b_final(ctx)
end

local unpack_account_with_checksum = cutil.unpack_account_with_checksum
local pack_account_with_checksum = cutil.pack_account_with_checksum
local unpack_balance_raw = cutil.unpack_balance_raw

local util = {
  blake2b = {
    init = blake2b_init,
    update = blake2b_update,
    final = blake2b_final,
    hash = blake2b_hash,
  },

  unpack_account = function(raw)
    return unpack_account_with_checksum(raw, blake2b_hash(raw, 5))
  end,

  pack_account = function(account, opt)
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
  
  to_hex = cutil.to_hex
}

return util

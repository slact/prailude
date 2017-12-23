local prailude = {}

local bc = require "bc"
local cutil = require "prailude.util"
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

prailude.util = {
  blake2b = {
    init = blake2b_init,
    update = blake2b_update,
    final = blake2b_final,
    hash = blake2b_hash
  },

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

return prailude

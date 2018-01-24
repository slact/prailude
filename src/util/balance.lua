local bc = require "bc"
local mm = require "mm"

bc.digits(40) --2^128 ~~ 10^38, so 40 digits past the decimal is plenty enough
local Balance = require "prailude.util.balance.lowlevel"
local numstring_drop_zeroes_past_decimal = Balance.numstring_drop_zeroes_past_decimal
local unit_to_raw = {
  raw = 1,
  Gxrb = bc.number(10^33),
  Mxrb = bc.number(10^30),
  kxrb = bc.number(10^27),
  Kxrb = bc.number(10^27),
  xrb =  bc.number(10^24),
  mxrb = bc.number(10^21),
  uxrb = bc.number(10^18)
}

local max_balance = bc.number("340282366920938463463374607431768211455")

local function scale(num, unit, reverse)
  unit = unit or Balance.default_unit()
  local mult = unit_to_raw[unit]
  assert(mult, "unknown unit " .. tostring(unit))
  local n = bc.number(num)
  if tonumber(n) == 0 and not tostring(num):match("^0*%.?0*$") then
    error("can't parse number " .. tostring(num))
  end
  if not reverse then
    n = n * mult
  else
    n = n / mult
  end
  if n > max_balance then
    error("number is too large")
  end
  return n
end

local mt = Balance.mt

local function is_balance(val)
  return type(val) == "userdata" and getmetatable(val) == mt
end

local new = Balance.new
Balance.new = function(num, unit)
  local self, err
  if is_balance(num) then
    self = num:copy()
  elseif type(num)=="string" and num:match("^0x") then
    --hex
    self = new(num)
  else
    num, err= scale(num, unit)
    if not num then
      return nil, err
    end
    self, err = new(tostring(num:trunc()))
    if not self then
      return nil, err
    end
  end
  if unit then
    self:unit(unit)
  end
  return self
end

local balance_unpack = Balance.unpack
function Balance.unpack(packed, unit)
  local self, err = balance_unpack(packed)
  if not self then return nil, err end
  if unit then
    self:unit(unit)
  end
  return self
end

local function rescale(self, val, must_be_balance)
  if is_balance(val) then
    return val
  else
    local str = tostring(assert(scale(val, self:unit())))
    if must_be_balance then
      local balance = Balance.new(str, "raw")
      balance:unit(self:unit())
      return balance
    else
      return str
    end
  end
end

local rescalable = {"add", "sub", "mul", "div", "mod", "eq", "lt", "le"}
for _, k in ipairs(rescalable) do
  local orig = mt["__" .. k]
  mt["__" .. k] = function(self, val)
    return orig(self, rescale(self, val, true))
  end
end


for _, k in pairs{"add", "subtract"} do
  local orig = mt.__index[k]
  mt.__index[k] = function(self, val)
    return orig(self, rescale(self, val, true))
  end
end

local balance_to_string = mt.__tostring
mt.__tostring = function(self)
  local res = scale(balance_to_string(self), self:unit(), "reverse")
  return numstring_drop_zeroes_past_decimal(tostring(res))
end

function mt.__tonumber(self) -- tonumber(self) doesn't actually call this. this is for future-proofing, basically
  return tonumber(tostring(self))
end
  
mt.__index.tostring = mt.__tostring
mt.__index.tonumber = mt.__tonumber

Balance.genesis = Balance.new("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
Balance.genesis:lock()

return Balance

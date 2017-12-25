local lowlevel = require "raiblocks.message.lowlevel"
local message_pack = lowlevel.pack

local msg_types = {
  invalid =       0,
  not_a_type =    1,
  keepalive =     2,
  publish =       3,
  confirm_req =   4,
  confirm_ack =   5,
  bulk_pull =     6,
  bulk_push =     7,
  frontier_req =  8
}

local attributes = {
  --header
  net = true, -- test / beta / live net
  net_version_max = true,
  net_version_cur = true,
  net_version_min = true,
  type = true, --message type (string)
  extensions = true, -- (0-255 int)
  block_type = true, -- string
  
  --keepalive
  peers = true, -- table, maxlen=8, {peer_address(string), peer_port(int)}  
  
  --etc
}

--instance metatable
local Message_metatable = {
  __index = {
    pack = function(self)
      local packed = message_pack(self)
      rawset(self, "_packed", packed)
      return packed
    end,
    
    typecode(self)
      return rawget(msg_types, rawget(self, "type"))
    end,
    
    block_typecode(self)
      local block = rawget(self, "block")
      if block then
        return block:typecode()
      else
        return 0 --not a block
      end
    end
  }
}

-- the class-like
local Message = {
  new = function(msgtype, data)
    assert(rawget(known_message_types[
    local msg = setmetatable({}, Message_metatable)
    
    rawset(msg, "type", msgtype)
    if not rawget(msg_types, msgtype) then
      error("unknown msgtype " .. tostring(msgtype))
    end
    
    --default to mainnet
    rawset(msg, "net", "main")
    
    --shallow-copy the data
    for k, v in pairs(data) do
      rawset(msg, k, v)
    end
    return msg
  end,
  unpack = function(self)
    
  end,
  get = function(msg_hash)
    --todo
  end,
  find = function(msg_hash)

  end
}

return Message

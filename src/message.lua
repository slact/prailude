local parser = require "prailude.message.parser"
local inspect = require "inspect"

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
  
    --default headers
    net = "main",
    version_max = 5,
    version_min = 1,
    version_cur = 4,
    extensions = 0,
    type = "invalid",
  
    pack = function(self)
      local packed, err = parser.pack_message(self)
      if not packed then return nil, err end
      rawset(self, "_packed", packed)
      return packed
    end,
    
    typecode = function(self)
      return rawget(msg_types, rawget(self, "type"))
    end,
    
    block_typecode = function(self)
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
local Message = {}

function Message.new(msgtype, data)
  if not rawget(msg_types, msgtype) then
    error("unknown msgtype " .. tostring(msgtype))
  end
  local msg = setmetatable(data or {}, Message_metatable)
  
  rawset(msg, "type", msgtype)

  
  --default to mainnet
  rawset(msg, "net", "main")
  
  --shallow-copy the data
  if data then
    for k, v in pairs(data) do
      rawset(msg, k, v)
    end
  end
  return msg
end

function Message.unpack(str)
  local data, leftovers = parser.unpack_message(str)
  if not data then return nil, leftovers end
  print ("weelbl")
  print(inspect(data))
  print(type(data))
  if type(data) == "function" then
    print(debug.getinfo(data))
  else
    print(inspect(data))
  end
  
  return Message.new(data.type, data)
end

return Message

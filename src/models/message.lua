local Parser = require "prailude.util.parser"
local Server = require "prailude.server"

local msg_types = {
  invalid =       0,
  not_a_type =    1,
  keepalive =     2,
  publish =       3,
  confirm_req =   4,
  confirm_ack =   5,
  bulk_pull =     6,
  bulk_push =     7,
  frontier_req =  8,
  bulk_pull_blocks = 9,
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
      local packed, err = Parser.pack_message(self)
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
    end,

    send = function(self, peer)
      local ok, err = Server.send(self, peer)
      if ok then
        return self
      else
        return nil, err
      end
    end,
    
    broadcast = function(self, peers)
      local ok, err
      for _, peer in pairs(peers) do
        ok, err = self:send(peer)
        if not ok then return nil, err end
      end
      return self
    end
  }
}

-- the class-like
local Message = {}

function Message.new(msgtype, data) -- (data) is also ok, as long as there's a data.type message type value
  local new_msg_data
  if type(msgtype) == "table" then -- wrap around the passed-in data
    data = msgtype
    msgtype = data.type
    new_msg_data = data
  elseif data then
    new_msg_data = data
    new_msg_data.type = msgtype
  else
    new_msg_data = {
      type = msgtype
    }
  end
  
  if not rawget(msg_types, msgtype) then
    error("unknown msgtype " .. tostring(msgtype))
  end
  local msg = setmetatable(new_msg_data, Message_metatable)
  
  if msgtype == "frontier_req" or msgtype == "bulk_pull" or msgtype == "bulk_push" then
    rawset(msg, "protocol", "tcp")
  else
    rawset(msg, "protocol", "udp")
  end
  
  return msg
end

function Message.unpack(str, unpack_block)
  local data, leftovers = Parser.unpack_message(str, unpack_block or false)
  if not data then
    return nil, leftovers
  else
    return Message.new(data)
  end
end

return Message

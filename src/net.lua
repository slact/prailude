local uv = require "luv"

local Peer_instance = {
  send_udp = function(self, message)
    local msg = assert(message:pack())
    print(self.address, self.port)
    self.udp_client:send(message:pack(), self.address, self.port)
  end
}
local peer_meta = {__index=Peer_instance}


local Peer = {
  new = function(peer_addr, peer_port)
    if not peer_port then
      local m1, m2 = peer_addr:match("(.*[^:]):(%d+)$")
      if not m1 then
        peer_port = 7075
      else
        peer_addr, peer_port = m1, m2
      end
    end
    
    local peer = {
      address = peer_addr,
      port = peer_port,
      udp_client = uv.new_udp()
    }
    setmetatable(peer, peer_meta)
    return peer
  end
}

return {
  server =    require "prailude.net.server",
  bootstrap = require "prailude.net.bootstrap",
  peer = Peer
}

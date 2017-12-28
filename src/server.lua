local uv =  require "luv"
local Message = require "prailude.message"
local inspect = require "inspect"

--local log = require "prailude.log"
local server = {}
function server.start(port)
  
  port = 7075
  
  local tcp_server = uv.new_tcp()
  assert(tcp_server:bind("::", port))
  assert(tcp_server:listen(128 --[[connection backlog size]], function(err)
    assert(not err, err)
    local client = uv.new_tcp()
    assert(tcp:accept(client))
    client:read_start(function(err, chunk, etc)
      print("TCP", err, chunk, etc)
      -- Crash hard on errors
      if true then return end
      assert(not err, err)
      
      if chunk then
        local ret, err = parser.parse_tcp(client, chunk)
        if not ret then --parser told us something was wrong. close connection
          --log
          client:close()
        end
      else
        -- When the stream ends, close the socket
        client:close()
      end
    end)
    --do anything on TCP connection start?...
  end))
  
  servrt.tcp = tcp_server
  
  local udp = uv.new_udp()
  assert(udp:bind("::", port))
  udp:recv_start(function(err, chunk, addr)
    print((addr or {}).ip, (addr or {}).port, err, #(chunk or ""))
    if chunk == nil and not err then --EAGAIN or something
      return
    end
    local data, leftovers_or_err = Message.unpack(chunk)
    if not data then
      error(leftovers_or_err)
    end
  end)
  
  return self
end

return server

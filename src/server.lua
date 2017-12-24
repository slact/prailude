local uv =  require "luv"
local parser = require "prailude.parser"
--local log = require "prailude.log"
local server = {}
function server.new(port)
  
  port = 7075
  
  local tcp = uv.new_tcp()
  assert(tcp:bind("::", port))
  tcp:listen(128 --[[connection backlog size]], function(err)
    assert(not err, err)
    local client = uv.new_tcp()
    server:accept(client)
    client:read_start(function(err, chunk, etc)
      print("TCP", err, chunk, etc)
      -- Crash hard on errors
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
  end)
  
  local udp = uv.new_udp()
  assert(udp:bind("::", port))
  udp:recv_start(function(err, chunk, address, flags)
    print("UDP", err, chunk, address, flags)
    assert(not err, err)
    local ret, err = parser.parse_udp(address, chunk)
  end)
  
  return {
    tcp = tcp,
    udp = udp
  }
  
end

return server

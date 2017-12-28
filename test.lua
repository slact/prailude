local prailude = require "prailude"
local mm = require "mm"
local uv = require "luv"

--[[
local raw="aaa\0\0\0\0aaacaaaaaadaaaaaaaaaaqa12"
print(#raw)

for i, v in pairs(prailude.util) do
  print(i, v)
end
  

local addr = prailude.util.unpack_account(raw)
print(addr, #addr)
print(prailude.util.pack_account(addr))

print(prailude.util.to_hex("\1\2\3\4\5\0\xF3\xD9"))


print(prailude.util.vanitygen("hello boys"))
]]


local timer = uv.new_timer()
timer:start(1000, 0, function()
  local m = prailude.message.new("keepalive", {peers={}})
  prailude.util.print_hex(assert(m:pack()))

  local addr = uv.getaddrinfo("rai.raiblocks.net", nil, {family="inet6", socktype="dgram", protocol="packet"})
  mm(addr)

  local peer = prailude.net.peer.new(addr[math.random(1,#addr)].addr)

  mm(peer)
  print(#m:pack())
  mm(prailude)
  mm(prailude.running_server.udp:send(m:pack(), peer.address, peer.port))
end)


prailude.run()

local prailude = require "prailude"

local raw="aaa\0\0\0\0aaacaaaaaadaaaaaaaaaaqa12"
print(#raw)

local addr = prailude.util.unpack_account(raw)
print(addr, #addr)
print(prailude.util.pack_account(addr))

print(prailude.util.to_hex("\1\2\3\4\5\0\xF3\xD9"))

local loaded, prailude = pcall(require,"prailude")
if not loaded then
  error("'prailude' package is required for RaiBlocks packet dissassembly. Please install it using luarocks for " .. _VERSION)
end
local blake2b_hash = prailude.util.blake2b.hash
local unpack_balance = prailude.util.unpack_balance
local unpack_account = prailude.util.unpack_account
local stohex = prailude.util.bytes_to_hex
local Balance = require "prailude.util.balance"

local TCP_state = {}
local packet_state = {}
local packet_bulkpull_state = {}
local packet_bulkpull_count = {}
local packet_frontier_state = {}

local function packet_endpoints_str(pinfo, reverse)
  if not reverse then
    return ("%s:%i->%s:%i"):format(pinfo.src, pinfo.src_port, pinfo.dst, pinfo.dst_port)
  else
    return ("%s:%i->%s:%i"):format(pinfo.dst, pinfo.dst_port, pinfo.src, pinfo.src_port)
  end
end

local function raw_balance_to_xrb(raw)
  local balance = Balance.unpack(raw, "Mxrb")
  if balance then
    return ("%s XRB"):format(tostring(balance))
  else
    return err
  end
end

local function hash_hashable(str)
  return blake2b_hash(str, 32)
end

local rai_proto = Proto("raiblocks", "RaiBlocks", "RaiBlocks Network Protocol")

local block_proto = Proto("raiblocks.block", "Block", "RaiBlocks Block")

-- setup protocol fields.
rai_proto.fields = {}
local fds = rai_proto.fields
local msg_types = {
  [0]="invalid",
  [1]="not_a_type",
  [2]="keepalive",
  [3]="publish",
  [4]="confirm_req",
  [5]="confirm_ack",
  [6]="bulk_pull",
  [7]="bulk_push",
  [8]="frontier_req",

  [10000]="frontier",
  [10001]="bulk"
}
local block_types = {
  [0]="invalid",
  [1]="not_a_block",
  [2]="send",
  [3]="receive",
  [4]="open",
  [5]="change"
}

local header = {
  greeting = 
    ProtoField.new("Magic number",    "raiblocks.magic",            ftypes.STRING),
  version_max = 
    ProtoField.new("max version",     "raiblocks.version_max",      ftypes.UINT8, nil, base.DEC),
  version_current = 
    ProtoField.new("current version", "raiblocks.version_using",    ftypes.UINT8, nil, base.DEC),
  version_min = 
    ProtoField.new("min version",     "raiblocks.version_min",      ftypes.UINT8, nil, base.DEC),
  message_type = 
    ProtoField.new("message type",    "raiblocks.message_type",     ftypes.UINT8, msg_types, base.DEC),
  extensions= 
    ProtoField.new("extensions",      "raiblocks.extensions",       ftypes.UINT16, nil, base.HEX),
  
}
for k, f in pairs(header) do
  fds[k]=f
end

--keepalive
fds.keepalive_peer= ProtoField.new("peer", "raiblocks.keepalive.peer", ftypes.STRING)

--block
local bfds = {
  block_type = 
    ProtoField.new("type",                  "raiblocks.block.type",         ftypes.UINT8, block_types, base.DEC),
  block_raw = 
    ProtoField.new("raw",                   "raiblocks.block.raw",          ftypes.BYTES),
  block_previous = 
    ProtoField.new("previous",              "raiblocks.block.previous",     ftypes.STRING),
  block_hash = 
    ProtoField.new("hash",                  "raiblocks.block.hash",         ftypes.STRING),
  block_balance = 
    ProtoField.new("balance",               "raiblocks.block.balance",      ftypes.STRING),
  block_destination = 
    ProtoField.new("destination",           "raiblocks.block.destination",  ftypes.STRING),
  block_account = 
    ProtoField.new("account",               "raiblocks.block.account",      ftypes.STRING),
  block_source = 
    ProtoField.new("source block",          "raiblocks.block.source",       ftypes.STRING),
  block_representative = 
    ProtoField.new("representative",        "raiblocks.block.representative",ftypes.STRING),
  block_work = 
    ProtoField.new("work",                  "raiblocks.block.work",         ftypes.BYTES),
  block_signature = 
    ProtoField.new("signature",             "raiblocks.block.signature",    ftypes.BYTES)
}
block_proto.fields = bfds

--vote
fds.vote_account = 
    ProtoField.new("voting account",        "raiblocks.vote.account",       ftypes.STRING)
fds.vote_signature = 
    ProtoField.new("vote signature",        "raiblocks.vote.signature",     ftypes.BYTES)
fds.vote_sequence = 
    ProtoField.new("vote sequence",         "raiblocks.vote.sequence",      ftypes.UINT64)


--frontier_req
fds.frontier_req_start = 
    ProtoField.new("frontier start account","raiblocks.frontier_req.start", ftypes.STRING)
fds.frontier_req_age = 
    ProtoField.new("frontier age",          "raiblocks.frontier_req.age",   ftypes.UINT32)
fds.frontier_req_count = 
    ProtoField.new("frontier count",        "raiblocks.frontier_req.count", ftypes.UINT32)

--frontier
local frontier_proto = Proto("raiblocks.frontier", "Frontier", "RaiBlocks Frontier")

--block
local ffds = {
  account =
    ProtoField.new("frontier account (raw)",      "raiblocks.frontier.account",ftypes.STRING),
  block_hash =
    ProtoField.new("frontier block hash",   "raiblocks.frontier.block_hash",ftypes.STRING)
}
frontier_proto.fields = ffds

--bulk_pull
fds.bulk_pull_start_account =
    ProtoField.new("bulk pull start account",      "raiblocks.bulk_pull.account",   ftypes.STRING)
fds.bulk_pull_end_block_hash =
    ProtoField.new("bulk pull end  block hash",   "raiblocks.bulk_pull.block_hash",ftypes.STRING)


local block_size = {
  send =      152,
  receive =   136,
  open =      168,
  change =    136,
  invalid =   0,
  not_a_block=0
}

function message_block_dissector(buf, pinfo, root, btype_buf, block_info)
  local fds = bfds
  local blockt = block_types[btype_buf:uint()]
  local tree = root:add(block_proto, ("Block %s(%s)"):format(block_info or "", blockt))
  tree:add(fds.block_type,              btype_buf)
  if     blockt == "send" then
    tree:add(fds.block_raw,              buf(0, block_size.send))
    tree:add(fds.block_previous,         buf(0, 32), stohex(buf:raw(0, 32)))
    tree:add(fds.block_destination,      buf(32, 32), unpack_account(buf:raw(32, 32)))
    tree:add(fds.block_balance,          buf(64, 16), raw_balance_to_xrb(buf:raw(64, 16)))
    tree:add(fds.block_signature,        buf(80, 64))
    tree:add(fds.block_work,             buf(144, 8))
    tree:add(fds.block_hash,             stohex(hash_hashable(buf:raw(0, 80))))
  elseif blockt == "receive" then
    tree:add(fds.block_raw,              buf(0, block_size.receive))
    tree:add(fds.block_previous,         buf(0, 32), stohex(buf:raw(0, 32)))
    tree:add(fds.block_source,           buf(32, 32), stohex(buf:raw(32, 32)))
    tree:add(fds.block_signature,        buf(64, 64))
    tree:add(fds.block_work,             buf(128, 8))
    tree:add(fds.block_hash,             stohex(hash_hashable(buf:raw(0, 64))))
  elseif blockt == "open" then
    tree:add(fds.block_raw,              buf(0, block_size.open))
    tree:add(fds.block_source,           buf(0, 32), stohex(buf:raw(0, 32)))
    tree:add(fds.block_representative,   buf(32, 32), unpack_account(buf:raw(32, 32)))
    tree:add(fds.block_account,          buf(64, 32), unpack_account(buf:raw(64, 32)))
    tree:add(fds.block_signature,        buf(96, 64))
    tree:add(fds.block_work,             buf(160, 8))
    tree:add(fds.block_hash,             stohex(hash_hashable(buf:raw(0, 96))))
  elseif blockt == "change" then
    tree:add(fds.block_raw,              buf(0, block_size.change))
    tree:add(fds.block_previous,         buf(0, 32), stohex(buf:raw(0, 32)))
    tree:add(fds.block_representative,   buf(32, 32), unpack_account(buf:raw(32, 32)))
    tree:add(fds.block_signature,        buf(64, 64))
    tree:add(fds.block_work,             buf(128, 8))
    tree:add(fds.block_hash,             stohex(hash_hashable(buf:raw(0, 64))))
  end
end

-- packet dissector
function rai_proto.dissector(buf, pinfo, root)
  pinfo.cols.protocol = "raiblocks"
  local tree = root:add(rai_proto)
  
  local conn_id = packet_endpoints_str(pinfo)
  local tcp_state = TCP_state[conn_id]
  local len = buf:len()
  if tcp_state == "frontier" or packet_state[pinfo.number] == "frontier" then
    packet_state[pinfo.number] = "frontier"
    tree:add(header.message_type,     10000) --custom "frontier_req" response messat_type
    local len = buf:len()
    local entries = math.floor(len/64)
    local leftovers=math.fmod(len, 64)
    if leftovers > 0 then
      --need more data to have a clean break for the next packet
      pinfo.desegment_len = 64 - leftovers
      return nil
    end
    
    local frontier, acct
    for offset=0, 64*entries-1, 64 do
      acct = unpack_account(buf:raw(offset, 32))
      frontier = tree:add(frontier_proto, "Frontier ".. acct)
      frontier:add(ffds.account,    buf(offset, 32), stohex(buf:raw(offset, 32)))
      frontier:add(ffds.block_hash, buf(offset+32, 32), stohex(buf:raw(offset+32, 32)))
    end
    --print("frontier   entries:", entries, "leftovers:", leftovers)
  elseif tcp_state=="bulk_pull" or packet_state[pinfo.number] == "bulk_pull" then
    local bulkpull_count = bulkpull_start_count or 0
    packet_state[pinfo.number] = "bulk_pull"
    local more_blocks = true
    local offset = 0
    local block_len, block_type, btype_buf
    tree:add(header.message_type,     10001) --custom "bulk" response message_type
    while offset < len do
      btype_buf = buf(offset,1)
      block_type = block_types[btype_buf:uint()]
      offset = offset+1
      if not block_type then
        return nil --weird?..
      else
        if block_type == "not_a_block" then 
          TCP_state[conn_id]=nil
        end
        block_len = block_size[block_type]
        if not block_len then
          error("invalid block type")
        elseif len - offset < block_size[block_type] then
          local leftovers = block_size[block_type] - (len - offset)
          pinfo.desegment_len = block_size[block_type] - (len - offset)
          return nil
        else
          message_block_dissector(buf(offset):tvb(), pinfo, tree, btype_buf)
        end
        offset = offset + block_len
      end
    end
  else
    local magic
    if len >=2 then
      magic = buf(0, 2):string()
    end
    if magic == "RA" or magic == "RB" or magic == "RC" then
      local btype_buf = buf(7,1)
      tree:add(header.greeting,         buf(0, 2))
      tree:add(header.version_max,      buf(2, 1))
      tree:add(header.version_current,  buf(3, 1))
      tree:add(header.version_min,      buf(4, 1))
      tree:add(header.message_type,     buf(5, 1))
      tree:add(header.extensions,       buf(6, 1))
      local msgt = msg_types[buf(5,1):uint()]
      local blocktype = block_types[btype_buf:uint()]
      if     msgt == "keepalive" then
        local blen = len - 8;
        local ips = blen/18
        for i=0, blen-1, 18 do
          tree:add(fds.keepalive_peer,  buf(8+i,18), 
                  ("%s:%i"):format(tostring(buf(8+i,16):ipv6()), buf(8+i+16,2):le_uint()))
        end
      elseif msgt == "publish" then
        message_block_dissector(buf(8):tvb(), pinfo, tree, btype_buf)
      elseif msgt == "confirm_req" then
        message_block_dissector(buf(8):tvb(), pinfo, tree, btype_buf)
      elseif msgt == "confirm_ack" then
        tree:add(fds.vote_account,          buf(8, 32), unpack_account(buf:raw(8, 32)))
        tree:add(fds.vote_signature,        buf(40, 64))
        tree:add(fds.vote_sequence,         buf(104, 8), buf(104,8):le_uint64())
        message_block_dissector(buf(112):tvb(), pinfo, tree, btype_buf)
      elseif msgt == "bulk_pull" then
        tree:add(fds.bulk_pull_start_account,buf(8,32), unpack_account(buf:raw(8, 32)))
        tree:add(fds.bulk_pull_end_block_hash,buf(40,32), stohex(buf:raw(40, 32)))
        TCP_state[packet_endpoints_str(pinfo, "reverse")]="bulk_pull"
      elseif msgt == "frontier_req" then
        tree:add(fds.frontier_req_start,    buf(8, 32), unpack_account(buf:raw(8, 32)))
        tree:add(fds.frontier_req_age,      buf(40, 4))
        tree:add(fds.frontier_req_count,    buf(44, 4))
        TCP_state[packet_endpoints_str(pinfo, "reverse")]="frontier"
      end
    end
  end
end


DissectorTable.get("udp.port"):add(7075, rai_proto)
DissectorTable.get("tcp.port"):add(7075, rai_proto)

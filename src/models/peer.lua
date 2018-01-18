local bus = require "prailude.bus"
local mm = require "mm"
local log = require "prailude.log"
local server = require "prailude.server"
local uv = require "luv"
local gettime = require "prailude.util.lowlevel".gettime
local coroutine = require "prailude.util.coroutine"
local Timer = require "prailude.util.timer"

local NilDB = require "prailude.db.nil" -- no database

local Peer

local coroutine_status, coroutine_resume, coroutine_running, coroutine_yield = coroutine.status, coroutine.resume, coroutine.running, coroutine.yield

local known_peers = {}

local Peer_instance = {
  send = function(self, message)
    local ret, err = server.send(message, self)
    if ret then
      self:update_timestamp("keepalive_sent")
    end
    return ret, err
  end,
  
  open_tcp = function(self, heartbeat_interval, heartbeat_callback)
    local coro = coroutine_running()
    assert(coro, "open_tcp expects to be called in a coroutine")
    if self.tcp then
      return error("tcp connection already open for peer " .. tostring(self))
    end
    if #self.address==0 then
      return nil, "invalid empty address"
    end
    
    self.tcp = uv.new_tcp()
    self.tcp_coroutine = coro
    local function connect_callback(err)
      if err then
        return self:close_tcp(err)
      end
      local function read_start_callback(read_err, chunk)
        if coroutine_status(coro) == "suspended" then
          if chunk then assert(type(chunk) == "string") end
          coroutine_resume(coro, chunk, read_err)
        end
      end
      self.tcp:read_start(read_start_callback)
      if coroutine_status(coro) == "suspended" then
        return coroutine_resume(coro, self)
      else
        return self
      end
    end
    uv.tcp_connect(self.tcp, self.address, self.port, connect_callback)
    
    if type(heartbeat_interval) == "function" then
      assert(heartbeat_callback == nil, "heartbeat callback and interval arguments are flipped")
      --open_tcp called with heartbeat callback but no interval. i guess we'll allow it
      heartbeat_callback, heartbeat_interval = heartbeat_interval, 1000
    end
    
    if heartbeat_callback then
      assert(heartbeat_interval, "heartbeat_interval required if heartbeat_callback given")
      self.tcp_heartbeat_timer = Timer.interval(heartbeat_interval, function()
        local ok, err = heartbeat_callback()
        if ok == false or (not ok and err) then
          self:close_tcp(err)
        end
      end)
    end
    
    return coroutine_yield()
  end,
  
  send_tcp = function(self, message)
    if not self.tcp then
      error("no open tcp connection for peer %s", tostring(self))
    end
    if type(message) == "string" then --raw send
      return self.tcp:write(message)
    else
      return self.tcp:write(message:pack())
    end
  end,
  
  close_tcp = function(self, errmsg)
    
    if not self.tcp then
      -- nothing to do, tcp is already closed.
      -- it's ok to do this multiple times, makes close callbacks easier to write
      -- each one can idempotently call peer:close_tcp()
      return self
    end
    if self.tcp_heartbeat_timer then
      Timer.cancel(self.tcp_heartbeat_timer)
      self.tcp_heartbeat_timer = nil
    end
    local coro = self.tcp_coroutine
    self.tcp_coroutine = nil
    self.tcp:close()
    self.tcp = nil
    if coro and coroutine_status(coro) == "suspended" then
      coroutine_resume(coro, nil, errmsg)
    end
  end,
  
  read_tcp = function(self)
    local coro = coroutine_running()
    assert(coro, "read_tcp expects to be called in a coroutine")
    if coro ~= self.tcp_coroutine then
      error("called read_tcp from a different coroutine than open_tcp. This should be ok but it's weird, so it's not ok.")
    end
    return coroutine_yield()
  end,
  
  update_timestamp = function(self, what)
    local field = "last_"..what
    local prev_val = rawget(self, field)
    rawset(self, field, gettime())
    Peer.update_timestamp_field(self, field, prev_val)
    return self
  end
}
local peer_meta = {
  __index=Peer_instance,
  __tostring=function(t)
    return t.id
  end
}

local function new_peer(peer_addr, peer_port)
  local peer
  if type(peer_addr) == "table" and peer_port == nil then
    peer = peer_addr
    assert(peer.address, "peer address is required")
    assert(peer.port, "peer port is required")
    peer.id = ("%s:%d"):format(peer.address, peer.port)
  else
    if not peer_port then
      local m1, m2 = peer_addr:match("(.*[^:]):(%d+)$")
      if not m1 then
        peer_port = 7075
      else
        peer_addr, peer_port = m1, m2
      end
    end
    peer = {
      address = peer_addr,
      port = peer_port,
      id = ("%s:%.0f"):format(peer_addr, peer_port)
    }
  end
  
  setmetatable(peer, peer_meta)
  return peer
end

local function ensure_ipv6_if_ipv4(peer_addr)
  local addr = { peer_addr:match("^(%d%d?%d?)%.(%d%d?%d?)%.(%d%d?%d?)%.(%d%d?%d?)") }
  if #addr == 0 then
    return peer_addr
  else
    return ("::ffff:%i.%i.%i.%i"):format(addr[1], addr[2], addr[3], addr[4])
  end
end

Peer = {
  new = new_peer,
  
  --find existing peer or make a new one
  get = function(peer_addr, peer_port)
    local id, not_recently_seen
    if type(peer_addr) == "table" then
      peer_port = peer_addr.port
      peer_addr = peer_addr.address
      id = ("%s:%.0f"):format(peer_addr, peer_port)
    elseif peer_addr and not peer_port then --maybe we were passed the peer id (addr:port)
      id = peer_addr
      peer_addr, peer_port = id:match("^(.*[^:]):(%d+)$")
    else
      peer_addr = ensure_ipv6_if_ipv4(peer_addr)
      id = ("%s:%.0f"):format(peer_addr, peer_port)
    end
    local peer = rawget(known_peers, id)
    if not peer then
      peer = Peer.find(peer_addr, peer_port)
    end
    if not peer then
      not_recently_seen = true
      peer = new_peer(peer_addr, peer_port)
      Peer.store(peer)
    end
    rawset(known_peers, id, peer)
    return peer, not_recently_seen
  end,
  
  inactivity_timeout = 60*7,
  keepalive_interval = 60*2,
  
  ------------
  -- database stuff is in db/[db_type]/peerdb.lua
  ------------
}

return setmetatable(Peer, NilDB.peer)

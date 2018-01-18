local bus = require "prailude.bus"
local mm = require "mm"
local logger = require "prailude.log"
local server = require "prailude.server"
local uv = require "luv"
local gettime = require "prailude.util.lowlevel".gettime
local coroutine = require "prailude.util.coroutine"
local Parser = require "prailude.message.parser"
local Timer = require "prailude.util.timer"

local NilDB = require "prailude.db.nil" -- no database

local Peer
local function is_ok(err)
  if err then
    return nil
  else
    return true
  end
end

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
  
  open_tcp = function(self, callback)
    if self.tcp then
      return error("tcp already open for peer " .. tostring(self))
    end
    self.tcp = uv.new_tcp()
    local coro_wrap
    if not callback then
      coro_wrap = coroutine.late_wrap()
      callback = coro_wrap
    end
    assert(callback, "no callback or coroutine given")
    local actual_callback = callback
    callback = function(err)
      if err then self:close_tcp() end
      return actual_callback(is_ok(err), err)
    end
    uv.tcp_connect(self.tcp, self.address, self.port, callback)
    if coro_wrap then
      return coroutine.yield()
    end
  end,
  
  send_tcp = function(self, message)
    if not self.tcp then
      error("no open tcp connection for peer %s", tostring(self))
    end
    return self.tcp:write(message:pack())
  end,
  
  read_frontiers = function(self, min_frontiers_per_sec, callback)
    min_frontiers_per_sec = min_frontiers_per_sec or 100
    assert(self.tcp, "expected an open tcp connection, instead there was an abyss of packets lost to time")
    
    local coro_wrap
    if not callback then
      coro_wrap = coroutine.late_wrap()
      callback = coro_wrap
    end
    assert(callback, "no callback or coroutine given")
    
    local frontiers_so_far = {}
    
    do --rate checking timer
      local last_frontiers_count = 0
      self.tcp_timer = Timer.interval(5000, function() --rate check
        if true or #frontiers_so_far - last_frontiers_count < (min_frontiers_per_sec * 5) then
          callback(nil, "frontier pull rate timeout")
          return --timer stopped by callback()
        else
          last_frontiers_count = #frontiers_so_far
        end
      end)
    end
    
    local buf = {}
    self.tcp:read_start(function(err, chunk)
      if err then
        logger:warning("tcp read error for peer %s: %s", self, err)
        self.tcp:read_stop()
        self:close_tcp()
        return callback(nil, err)
      end
      
      table.insert(buf, chunk)
      local fresh_frontiers, leftover_buf_or_err, done, progress = Parser.unpack_frontiers(table.concat(buf))
      
      if not fresh_frontiers then -- there was an error
        logger:warning("error getting frontiers from peer %s: %s", self, leftover_buf_or_err)
        self.tcp:read_stop()
        self:close_tcp()
        return callback(nil, leftover_buf_or_err)
      elseif not done then
        logger:debug("bootstrap: got %5d frontiers (%7d total) [%4.3f%%] from %s", #fresh_frontiers, #frontiers_so_far, (progress or 0) * 100, self)
        --got some new frontiers
        for _, frontier in pairs(fresh_frontiers) do
          table.insert(frontiers_so_far, frontier)
        end
        if leftover_buf_or_err and #leftover_buf_or_err > 0 then
          buf = {leftover_buf_or_err}
        end
      else
      logger:debug("finished getting frontiers (%7d total) from %s", #frontiers_so_far, self)
        -- no more frontiers here
        self.tcp:read_stop()
        Timer.cancel(self.tcp_timer)
        self.tcp_timer = nil
        return callback(frontiers_so_far)
      end
    end)
    if coro_wrap then
      return coroutine.yield()
    end
  end,
  
  close_tcp = function(self)
    if not self.tcp then
      error("trying to close unopened TCP connection to " .. tostring(self))
    end
    self.tcp:close()
    self.tcp = nil
    if self.tcp_timer then
      Timer.cancel(self.tcp_timer)
      self.tcp_timer = nil
    end
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

local bus = require "prailude.bus"
local mm = require "mm"
local log = require "prailude.log"
local server = require "prailude.server"
local uv = require "luv"
local gettime = require "prailude.util.lowlevel".gettime
local coroutine = require "prailude.util.coroutine"
local Parser = require "prailude.message.parser"

local Peer

local function is_ok(err)
  if err then
    return nil
  else
    return true
  end
end

local schema = [[
  CREATE TABLE IF NOT EXISTS peers (
    address                  TEXT,
    port                     INTEGER,
    last_received            REAL,
    last_sent                REAL,
    last_keepalive_sent      REAL,
    last_keepalive_received  REAL,
    PRIMARY KEY(address, port)
  ) WITHOUT ROWID;
  CREATE INDEX IF NOT EXISTS peer_last_received_idx           ON peers (last_received);
  CREATE INDEX IF NOT EXISTS peer_last_sent_idx               ON peers (last_sent);
  CREATE INDEX IF NOT EXISTS peer_last_keepalive_sent_idx     ON peers (last_keepalive_sent);
  CREATE INDEX IF NOT EXISTS peer_last_keepalive_received_idx ON peers (last_keepalive_received);
]]

local known_peers = {}
local db

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
  
  read_frontiers = function(self, callback)
    assert(self.tcp, "expected an open tcp connection, instead there was an abyss of packets lost to time")
    local coro_wrap
    if not callback then
      coro_wrap = coroutine.late_wrap()
      callback = coro_wrap
    end
    assert(callback, "no callback or coroutine given")
    
    local frontiers_so_far = {}
    local buf = {}
    self.tcp:read_start(function(err, chunk)
      if err then
        self.tcp:read_stop()
        self:close_tcp()
        return callback(nil, err)
      end
      
      table.insert(buf, chunk)
      local fresh_frontiers, leftover_buf_or_err, done = Parser.parse_frontier(table.concat(buf))
      
      if not fresh_frontiers then -- there was an error
        self.tcp:read_stop()
        self:close_tcp()
        return callback(nil, leftover_buf_or_err)
      elseif not done then
        --got some new frontiers
        for _, frontier in pairs(fresh_frontiers) do
          table.insert(frontiers_so_far, frontier)
        end
        if leftover_buf_or_err and #leftover_buf_or_err > 0 then
          buf = {leftover_buf_or_err}
        end
      else
        -- no more frontiers here
        self.tcp:read_stop()
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
  end,
  
  update_timestamp = function(self, what)
    local field = "last_"..what
    local current_val = self[field]
    local now = gettime()
    if not current_val or current_val < now - 60 then --every minute update
      local query = ("UPDATE peers SET %s=%f WHERE address=\"%s\" AND port=%f"):format(
        field,
        now,
        self.address,
        self.port
      )
      db:exec(query)
      self[field] = now
    end
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
      local select = ("SELECT * FROM peers WHERE address=\"%s\" AND port=%d LIMIT 1"):format(peer_addr, peer_port)
      for row in db:nrows(select) do
        peer = new_peer(row)
      end
    end
    if not peer then
      not_recently_seen = true
      peer = new_peer(peer_addr, peer_port)
      db:exec(("INSERT OR IGNORE INTO peers (address, port) VALUES( \"%s\", %i)"):format(peer_addr, peer_port))
    end
    rawset(known_peers, id, peer)
    return peer, not_recently_seen
  end,
  
  get8 = function(except_peer)
    local select = ("SELECT * FROM peers WHERE last_keepalive_received > datetime('now') - 120 AND address !=\"%s\" AND port != %d ORDER BY RANDOM() LIMIT 8"):format(except_peer.address, except_peer.port)
    local peers = {}
    for row in db:nrows(select) do
      table.insert(peers, new_peer(row))
    end
    return peers
  end,
  
  get_active_needing_keepalive = function()
    local now = gettime()
    local select = ("SELECT * FROM peers WHERE last_keepalive_received > %i AND last_keepalive_received < %i ORDER BY RANDOM()"):format(now - Peer.inactivity_timeout, now - Peer.keepalive_interval)
    local peers = {}
    for row in db:nrows(select) do
      table.insert(peers, new_peer(row))
    end
    return peers
  end,
  
  get_fastest_ping = function(n)
    n = n or 1
    local select = "SELECT *, (last_keepalive_received - last_keepalive_sent) AS ping FROM peers WHERE last_keepalive_received NOT NULL AND ping >= 0 ORDER BY ping ASC limit " .. n
    local peers = {}
    for row in db:nrows(select) do
      table.insert(peers, new_peer(row))
    end
    return peers
  end,
  
  initialize = function(db_ref)
    db = db_ref
    db:exec(schema)
  end,
  
  inactivity_timeout = 60*7,
  keepalive_interval = 60*2,
  
}

return Peer

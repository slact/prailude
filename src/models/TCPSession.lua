local coroutine = require "prailude.util.coroutine"
local Timer = require "prailude.util.timer"
local uv = require "luv"
local mm = require "mm"

local ERR = {
  ENOSTR =       "Device not a stream", --60
  ENODATA =      "No data available", --61
  ENOSR =        "Out of streams resources", --63
  ENONET =       "Machine is not on the network", --64
  ENOLINK =      "Link has been severed", --67
  ECOMM =        "Communication error on send", --70
  EPROTO =       "Protocol error", --71
  EBADMSG =      "Not a data message", --74
  EBADFD =       "File descriptor in bad state", --77
  EREMCHG =      "Remote address changed", --78
  ENOTSOCK =     "Socket operation on non-socket", --88
  EDESTADDRREQ = "Destination address required", --89
  EMSGSIZE =     "Message too long", --90
  EPROTOTYPE =   "Protocol wrong type for socket", --91
  ENOPROTOOPT =  "Protocol not available", --92
  EPROTONOSUPPORT="Protocol not supported", --93
  ESOCKTNOSUPPORT="Socket type not supported", --94
  EOPNOTSUPP =   "Operation not supported on transport endpoint", --95
  EPFNOSUPPORT = "Protocol family not supported", --96
  EAFNOSUPPORT = "Address family not supported by protocol", --97
  EADDRINUSE =   "Address already in use", --98
  EADDRNOTAVAIL ="Cannot assign requested address", --99
  ENETDOWN =     "Network is down", --100
  ENETUNREACH =  "Network is unreachable", --101
  ENETRESET =    "Network dropped connection because of reset", --102
  ECONNABORTED = "Software caused connection abort", --103
  ECONNRESET =   "Connection reset by peer", --104
  ENOBUFS =      "No buffer space available", --105
  EISCONN =      "Transport endpoint is already connected", --106
  ENOTCONN =     "Transport endpoint is not connected", --107
  ESHUTDOWN =    "Cannot send after transport endpoint shutdown", --108
  ETIMEDOUT =    "Connection timed out", --110
  ECONNREFUSED = "Connection refused", --111
  EHOSTDOWN =    "Host is down", --112
  EHOSTUNREACH = "No route to host", --113
  EREMOTEIO =    "Remote I/O error", --121
  EDQUOT =       "Quota exceeded", --122
}
setmetatable(ERR, {__index = function(_, k) return k end})

local coroutine_status, coroutine_resume, coroutine_running, coroutine_yield, coroutine_create = coroutine.status, coroutine.resume, coroutine.running, coroutine.yield, coroutine.create

local function Buffer()
  local buf = {}
  local function flush()
    if #buf == 1 then
      return table.remove(buf)
    else
      local out = table.concat(buf)
      buf = {}
      return out
    end
  end
  local function push(self, chunk)
    if type(self) == "string" and not chunk then --whatever
      chunk, self = self, nil
    end
    table.insert(buf, chunk)
    return self or true
  end
  local function clear(self)
    if #buf > 0 then
      buf = {}
    end
    return self or true
  end
  
  return setmetatable({
    flush = flush,
    clear = clear,
    push = push,
  }, {__call = flush})
end

local TCPSession_meta = {
  __index = {
    resume_session = (function()
      local function session_coroutine_result(tcp, ok, ...)
        if not ok then
          return tcp:stop(nil, ({...})[1])
        end
        local status = coroutine_status(tcp.session)
        if status == "dead" then
          return tcp:stop(true, ...)
        end
        return ok, ...
      end
      
      return function(self, ...)
        local session = self.session
        local status = coroutine_status(session)
        if status == "suspended" then
          return session_coroutine_result(self, coroutine_resume(session, ...))
        elseif status == "running" then
          error("can't call resume_session from inside the session")
        elseif status == "normal" then
          error("don't yield to another coroutine from inside the session")
        elseif status == "dead" then
          return self:stop("session coroutine is dead")
        end
      end
    end)(),
    
    run = function(self, name, session, watchdog)
      if self.session then
        error("TCP session " .. self.name .. "already running")
      end
      
      if #self.peer.address == 0 then
        return nil, "invalid empty-address peer"
      end
      local coro = coroutine_running()
      assert(coro, "must be in a coroutine")
      assert(type(name) == "string", "TCP session needs a stringy name")
      self.error = nil
      self.coro = coro
      self.name = name
      local session_coro = coroutine_create(session)
      self.session = session_coro
      self.session_write_callback = function(err)
        local current_session_coro = self.session
        if current_session_coro then
          if current_session_coro ~= session_coro then
            --this is a write callback for a previous session
            return nil, "previous session"
          elseif coroutine_status(current_session_coro) == "suspended" then
            return coroutine_resume(current_session_coro, not err, err)
          else
            return not err, err
          end
        end
      end
      
      self.buf:clear()
      
      local ok, err
      
      if self.idle_timer then
        Timer.cancel(self.idle_timer)
        self.idle_timer = nil
      end
      if watchdog then
        assert(type(watchdog) == "function", "watchdog must be a function")
        self.watchdog_timer = Timer.interval(self.watchdog_interval or 1000, function()
          local watchdog_ok, watchdog_err = watchdog()
          if watchdog_ok == false or (not watchdog_ok and watchdog_err) then
            return self:stop(nil, watchdog_err)
          end
        end)
      end
      
      if not self.tcp then
        self.tcp = uv.new_tcp()
      end
      if not self.tcp:is_readable() or not self.tcp:is_writable() then --is not connected
        --let's connect
        self.tcp:connect(self.peer.address, self.peer.port, function(connect_err)
          --this function could be called _after_ the watchdog terminates the session. care must be taken
          if connect_err then
            connect_err = ERR[connect_err]
          end
          if self.coro == coro and self.session == session_coro then -- same session
            if connect_err then
              return coroutine_resume(coro, nil, connect_err)
            else
              return coroutine_resume(coro, self)
            end
          else
            return "omae wa mou shindeiru"
          end
        end)
        ok, err = coroutine_yield()
        if not ok then -- connection failed
          return self:stop(nil, err)
        end
      end
      
      self.tcp:read_start(function(read_err, chunk)
        if read_err then
          return self:stop(nil, read_err)
        end
        self.buf:push(chunk)
        if self.resume_coro_on_read then
          rawset(self, "resume_session_on_read", nil)
          return self:resume_session(chunk)
        end
      end)
      
      local ret = {coroutine_resume(self.session, self)} --start it up
      if coroutine_status(self.session) == "dead" then -- done already? alright...
        return self:stop(table.unpack(ret))
      end
      
      return coroutine_yield()
    end,
    
    stop = function(self, ok, arg, ...)
      if not ok then
        arg = ERR[arg]
      end
      self.buf:clear()
      self.resume_coro_on_read = nil
      self.resume_coro_on_write = nil
      if self.session then
        self.session = nil
      end
      if self.session_write_callback then
        self.session_write_callback = nil
      end
      self.idle_timer = Timer.delay(self.idle_ttl, function()
        if not self.session then
          if self.tcp then
            self.tcp:close()
            self.tcp = nil
          end
          --remove tcp session ref from peer
          if self.peer then
            self.peer.tcp = nil
            self.peer = nil
          end
        end
      end)
      if self.watchdog_timer then
        Timer.cancel(self.watchdog_timer)
        self.watchdog_timer = nil
      end
      if not ok then
        self.error = arg or "unspecified error"
      end
      
      local coro = self.coro
      self.coro = nil
      
      if self.tcp then
        self.tcp:close()
        self.tcp = nil
      end
      
      if coro and coroutine_status(coro) == "suspended" then
        if not ok then
          return coroutine_resume(coro, nil, arg)
        else
          return coroutine_resume(coro, arg, ...)
        end
      else
        if not ok then
          return nil, arg
        else
          return arg, ...
        end
      end
    end,
    
    read = function(self)
      assert(coroutine_running() == self.session, "session coroutine not running!")
      rawset(self, "resume_coro_on_read", true)
      return coroutine_yield()
    end,
    
    write = function(self, data)
      local write_callback = self.session_write_callback
      if not data or #data == 0 then
        error("can't write empty data")
      end
      self.tcp:write(data, write_callback)
      if write_callback then
        --resume when write finishes
        return coroutine_yield()
      else
        --no callback to tcp:write(), just keep going
        return true
      end
    end
  }
}

local TCPSession = {
  get = function(peer, opt)
    if peer.tcp then
      return peer.tcp
    end
    opt = opt or {}
    local sesh = setmetatable ({
      peer = peer,
      buf = Buffer(),
      idle_ttl = opt.idle_ttl or 2500 --idle for 2.5sec after a session, then quit
    }, TCPSession_meta)
    
    sesh:stop()
    peer.tcp = sesh
    return sesh
  end
}

return TCPSession


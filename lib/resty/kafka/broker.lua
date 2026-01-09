-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local request = require "resty.kafka.request"

local to_int32 = response.to_int32
local setmetatable = setmetatable
local tcp = ngx.socket.tcp
local pid = ngx.worker.pid
local tostring = tostring

local sasl = require "resty.kafka.sasl"

local _M = {}
local mt = { __index = _M }


-- 发送请求，并接收响应
local function _sock_send_recieve(sock, request)
    -- https://github.com/openresty/lua-nginx-module?tab=readme-ov-file#tcpsocksend
    -- The input argument data can either be a Lua string or a (nested) Lua table holding string fragments
    -- In case of table arguments, this method will copy all the string elements piece by piece to the underlying Nginx socket send buffers,
    -- which is usually optimal than doing string concatenation operations on the Lua land
    local bytes, err = sock:send(request:package())
    if not bytes then
        return nil, err, true
    end

    -- 接收响应
    -- 首先读取响应长度
    local len, err = sock:receive(4)
    if not len then
        if err == "timeout" then
            sock:close()
            return nil, err
        end
        return nil, err, true
    end

    -- 接收所有响应
    local data, err = sock:receive(to_int32(len))
    if not data then
        if err == "timeout" then
            sock:close()
            return nil, err
        end
        return nil, err, true
    end

    -- 构建响应体
    return response:new(data, request.api_version), nil, true
end


local function _sasl_handshake(sock, brk)
    local cli_id = "worker" .. pid()
    local req = request:new(request.SaslHandshakeRequest, 0, cli_id,
                            request.API_VERSION_V1)

    req:string(brk.auth.mechanism)

    local resp, err = _sock_send_recieve(sock, req, brk.config)
    if not resp  then
        return nil, err
    end

    local err_code = resp:int16()
    if err_code ~= 0 then
        local error_msg = resp:string()

        return nil, error_msg
    end

    return true
end


local function _sasl_auth(sock, brk)
    local cli_id = "worker" .. pid()
    local req = request:new(request.SaslAuthenticateRequest, 0, cli_id,
                            request.API_VERSION_V1)

    local ok, msg = sasl.encode(brk.auth.mechanism, nil, brk.auth.user,
                            brk.auth.password, sock)
    if not ok then
        return nil, msg
    end
    req:bytes(msg)

    local resp, err = _sock_send_recieve(sock, req, brk.config)
    if not resp  then
        return nil, err
    end

    local err_code = resp:int16()
    local error_msg = resp:string()
    local auth_bytes = resp:bytes()

    if err_code ~= 0 then
        return nil, error_msg
    end

    return true
end


local function sasl_auth(sock, broker)
    local ok, err = _sasl_handshake(sock, broker)
    if  not ok then
        return nil, err
    end

    local ok, err = _sasl_auth(sock, broker)
    if not ok then
        return nil, err
    end

    return true
end


-- 只是设置元表
function _M.new(self, host, port, socket_config, sasl_config)
    return setmetatable({
        host = host,
        port = port,
        config = socket_config,
        auth = sasl_config,
    }, mt)
end


-- producer.lua:_send -> .
-- 发送请求并接收响应,
function _M.send_receive(self, request)
    -- ngx.socket.tcp
    local sock, err = tcp()
    if not sock then
        return nil, err, true
    end

    -- 设置超时时间
    sock:settimeout(self.config.socket_timeout)

    -- 建立连接 https://github.com/openresty/lua-nginx-module?tab=readme-ov-file#tcpsockconnect
    -- Before actually resolving the host name and connecting to the remote backend,
    -- this method will always look up the connection pool for matched idle connections created by previous calls of this method (or the ngx.socket.connect function).
    local ok, err = sock:connect(self.host, self.port)
    if not ok then
        return nil, err, true
    end

    local times, err = sock:getreusedtimes()
    if not times then
        return nil, "failed to get reused time: " .. tostring(err), true
    end

    -- 如果是新建立的连接，且是ssl协议，则进行ssl握手
    if self.config.ssl and times == 0 then
        -- first connectted connnection
        -- 执行ssl握手
        local ok, err = sock:sslhandshake(false, self.host,
                                          self.config.ssl_verify)
        if not ok then
            return nil, "failed to do SSL handshake with "
                        ..  self.host .. ":" .. tostring(self.port) .. ": "
                        .. err, true
        end
    end

    -- 如果是新建立的连接，且需要认证，执行auth认证
    if self.auth and times == 0 then -- SASL AUTH
        local ok, err = sasl_auth(sock, self)
        if  not ok then
            return nil, "failed to do " .. self.auth.mechanism .." auth with "
                        ..  self.host .. ":" .. tostring(self.port) .. ": "
                        .. err, true

        end
    end

    -- 发送请求并接收响应。当是网络错误时，retryable为false
    local data, err, retryable = _sock_send_recieve(sock, request)

    -- 回收连接
    sock:setkeepalive(self.config.keepalive_timeout, self.config.keepalive_size)

    return data, err, retryable
end


return _M

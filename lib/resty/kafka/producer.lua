-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local request = require "resty.kafka.request"
local broker = require "resty.kafka.broker"
local client = require "resty.kafka.client"
local Errors = require "resty.kafka.errors"
local sendbuffer = require "resty.kafka.sendbuffer"
local ringbuffer = require "resty.kafka.ringbuffer"


local setmetatable = setmetatable
local timer_at = ngx.timer.at
local timer_every = ngx.timer.every
local is_exiting = ngx.worker.exiting
local ngx_sleep = ngx.sleep
local ngx_log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO
local DEBUG = ngx.DEBUG
local debug = ngx.config.debug
local crc32 = ngx.crc32_short
local pcall = pcall
local pairs = pairs

local API_VERSION_V0 = 0
local API_VERSION_V1 = 1
local API_VERSION_V2 = 2

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = { _VERSION = "0.20" }
local mt = { __index = _M }


-- weak value table is useless here, cause _timer_flush always ref p
-- so, weak value table won't works
-- 支持多个集群，key为集群名称，value为produce实例
local cluster_inited = {}
local DEFAULT_CLUSTER_NAME = 1


-- 默认的消息分区选择器
-- num为topic对于的分区数量，key为消息的分区键
local function default_partitioner(key, num, correlation_id)
    local id = key and crc32(key) or correlation_id

    -- partition_id is continuous and start from 0
    return id % num
end


local function correlation_id(self)
    local id = (self.correlation_id + 1) % 1073741824 -- 2^30
    self.correlation_id = id

    return id
end


-- 组件发送消息请求
local function produce_encode(self, topic_partitions)
    local req = request:new(request.ProduceRequest,
                            correlation_id(self), self.client.client_id, self.api_version)

    --- https://kafka.apache.org/41/design/protocol/#The_Messages_Produce
    -- required_acks 这个值表示服务端收到多少确认后才发送反馈消息给客户端。
    -- 如果设置为0，那么服务端将不发送response（这是唯一的服务端不发送response的情况）。
    -- 如果这个值为1，那么服务器将等到数据写入到本地日之后发送response。（默认为1）
    -- 如果这个值是-1，那么服务端将阻塞，直到这个消息被所有的同步副本写入后再发送response。
    req:int16(self.required_acks)
    -- 以毫秒为单位的超时时间
    req:int32(self.request_timeout)
    -- topic个数
    req:int32(topic_partitions.topic_num)

    for topic, partitions in pairs(topic_partitions.topics) do
        -- topic
        req:string(topic)
        -- 分区个数
        req:int32(partitions.partition_num)

        for partition_id, buffer in pairs(partitions.partitions) do
            -- 分区id
            req:int32(partition_id)

            -- MessageSetSize and MessageSet
            req:message_set(buffer.queue, buffer.index)
        end
    end

    return req
end


-- 解析发送topic后的响应
-- 解析结果 ret[topic][partition] = {errcode, offset, timestamp}
local function produce_decode(resp)
    -- https://kafka.apache.org/41/design/protocol/#The_Messages_Produce
    -- 先读取涉及的topic个数
    local topic_num = resp:int32()
    local ret = new_tab(0, topic_num)
    local api_version = resp.api_version

    -- 遍历
    for i = 1, topic_num do
        local topic = resp:string()
        local partition_num = resp:int32()

        ret[topic] = {}

        -- ignore ThrottleTime
        for j = 1, partition_num do
            local partition = resp:int32()

            if api_version == API_VERSION_V0 or api_version == API_VERSION_V1 then
                ret[topic][partition] = {
                    errcode = resp:int16(),
                    offset = resp:int64(),
                }

            elseif api_version == API_VERSION_V2 then
                ret[topic][partition] = {
                    errcode = resp:int16(),
                    offset = resp:int64(),
                    timestamp = resp:int64(), -- If CreateTime is used, this field is always -1
                }
            end
        end
    end

    -- 解析结果 ret[topic][partition] = {errcode, offset, timestamp}
    return ret
end


-- 选择分区
local function choose_partition(self, topic, key)
    local brokers, partitions = self.client:fetch_metadata(topic)
    if not brokers then
        return nil, partitions
    end

    -- 调用用户传递的或默认的 default_partitioner
    return self.partitioner(key, partitions.num, self.correlation_id)
end


-- 用于控制并发flush，返回false表示有其他协程正在执行flush. 其实就是self.flushing字段
local function _flush_lock(self)
    if not self.flushing then
        if debug then
            ngx_log(DEBUG, "flush lock accquired")
        end
        self.flushing = true
        return true
    end
    return false
end


-- 释放flush锁, self.flushing = false
local function _flush_unlock(self)
    if debug then
        ngx_log(DEBUG, "flush lock released")
    end
    self.flushing = false
end


-- 向单个broker发送消息。broker_conf 表示一个broker, topic_partitions表示要发往这个broker的消息
local function _send(self, broker_conf, topic_partitions)
    local sendbuffer = self.sendbuffer
    local resp, retryable = nil, true
    -- 创建broker, new里只是setmetatable
    local bk, err = broker:new(broker_conf.host, broker_conf.port, self.socket_config, broker_conf.sasl_config)
    if bk then
        local req = produce_encode(self, topic_partitions)

        -- 发送请求并接收响应
        resp, err, retryable = bk:send_receive(req)
        if resp then
            -- 解析发送结果 ret[topic][partition] = {errcode, offset, timestamp}
            local result = produce_decode(resp)

            for topic, partitions in pairs(result) do
                for partition_id, r in pairs(partitions) do
                    local errcode = r.errcode

                    -- 成功
                    if errcode == 0 then
                        -- 设置sendbuffer对应的offset
                        sendbuffer:offset(topic, partition_id, r.offset)
                        -- 重置self.topics[topic][partition_id]对于的buffer相关属性,清空buffer
                        sendbuffer:clear(topic, partition_id)
                    else
                        -- 有错误
                        -- 获取错误码对应的错误消息
                        err = Errors[errcode] or Errors[-1]

                        -- set retries according to the error list
                        local retryable0 = retryable or err.retriable

                        -- 设置对应topic-partition_id上queue的错误消息
                        local index = sendbuffer:err(topic, partition_id, err.msg, retryable0)

                        -- length是消息的个数
                        ngx_log(INFO, "retry to send messages to kafka err: ", err.msg, "(", errcode, "), retryable: ",
                            retryable0, ", topic: ", topic, ", partition_id: ", partition_id, ", length: ", index / 2)
                    end
                end
            end

            return
        end
    end

    --- broker:new 失败, retryable为true
    -- when broker new failed or send_receive failed
    for topic, partitions in pairs(topic_partitions.topics) do
        for partition_id, partition in pairs(partitions.partitions) do
            sendbuffer:err(topic, partition_id, err, retryable)
        end
    end
end


-- 真正开始向kafka批量发送消息了。
-- 返回true表示所有的消息都已经发送完。
-- 返回false表示超过重试次数后，仍然没有发送完（存在错误）
local function _batch_send(self, sendbuffer)
    local try_num = 1
    while try_num <= self.max_retry do
        -- aggregator。sendbroker为以broker为key对消息进行分组。send_num为sendbroker数组大小
        local send_num, sendbroker = sendbuffer:aggregator(self.client)
        -- 已经没有消息了
        if send_num == 0 then
            break
        end

        -- 遍历每个broker, 向所有broker发送消息
        for i = 1, send_num, 2 do
            local broker_conf, topic_partitions = sendbroker[i], sendbroker[i + 1]

            -- 向单个broker发送消息
            _send(self, broker_conf, topic_partitions)
        end

        -- done()返回true说明sendbuffer中所有topic下的所有partition中的消息都发完了
        if sendbuffer:done() then
            return true
        end

        --- 此处说明没有发送成功
        -- 重新获取元数据信息
        self.client:refresh()

        -- 重试策略
        try_num = try_num + 1
        if try_num < self.max_retry then
            ngx_sleep(self.retry_backoff / 1000)   -- ms to s
        end
    end
end


local _flush_buffer


-- flush要发送的消息。两种触发调用路径：1.定时调用(默认每秒调用一次);2.ringbuffer中的消息数量已经超过了batch_num了
local function _flush(premature, self)
    -- 并发控制，_flush_lock返回false表示有其他协程正在执行
    if not _flush_lock(self) then
        if debug then
            ngx_log(DEBUG, "previous flush not finished")
        end
        return
    end

    local ringbuffer = self.ringbuffer
    local sendbuffer = self.sendbuffer
    --- 每次循环遍历从ringbuffer中取出一条消息，计算消息所属分区，根据分区将其放入到sendbuffer中再批量发送
    while true do
        -- 从ringbuffer中取出一条消息
        local topic, key, msg = ringbuffer:pop()
        -- ringbuffer中所有消息已经取完了
        if not topic then
            break
        end

        -- 选择消息所属分区
        local partition_id, err = choose_partition(self, topic, key)
        if not partition_id then
            partition_id = -1
        end

        -- 加入到sendbuffer。 sendbuffer以 topic/partition_id 进行分组组织所有的message
        -- 返回overflow为true表示partition_id所在的分区消息长度超过了batch_size或消息个数超过了batch_num
        local overflow = sendbuffer:add(topic, partition_id, key, msg)
        if overflow then    -- reached batch_size in one topic-partition
            break
        end
    end

    -- 批量发送 send_buffer中的消息
    local all_done = _batch_send(self, sendbuffer)

    --- all_done为false说明，超过重试次数self.max_retry后，仍然没有发送结束。即存在错误
    if not all_done then
        -- 此次loop只会返回 buffer里还有数据的
        for topic, partition_id, buffer in sendbuffer:loop() do
            local queue, index, err, retryable = buffer.queue, buffer.index, buffer.err, buffer.retryable

            -- 创建producer时传入的错误处理函数
            if self.error_handle then
                -- 执行错误处理函数回调
                local ok, err = pcall(self.error_handle, topic, partition_id, queue, index, err, retryable)
                if not ok then
                    ngx_log(ERR, "failed to callback error_handle: ", err)
                end
            else
                -- 没有传入自定义 error_handle
                ngx_log(ERR, "buffered messages send to kafka err: ", err,
                    ", retryable: ", retryable, ", topic: ", topic,
                    ", partition_id: ", partition_id, ", length: ", index / 2)
            end

            -- 清空 sendbuffer
            sendbuffer:clear(topic, partition_id)
        end
    end

    -- 释放flush锁, self.flushing = false
    _flush_unlock(self)

    -- reset _timer_flushing_buffer after flushing complete
    self._timer_flushing_buffer = false

    -- 如果ringbuffer中当前topic个数仍然大于 self.batch_num
    if ringbuffer:need_send() then
        -- 提交一个timer任务重新调自己，timer_at(0, _flush, self)
        _flush_buffer(self)

     -- ringbuffer:left_num() > 0表示队列中还有消息待发送
    elseif is_exiting() and ringbuffer:left_num() > 0 then
        -- still can create 0 timer even exiting
        -- 提交一个timer任务重新调自己，timer_at(0, _flush, self)
        _flush_buffer(self)
    end

    return true
end


-- 异步flush要发送的消息。本方法只是提交了一个timer调用 _flush
-- 两种触发调用路径：1.定时调用 (在创建 producer时，如果是异步模式，会启动定时任务调用此方法， 默认每秒调用一次);
--                2.ringbuffer中的消息数量已经超过了batch_num了
_flush_buffer = function (self)
    -- 并发控制
    -- self._timer_flushing_buffer 为true表示timer提交的flush任务还在执行
    if self._timer_flushing_buffer then
        if debug then
            ngx_log(DEBUG, "another timer is flushing buffer, skipping it")
        end

        return
    end

    local ok, err = timer_at(0, _flush, self)
    -- 成功提交timer
    if ok then
        self._timer_flushing_buffer = true
        return
    end

    ngx_log(ERR, "failed to create timer_at timer, err:", err)
end


-- 定时任务定时flush要发送的消息
local function _timer_flush(premature, self)
    -- _timer_flushing_buffer用于并发控制，避免多个协程同时执行flush
    self._timer_flushing_buffer = false
    _flush_buffer(self)
end


--
-- https://github.com/hachi029/lua-resty-kafka?tab=readme-ov-file#methods-1
function _M.new(self, broker_list, producer_config, cluster_name)
    -- cluster_name specifies the name of the cluster, default 1 (yeah, it's number).
    -- You can Specifies different names when you have two or more kafka clusters.
    -- And this only works with async producer_type
    local name = cluster_name or DEFAULT_CLUSTER_NAME
    local opts = producer_config or {}
    -- 默认为异步模式
    local async = opts.producer_type == "async"
    -- 异步发送模式， 多集群支持
    if async and cluster_inited[name] then
        return cluster_inited[name]
    end

    -- "resty.kafka.client" https://github.com/doujiang24/lua-resty-kafka?tab=readme-ov-file#new
    local cli = client:new(broker_list, producer_config)
    local p = setmetatable({
        client = cli,
        correlation_id = 1,
        request_timeout = opts.request_timeout or 2000,
        retry_backoff = opts.retry_backoff or 100,   -- ms
        max_retry = opts.max_retry or 3,
        required_acks = opts.required_acks or 1,
        -- the partitioner that choose partition from key and partition num
        partitioner = opts.partitioner or default_partitioner,
        error_handle = opts.error_handle,
        api_version = opts.api_version or API_VERSION_V1,
        async = async,
        socket_config = cli.socket_config,
        _timer_flushing_buffer = false,
        ringbuffer = ringbuffer:new(opts.batch_num or 200, opts.max_buffering or 50000,
                opts.wait_on_buffer_full or false, opts.wait_buffer_timeout or 5),   -- 200, 50K, flase, 5s
        -- batch_size默认值1048576， 会导致报错 MESSAGE_TOO_LARGE
        sendbuffer = sendbuffer:new(opts.batch_num or 200, opts.batch_size or 1048576)
                        -- default: 1K, 1M
                        -- batch_size should less than (MaxRequestSize / 2 - 10KiB)
                        -- config in the kafka server, default 100M
    }, mt)

    -- 异步模式
    if async then
        cluster_inited[name] = p
        -- 启动定时任务，定时执行 _timer_flush
        local ok, err = timer_every((opts.flush_time or 1000) / 1000, _timer_flush, p) -- default: 1s
        if not ok then
            ngx_log(ERR, "failed to create timer_every, err: ", err)
        end

    end

    return p
end


-- https://github.com/hachi029/lua-resty-kafka?tab=readme-ov-file#send
-- In sync model
--
-- In case of success, returns the offset (** cdata: LL **) of the current broker and partition.
-- In case of errors, returns nil with a string describing the error.
--
--In async model
--
--The message will write to the buffer first.
-- It will send to the kafka server when the buffer exceed the batch_num, or every flush_time flush the buffer.
--
--It case of success, returns true. In case of errors, returns nil with a string describing the error (buffer overflow).
-- offset is cdata (LL in luajit)
function _M.send(self, topic, key, message)
    --- 异步模式，只是将消息添加到self.ringbuffer中
    if self.async then
        local ringbuffer = self.ringbuffer

        -- 将消息加入到ringbuffer
        local ok, err = ringbuffer:add(topic, key, message)
        if not ok then
            -- 如ringbuffer已满等错误 "buffer overflow"
            return nil, err
        end

        -- self.flushing表示正在flush
        -- need_send() 表示 ringbuffer中的消息数量已经超过了batch_num了
        if not self.flushing and (ringbuffer:need_send() or is_exiting()) then
            -- 触发flush_buffer。此处也是异步发送，仅仅提交了个ngx.timer任务
            _flush_buffer(self)
        end

        return true
    end

    --- 同步模式
    -- 选择分区
    local partition_id, err = choose_partition(self, topic, key)
    if not partition_id then
        return nil, err
    end

    local sendbuffer = self.sendbuffer
    -- 直接向sendbuffer中添加消息
    sendbuffer:add(topic, partition_id, key, message)

    -- 立即发送。ok 为true表示发送成功。sendbuffer中只有1条消息
    local ok = _batch_send(self, sendbuffer)
    if not ok then
        -- 发送失败
        sendbuffer:clear(topic, partition_id)
        return nil, sendbuffer:err(topic, partition_id)
    end

    return sendbuffer:offset(topic, partition_id)
end


-- 同步调用, 发送ringbuffer中所有消息
function _M.flush(self)
    return _flush(nil, self)
end


--
-- Return the sum of all the topic-partition offset (return by the ProduceRequest api);
-- and the details of each topic-partition
-- offset is cdata (LL in luajit)
function _M.offset(self)
    local topics = self.sendbuffer.topics
    -- sum: the sum of all the topic-partition offset (return by the ProduceRequest api);
    -- details: the details of each topic-partition
    local sum, details = 0, {}

    for topic, partitions in pairs(topics) do
        details[topic] = {}
        for partition_id, buffer in pairs(partitions) do
            sum = sum + buffer.offset
            details[topic][partition_id] = buffer.offset
        end
    end

    return sum, details
end


return _M

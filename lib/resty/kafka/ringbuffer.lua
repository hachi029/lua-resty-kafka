-- Copyright (C) Dejiang Zhu(doujiang24)

local semaphore = require "ngx.semaphore"

local setmetatable = setmetatable
local ngx_null = ngx.null

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = {}
local mt = { __index = _M }

-- flush_time: Specifies the queue.buffering.max.ms. Default 1000.
-- batch_num: Specifies the batch.num.messages. Default 200.
-- max_buffering: Specifies the queue.buffering.max.messages. Default 50,000.
-- wait_on_buffer_full: Specifies whether to wait when the buffer queue is full, Default false.
--              When buffer queue is full, if option passed true, will use semaphore wait function to block coroutine until timeout or buffer queue has reduced,
--              Otherwise, return "buffer overflow" error with false. Notice, it could not be used in those phases which do not support yields, i.e. log phase
-- wait_buffer_timeout: Specifies the max wait time when buffer is full, Default 5 seconds.
function _M.new(self, batch_num, max_buffering, wait_on_buffer_full, wait_buffer_timeout)
    local sendbuffer = {
        -- max_buffering 3倍的数组. 连续三项存放一条消息，分别为 queue[1]=topic, queue[2]=key, queue[3]=message
        queue = new_tab(max_buffering * 3, 0),
        -- 当queue中的消息个数超过batch_num后会触发flush, 参考_M.need_send()方法
        batch_num = batch_num,
        -- size是个定值，记录queue的大小
        size = max_buffering * 3,
        -- 首个元素索引位置
        start = 1,
        -- 记录当前元素的数量。(num >= size 表示queue已满。num<=0表示queue为空)
        num = 0,
        -- 当queue满了的时候，再向queue中加入消息时是等待还是直接返回overflow
        wait_on_buffer_full = wait_on_buffer_full,
        -- 当queue满了的时候，如果要等待，等待超时时间
        wait_buffer_timeout = wait_buffer_timeout,
    }

    if wait_on_buffer_full then
        sendbuffer.sema = semaphore.new()
    end

    return setmetatable(sendbuffer, mt)
end


-- producer在异步模式下，只是调用此方法
function _M.add(self, topic, key, message, wait_timeout, depth)
    -- 当前元素的数量
    local num = self.num
    -- 可容纳元素的最大数量
    local size = self.size

    --- queue已满
    if num >= size then
        --- 如果不等待，直接返回 "buffer overflow"
        if not self.wait_on_buffer_full then
            return nil, "buffer overflow"
        end

        --- 等待队列有空闲
        -- 这个方法是递归调用，depth标识递归调用层级
        depth = depth or 1
        if depth > 10 then
            return nil, "buffer overflow and over max depth"
        end

        local timeout = wait_timeout or self.wait_buffer_timeout
        -- 直接返回等待超时
        if timeout <= 0 then
             return nil, "buffer overflow and timeout"
        end

        -- 等待。当其他协程从queue中取出元素后，会调用self.sema:post()唤醒等待
        local start_time = ngx.now()
        local ok, err = self.sema:wait(timeout)
        if not ok then
            return nil, "buffer overflow " .. err
        end
        timeout = timeout - (ngx.now() - start_time)

        -- 重新尝试add
        -- since sema:post to sema:wait is async, so need to check ringbuffer is available again
        return self:add(topic, key, message, timeout, depth + 1)
    end

    --- queue未满，计算当前消息应该存放的位置
    -- self.start 是首个元素的位置
    local index = (self.start + num) % size
    local queue = self.queue

    -- index为当前消息应该存放的位置
    queue[index] = topic
    queue[index + 1] = key
    queue[index + 2] = message

    self.num = num + 3

    return true
end


-- 如果queu上有等待放入的协程，唤醒之
function _M.release_buffer_wait(self)
    if not self.wait_on_buffer_full then
        return
    end

    -- It is enough to release a waiter as only one message pops up
    if self.sema:count() < 0 then
        self.sema:post(1)
    end
end


-- 从queue中取出一条消息
function _M.pop(self)
    local num = self.num
    -- 队列为空
    if num <= 0 then
        return nil, "empty buffer"
    end

    -- 更新self.num
    self.num = num - 3

    local start = self.start
    local queue = self.queue

    -- 更新 self.start
    self.start = (start + 3) % self.size

    local key, topic, message = queue[start], queue[start + 1], queue[start + 2]

    -- 结束位置。当返回key为null时，表示queue已空
    queue[start], queue[start + 1], queue[start + 2] = ngx_null, ngx_null, ngx_null

    -- 如果queu上有等待放入的协程，唤醒之
    self:release_buffer_wait()

    return key, topic, message
end


-- 队列中消息个数
function _M.left_num(self)
    return self.num / 3
end


-- 只要 当前topic个数大于 self.batch_num， 则返回true
function _M.need_send(self)
    return self.num / 3 >= self.batch_num
end


return _M

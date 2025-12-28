-- Copyright (C) Dejiang Zhu(doujiang24)


local setmetatable = setmetatable
local pairs = pairs
local next = next


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local MAX_REUSE = 10000


local _M = {}
local mt = { __index = _M }


--- sendbuffer以topic/partition_id进行分组组织所有的message. 格式为 topics[topic][partition_id] = {queue, index,..}
-- batch_num: Specifies the batch.num.messages. Default 200.
-- batch_size: Specifies the send.buffer.bytes. Default 1M(may reach 2M).
--              Be careful, SHOULD be smaller than the socket.request.max.bytes / 2 - 10k config in kafka server.
function _M.new(self, batch_num, batch_size)
    local sendbuffer = {
        -- 格式为 topics[topic][partition_id] = {queue, index,..}
        topics = {},
        -- 记录当前sendbuffer的queue的个数。即总的topics[topic][partition_id]的个数
        queue_num = 0,
        -- 每2个item标识一条消息queue[1]=key, queue[2]=message,
        batch_num = batch_num * 2,
        -- send.buffer.bytes。 如果某个queue中消息长度达到这个数量，将触发flush
        batch_size = batch_size,
    }
    return setmetatable(sendbuffer, mt)
end


--- 添加一条消息, 以self.topics[topic][partition_id] ={queue} 格式组织数据
-- 返回true表示，topic和指定partition_id中消息长度超过了batch_size或消息个数超过了batch_num。将触发立即将所有消息发送至broker
function _M.add(self, topic, partition_id, key, msg)
    local topics = self.topics

    -- 初始化topic
    if not topics[topic] then
        topics[topic] = {}
    end

    -- 初始化partition_id
    if not topics[topic][partition_id] then
        topics[topic][partition_id] = {
            queue = new_tab(self.batch_num, 0),
            -- 记录下次要写入的queue的索引，每两个queue的元素为一条kafka消息
            index = 0,
            -- 记录重用次数，超过 MAX_REUSE后queue会重建，参考_M.clear()方法
            used = 0,
            -- 记录当前partition_id下消息总长度(字节数 #key+#msg)
            size = 0,
            -- 此字段好像没有作用
            offset = 0,
            retryable = true,
            -- 当发送错误时，此字段记录错误消息
            err = "",
        }
    end

    -- 找到当前消息所属的buffer
    local buffer = topics[topic][partition_id]
    local index = buffer.index
    local queue = buffer.queue

    -- 初次调用
    if index == 0 then
        -- 记录当前sendbuffer的queue的个数
        self.queue_num = self.queue_num + 1
        -- 重置 buffer.retryable
        buffer.retryable = true
    end

    -- 将消息放入queue中
    queue[index + 1] = key
    queue[index + 2] = msg

    buffer.index = index + 2
    -- 记录消息长度（字节数）
    buffer.size = buffer.size + #msg + (key and #key or 0)

    -- 如果消息长度超过batch_size或消息个数超过batch_num，则返回true
    if (buffer.size >= self.batch_size) or (buffer.index >= self.batch_num) then
        return true
    end
end


-- 设置或读取offset. offset is a unique identifier assigned to each record (message) in a partition
-- Offsets are sequential integers that Kafka uses to maintain the order of messages within a partition
-- 如果offset为nil, 则返回self.topics[topic][partition_id].offset
-- 如果offset不为nil, 则设置self.topics[topic][partition_id].offset
function _M.offset(self, topic, partition_id, offset)
    local buffer = self.topics[topic][partition_id]

    if not offset then
        return buffer.offset
    end

    -- 更新 offset。offset is a unique identifier assigned to each record (message) in a partition
    buffer.offset = offset + (buffer.index / 2)
end


-- 重置self.topics[topic][partition_id]对于的buffer相关属性,清空buffer
function _M.clear(self, topic, partition_id)
    local buffer = self.topics[topic][partition_id]
    -- 重置index, size
    buffer.index = 0
    buffer.size = 0
    buffer.used = buffer.used + 1

    -- buffer.used超过10000，则重新创建buffer
    if buffer.used >= MAX_REUSE then
        buffer.queue = new_tab(self.batch_num, 0)
        buffer.used = 0
    end

    -- 更新queue_num, queue_num -1
    self.queue_num = self.queue_num - 1
end


-- 返回true说明sendbuffer中所有topic下的所有partition中的消息都发完了
function _M.done(self)
    return self.queue_num == 0
end


-- 当发送错误时，调用此方法。 err为根据响应体中错误码映射出来的错误消息msg
function _M.err(self, topic, partition_id, err, retryable)
    local buffer = self.topics[topic][partition_id]

    -- err不为nil, 则设置nil
    if err then
        buffer.err = err
        buffer.retryable = retryable
        return buffer.index
    else
        -- 读取err, retryable
        return buffer.err, buffer.retryable
    end
end


-- 遍历self.topics[topic][partition_id]={queue,..}, 每次遍历返回一个{queue,..}
function _M.loop(self)
    -- t为topic名称,p 为partition_id，使用upvalue记录上次遍历的位置
    local topics, t, p = self.topics

    return function ()
        -- t 不为nil表示不是首次遍历
        if t then
            -- next(table, index) 函数。
            -- 这允许迭代器在被外部 for 循环调用时，能够“记住”上一次遍历到哪了，而不需要将所有数据一次性拷贝到数组中
            for partition_id, queue in next, topics[t], p do
                p = partition_id
                -- 只返回不为空的队列
                if queue.index > 0 then
                    return t, partition_id, queue
                end
            end
        end


        -- 首次遍历
        for topic, partitions in next, topics, t do
            t = topic
            p = nil
            for partition_id, queue in next, partitions, p do
                p = partition_id
                -- 只返回不为空的队列
                if queue.index > 0 then
                    return topic, partition_id, queue
                end
            end
        end

        return
    end
end


-- 以broker为key对消息进行分组
-- 返回分组结果 sendbroker，及其大小
function _M.aggregator(self, client)
    local num = 0
    -- 保存了以broker为key
    local sendbroker = {}
    local brokers = {}

    local i = 1
    -- 遍历self.topics，每次循环从self.topics[topic][partition_id]取出一项
    for topic, partition_id, queue in self:loop() do
        if queue.retryable then
            -- 根据指定topic/partition_id选择对应的leader broker
            local broker_conf, err = client:choose_broker(topic, partition_id)
            if not broker_conf then
                self:err(topic, partition_id, err, true)

            else
                -- broker有效
                if not brokers[broker_conf] then
                    -- 初始化broker对应的队列
                    brokers[broker_conf] = {
                        topics = {},
                        topic_num = 0,
                        size = 0,
                    }
                end

                local broker = brokers[broker_conf]
                if not broker.topics[topic] then
                    -- 初始化broker中指定topic对应的队列
                    brokers[broker_conf].topics[topic] = {
                        partitions = {},
                        partition_num = 0,
                    }

                    -- topic_num 存放了broker中topic个数
                    broker.topic_num = broker.topic_num + 1
                end

                local broker_topic = broker.topics[topic]

                -- 设置broker-topic-paritition 对于的queue
                broker_topic.partitions[partition_id] = queue
                broker_topic.partition_num = broker_topic.partition_num + 1

                -- 总字节数
                broker.size = broker.size + queue.size

                -- broker.size为发往当前broker所有消息的大小
                if broker.size >= self.batch_size then
                    sendbroker[num + 1] = broker_conf
                    sendbroker[num + 2] = brokers[broker_conf]

                    num = num + 2
                    -- 已经放入sendbroker的置null
                    brokers[broker_conf] = nil
                end
            end
        end
    end

    -- 上一步将 broker.size >= self.batch_size 的 broker所属队列已经加到sendbroker中了
    -- 此处添加剩余的。（也就是优先发送broker消息已经满了的）
    for broker_conf, topic_partitions in pairs(brokers) do
        sendbroker[num + 1] = broker_conf
        sendbroker[num + 2] = brokers[broker_conf]
        num = num + 2
    end

    -- 返回sendbroker的大小
    return num, sendbroker
end


return _M

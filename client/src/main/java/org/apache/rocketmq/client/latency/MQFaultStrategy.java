/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    /**
     * 延迟故障容错，维护每个Broker的发送消息的延迟
     * key：brokerName
     * latency：延迟
     * Fault： 故障
     * Tolerance：宽容，容忍 -- 容错
     */
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    /**
     * 发送消息延迟容错开关
     * 消息发送容错策略。默认情况下容错策略关闭，
     */
    private boolean sendLatencyFaultEnable = false;

    /**
     * 延迟级别数组
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    /**
     * 不可用时长数组
     */
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 根据topic发布信息 选择一个消息队列
     * @param tpInfo topic发布信息
     * @param lastBrokerName brokerName
     * @return 消息队列
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {
            /**
             * 容错策略选择消息队列逻辑。优先获取可用队列，
             * 其次选择一个broker获取队列，最差返回任意broker的一个队列
             */
            try {
                // 获取 brokerName=lastBrokerName && 可用的一个消息队列
                //从主题路由信息中获取一个队列
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //选择的 队列如果可用 第一次发送消息 或选择出可用的broker为最后使用的一个
                    // 校验对象是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        //
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                // 选择一个相对好的broker，并获得其对应的一个消息队列，不考虑该队列的可用性
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            // 选择一个消息队列，不考虑队列的可用性,未开启容错策略选择消息队列逻辑
            return tpInfo.selectOneMessageQueue();
        }
        // 获得 lastBrokerName 对应的一个消息队列，不考虑该队列的可用性
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 更新延迟容错信息
     * @param brokerName brokerName
     * @param currentLatency 延迟
     * @param isolation 是否隔离 当开启隔离时 默认延迟为30000 目前主要用于发送消息异常时
     * 更新延迟容错信息。当 Producer 发送消息时间过长，则逻辑认为N秒内不可用。
     * 按照latencyMax，notAvailableDuration的配置，对应如下：
     * | Producer发送消息消耗时长 | Broker不可用时长 |
     * | — | — |
     * | >= 15000 ms | 600 1000 ms |
     * | >= 3000 ms | 180 1000 ms |
     * | >= 2000 ms | 120 1000 ms |
     * | >= 1000 ms | 60 1000 ms |
     * | >= 550 ms | 30 * 1000 ms |
     * | >= 100 ms | 0 ms |
     * | >= 50 ms | 0 ms |
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算延迟对应的不可用时间
     * @param currentLatency 延迟
     * @return 不可用时间
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}

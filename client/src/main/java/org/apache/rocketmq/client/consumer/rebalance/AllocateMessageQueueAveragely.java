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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 * 平均分配队列策略
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        // 校验参数是否正确
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        // 平均分配
        /**
         * index ：当前 Consumer 在消费集群里是第几个。这里就是为什么需要对传入的 cidAll 参数必须进行排序的原因。
         * 如果不排序，Consumer 在本地计算出来的 index 无法一致，影响计算结果
         */
        int index = cidAll.indexOf(currentCID);// 第几个consumer。
        int mod = mqAll.size() % cidAll.size();// 余数，即多少消息队列无法平均分配。
        /**
         * 前面 mod 个 Consumer 平分余数，多获得 1 个消息队列。
         */
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        // 有余数的情况下，[0, mod) 平分余数，即每consumer多分配一个节点；第index开始，跳过前mod余数。
        //startIndex ：Consumer 分配消息队列开始位置
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        // 分配队列数量。之所以要Math.min()的原因是，mqAll.size() <= cidAll.size()，部分consumer分配不到消息队列。
        //range ：分配队列数量。之所以要 Math#min(...) 的原因：当 mqAll.size() <= cidAll.size() 时，最后几个 Consumer 分配不到消息队列。
        int range = Math.min(averageSize, mqAll.size() - startIndex);

        //生成分配消息队列结果
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}

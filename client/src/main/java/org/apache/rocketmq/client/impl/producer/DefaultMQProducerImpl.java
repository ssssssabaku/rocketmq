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
package org.apache.rocketmq.client.impl.producer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.RequestFutureTable;
import org.apache.rocketmq.client.producer.RequestResponseFuture;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;

public class DefaultMQProducerImpl implements MQProducerInner {
    private final InternalLogger log = ClientLogger.getLog();
    private final Random random = new Random();
    private final DefaultMQProducer defaultMQProducer;
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable =
        new ConcurrentHashMap<String, TopicPublishInfo>();
    private final ArrayList<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    private final RPCHook rpcHook;
    private final BlockingQueue<Runnable> asyncSenderThreadPoolQueue;
    private final ExecutorService defaultAsyncSenderExecutor;
    private final Timer timer = new Timer("RequestHouseKeepingService", true);
    protected BlockingQueue<Runnable> checkRequestQueue;
    protected ExecutorService checkExecutor;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<CheckForbiddenHook>();
    private int zipCompressLevel = Integer.parseInt(System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));
    private MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();
    private ExecutorService asyncSenderExecutor;

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
        this(defaultMQProducer, null);
    }

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
        this.defaultMQProducer = defaultMQProducer;
        this.rpcHook = rpcHook;

        this.asyncSenderThreadPoolQueue = new LinkedBlockingQueue<Runnable>(50000);
        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.asyncSenderThreadPoolQueue,
            new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "AsyncSenderExecutor_" + this.threadIndex.incrementAndGet());
                }
            });
    }

    public void registerCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook) {
        this.checkForbiddenHookList.add(checkForbiddenHook);
        log.info("register a new checkForbiddenHook. hookName={}, allHookSize={}", checkForbiddenHook.hookName(),
            checkForbiddenHookList.size());
    }

    public void initTransactionEnv() {
        TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;
        if (producer.getExecutorService() != null) {
            this.checkExecutor = producer.getExecutorService();
        } else {
            this.checkRequestQueue = new LinkedBlockingQueue<Runnable>(producer.getCheckRequestHoldMax());
            this.checkExecutor = new ThreadPoolExecutor(
                producer.getCheckThreadPoolMinSize(),
                producer.getCheckThreadPoolMaxSize(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.checkRequestQueue);
        }
    }

    public void destroyTransactionEnv() {
        if (this.checkExecutor != null) {
            this.checkExecutor.shutdown();
        }
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register sendMessage Hook, {}", hook.hookName());
    }

    public void start() throws MQClientException {
        this.start(true);
    }

    public void start(final boolean startFactory) throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;

                this.checkConfig();

                // 检查productgroup是否符合要求，并改变生产者的instancename 为进程ID
                if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                    this.defaultMQProducer.changeInstanceNameToPID();
                }

                //创建MQclientinstance实例。整个jvm中只创建一个mqclientManager
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);

                // 向mqclientmanager注册，将当前生产着加入到mqclientinstance管理中，方便后续继续调用、进行心跳检测
                boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }

                this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());

                if (startFactory) {
                    mQClientFactory.start();
                }

                log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(),
                    this.defaultMQProducer.isSendMessageWithVIPChannel());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The producer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }

        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    RequestFutureTable.scanExpiredRequest();
                } catch (Throwable e) {
                    log.error("scan RequestFutureTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

        if (null == this.defaultMQProducer.getProducerGroup()) {
            throw new MQClientException("producerGroup is null", null);
        }

        if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.",
                null);
        }
    }

    public void shutdown() {
        this.shutdown(true);
    }

    public void shutdown(final boolean shutdownFactory) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
                this.defaultAsyncSenderExecutor.shutdown();
                if (shutdownFactory) {
                    this.mQClientFactory.shutdown();
                }

                log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    @Override
    public Set<String> getPublishTopicList() {
        Set<String> topicList = new HashSet<String>();
        for (String key : this.topicPublishInfoTable.keySet()) {
            topicList.add(key);
        }

        return topicList;
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

        return null == prev || !prev.ok();
    }

    /**
     * This method will be removed in the version 5.0.0 and <code>getCheckListener</code> is recommended.
     *
     * @return
     */
    @Override
    @Deprecated
    public TransactionCheckListener checkListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionCheckListener();
        }

        return null;
    }

    @Override
    public TransactionListener getCheckListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionListener();
        }
        return null;
    }

    @Override
    public void checkTransactionState(final String addr, final MessageExt msg,
        final CheckTransactionStateRequestHeader header) {
        Runnable request = new Runnable() {
            private final String brokerAddr = addr;
            private final MessageExt message = msg;
            private final CheckTransactionStateRequestHeader checkRequestHeader = header;
            private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();

            @Override
            public void run() {
                TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();
                TransactionListener transactionListener = getCheckListener();
                if (transactionCheckListener != null || transactionListener != null) {
                    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                    Throwable exception = null;
                    try {
                        if (transactionCheckListener != null) {
                            localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
                        } else if (transactionListener != null) {
                            log.debug("Used new check API in transaction message");
                            localTransactionState = transactionListener.checkLocalTransaction(message);
                        } else {
                            log.warn("CheckTransactionState, pick transactionListener by group[{}] failed", group);
                        }
                    } catch (Throwable e) {
                        log.error("Broker call checkTransactionState, but checkLocalTransactionState exception", e);
                        exception = e;
                    }

                    this.processTransactionState(
                        localTransactionState,
                        group,
                        exception);
                } else {
                    log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
                }
            }

            private void processTransactionState(
                final LocalTransactionState localTransactionState,
                final String producerGroup,
                final Throwable exception) {
                final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
                thisHeader.setCommitLogOffset(checkRequestHeader.getCommitLogOffset());
                thisHeader.setProducerGroup(producerGroup);
                thisHeader.setTranStateTableOffset(checkRequestHeader.getTranStateTableOffset());
                thisHeader.setFromTransactionCheck(true);

                String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (uniqueKey == null) {
                    uniqueKey = message.getMsgId();
                }
                thisHeader.setMsgId(uniqueKey);
                thisHeader.setTransactionId(checkRequestHeader.getTransactionId());
                switch (localTransactionState) {
                    case COMMIT_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                        break;
                    case ROLLBACK_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                        log.warn("when broker check, client rollback this transaction, {}", thisHeader);
                        break;
                    case UNKNOW:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                        log.warn("when broker check, client does not know this transaction state, {}", thisHeader);
                        break;
                    default:
                        break;
                }

                String remark = null;
                if (exception != null) {
                    remark = "checkLocalTransactionState Exception: " + RemotingHelper.exceptionSimpleDesc(exception);
                }

                try {
                    DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, thisHeader, remark,
                        3000);
                } catch (Exception e) {
                    log.error("endTransactionOneway exception", e);
                }
            }
        };

        this.checkExecutor.submit(request);
    }

    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev.toString());
            }
        }
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQProducer.isUnitMode();
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.makeSureStateOK();
        Validators.checkTopic(newTopic);

        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK, "
                + this.serviceState
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                null);
        }
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public MessageExt viewMessage(
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.makeSureStateOK();

        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
        throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    /**
     * DEFAULT ASYNC -------------------------------------------------------
     */
    public void send(Message msg,
        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        send(msg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout.
     * A new one will be provided in next version
     * @param msg
     * @param sendCallback
     * @param timeout the <code>sendCallback</code> will be invoked at most time
     * @throws RejectedExecutionException
     */
    @Deprecated
    public void send(final Message msg, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            sendDefaultImpl(msg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {
                        sendCallback.onException(
                            new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
    }

    /**
     *
     * @param brokerName broker名称
     * @param currentLatency 本次消息发送延迟时间
     * @param isolation 是否隔离是否隔离，该参数的含义如果为true ，则使用默认时长30s 来
     * 计算Broker 故障规避时长，如果为false
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
    }

    private void validateNameServerSetting() throws MQClientException {
        List<String> nsList = this.getmQClientFactory().getMQClientAPIImpl().getNameServerAddressList();
        if (null == nsList || nsList.isEmpty()) {
            throw new MQClientException(
                "No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null).setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
        }

    }

    private SendResult sendDefaultImpl(
        Message msg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.makeSureStateOK();//校验Producer服务器是否处于运行状态
        Validators.checkMessage(msg, this.defaultMQProducer);//校验消息格式
        final long invokeID = random.nextLong();//生成消息编号,用于日志打印,表示同一消息
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        long endTimestamp = beginTimestampFirst;
        //获取topic路由信息
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            boolean callTimeout = false;
            MessageQueue mq = null;//消息将要发送到的队列
            Exception exception = null;
            SendResult sendResult = null;//最后一次的发送结果
            int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
            int times = 0;//第几次发送
            String[] brokersSent = new String[timesTotal];//保存每次发送消息选择的broker
            for (; times < timesTotal; times++) {
                //循环发送消息直到成功或者最大次数
                String lastBrokerName = null == mq ? null : mq.getBrokerName();
                //选择消息队列（将要发送到的队列）
                MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
                if (mqSelected != null) {
                    mq = mqSelected;
                    brokersSent[times] = mq.getBrokerName();
                    try {
                        beginTimestampPrev = System.currentTimeMillis();
                        if (times > 0) {
                            //Reset topic with namespace during resend.
                            msg.setTopic(this.defaultMQProducer.withNamespace(msg.getTopic()));
                        }
                        long costTime = beginTimestampPrev - beginTimestampFirst;
                        if (timeout < costTime) {
                            callTimeout = true;
                            break;
                        }
                        //发送消息(发送消息的核心方法)
                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
                        endTimestamp = System.currentTimeMillis();
                        //更新broker可用信息
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        switch (communicationMode) {
                            case ASYNC:
                                return null;
                            case ONEWAY:
                                return null;
                            case SYNC:
                                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                    if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                                        continue;
                                    }
                                }

                                return sendResult;
                            default:
                                break;
                        }
                    } catch (RemotingException e) {//打印异常
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (MQClientException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (MQBrokerException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        switch (e.getResponseCode()) {
                            //如下异常进行消息重试
                            case ResponseCode.TOPIC_NOT_EXIST:
                            case ResponseCode.SERVICE_NOT_AVAILABLE:
                            case ResponseCode.SYSTEM_ERROR:
                            case ResponseCode.NO_PERMISSION:
                            case ResponseCode.NO_BUYER_ID:
                            case ResponseCode.NOT_IN_CURRENT_UNIT:
                                continue;
                            default:
                                //有发送结果进行返回,否则抛出异常
                                if (sendResult != null) {
                                    return sendResult;
                                }

                                throw e;
                        }
                    } catch (InterruptedException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        log.warn(String.format("sendKernelImpl exception, throw exception, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());

                        log.warn("sendKernelImpl exception", e);
                        log.warn(msg.toString());
                        throw e;
                    }
                } else {
                    break;
                }
            }
            //返回发送结果
            if (sendResult != null) {
                return sendResult;
            }
            //根据不同情况,抛出不同异常
            String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s",
                times,
                System.currentTimeMillis() - beginTimestampFirst,
                msg.getTopic(),
                Arrays.toString(brokersSent));

            info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

            MQClientException mqClientException = new MQClientException(info, exception);
            if (callTimeout) {
                throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
            }

            if (exception instanceof MQBrokerException) {
                mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
            } else if (exception instanceof RemotingConnectException) {
                mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
            } else if (exception instanceof RemotingTimeoutException) {
                mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
            } else if (exception instanceof MQClientException) {
                mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
            }

            throw mqClientException;
        }

        validateNameServerSetting();

        throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
            null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
    }

    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        //本地路由信息表获取topic信息(// 缓存中获取 Topic发布信息)
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        //判断本地路由信息表是否存在topic(// 当本地无可用的 Topic发布信息时，从Namesrv获取一次)
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            //新创建一个对象放入路由信息表中
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            //在nameserver中更新路由信息(尝试第一次从nameserver获取路由信息)
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            //本地信息路由表中取出路由信息
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }
        // 若获取的 Topic发布信息时候可用，则返回
        if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
            return topicPublishInfo;
        } else {
            //第一次获取路由信息没有找到。
            // 第二次尝试获取路由信息(使用默认的)
            /**
             * {@link DefaultMQProducer#createTopicKey}对应的 Topic发布信息。用于 Topic发布信息不存在 && Broker支持自动创建Topic
              */
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
    }

    /**
     *
     * 发送消息核心方法。该方法真正发起网络请求，发送消息给 Broker。
     * @param msg 待发送消息
     * @param mq 消息发送队列
     * @param communicationMode 消息发送模式 同步、异步、ONEWAY
     * @param sendCallback 异步消息回调函数
     * @param topicPublishInfo 主题路由信息
     * @param timeout 消息发送超时时间
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    private SendResult sendKernelImpl(final Message msg,
        final MessageQueue mq,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final TopicPublishInfo topicPublishInfo,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        // 获取broker地址
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            tryToFindTopicPublishInfo(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        SendMessageContext context = null;
        if (brokerAddr != null) {
            //是否使用 brokervip通道。broker会开启两个端口对外服务
            brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);
            //记录消息内容 下面逻辑可能改变消息内容 如：压缩消息
            byte[] prevBody = msg.getBody();
            try {
                //for MessageBatch,ID has been set in the generating process
                if (!(msg instanceof MessageBatch)) {
                    MessageClientIDSetter.setUniqID(msg);//设置全局唯一ID
                }

                boolean topicWithNamespace = false;
                if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
                    msg.setInstanceId(this.mQClientFactory.getClientConfig().getNamespace());//设置INSTANCE_ID
                    topicWithNamespace = true;
                }

                int sysFlag = 0;
                boolean msgBodyCompressed = false;
                //压缩消息
                if (this.tryToCompressMessage(msg)) {
                    sysFlag |= MessageSysFlag.COMPRESSED_FLAG;//将消息标记为 "压缩消息"
                    msgBodyCompressed = true;//是否已压缩
                }

                //事物
                final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (tranMsg != null && Boolean.parseBoolean(tranMsg)) {
                    sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;//标记为事物消息
                }

                //钩子函数，发送消息校验
                if (hasCheckForbiddenHook()) {
                    CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
                    checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
                    checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
                    checkForbiddenContext.setCommunicationMode(communicationMode);
                    checkForbiddenContext.setBrokerAddr(brokerAddr);
                    checkForbiddenContext.setMessage(msg);
                    checkForbiddenContext.setMq(mq);
                    checkForbiddenContext.setUnitMode(this.isUnitMode());
                    this.executeCheckForbiddenHook(checkForbiddenContext);
                }
                //钩子函数 发送消息前逻辑
                if (this.hasSendMessageHook()) {
                    context = new SendMessageContext();
                    context.setProducer(this);
                    context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                    context.setCommunicationMode(communicationMode);
                    context.setBornHost(this.defaultMQProducer.getClientIP());
                    context.setBrokerAddr(brokerAddr);
                    context.setMessage(msg);
                    context.setMq(mq);
                    context.setNamespace(this.defaultMQProducer.getNamespace());
                    String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (isTrans != null && isTrans.equals("true")) {
                        context.setMsgType(MessageType.Trans_Msg_Half);
                    }

                    if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
                        context.setMsgType(MessageType.Delay_Msg);
                    }
                    this.executeSendMessageHookBefore(context);
                }

                //构建发送消息请求
                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                requestHeader.setTopic(msg.getTopic());
                requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setSysFlag(sysFlag);
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                requestHeader.setFlag(msg.getFlag());
                requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
                requestHeader.setReconsumeTimes(0);
                requestHeader.setUnitMode(this.isUnitMode());
                requestHeader.setBatch(msg instanceof MessageBatch);
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {//消息重发topic
                    String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                    if (reconsumeTimes != null) {
                        requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                    }

                    String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                    if (maxReconsumeTimes != null) {
                        requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                    }
                }

                //发送消息
                SendResult sendResult = null;
                switch (communicationMode) {
                    case ASYNC:
                        Message tmpMessage = msg;
                        boolean messageCloned = false;
                        if (msgBodyCompressed) {
                            //If msg body was compressed, msgbody should be reset using prevBody.
                            //Clone new message using commpressed message body and recover origin massage.
                            //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
                            tmpMessage = MessageAccessor.cloneMessage(msg);
                            messageCloned = true;
                            msg.setBody(prevBody);
                        }

                        if (topicWithNamespace) {
                            if (!messageCloned) {
                                tmpMessage = MessageAccessor.cloneMessage(msg);
                                messageCloned = true;
                            }
                            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                        }

                        long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeAsync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            mq.getBrokerName(),
                            tmpMessage,
                            requestHeader,
                            timeout - costTimeAsync,
                            communicationMode,
                            sendCallback,
                            topicPublishInfo,
                            this.mQClientFactory,
                            this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(),
                            context,
                            this);
                        break;
                    case ONEWAY:
                    case SYNC:
                        long costTimeSync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeSync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            mq.getBrokerName(),
                            msg,
                            requestHeader,
                            timeout - costTimeSync,
                            communicationMode,
                            context,
                            this);
                        break;
                    default:
                        assert false;
                        break;
                }

                //钩子函数，发送消息之后的逻辑
                if (this.hasSendMessageHook()) {
                    context.setSendResult(sendResult);
                    this.executeSendMessageHookAfter(context);
                }

                //返回发送结果
                return sendResult;
            } catch (RemotingException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (MQBrokerException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (InterruptedException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } finally {
                msg.setBody(prevBody);
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
            }
        }

        // broker为空抛出异常
        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    private boolean tryToCompressMessage(final Message msg) {
        if (msg instanceof MessageBatch) {
            //batch dose not support compressing right now
            return false;
        }
        byte[] body = msg.getBody();
        if (body != null) {
            //消息长度大于4K，则对消息进行压缩处理
            if (body.length >= this.defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
                try {
                    byte[] data = UtilAll.compress(body, zipCompressLevel);
                    if (data != null) {
                        msg.setBody(data);
                        return true;
                    }
                } catch (IOException e) {
                    log.error("tryToCompressMessage exception", e);
                    log.warn(msg.toString());
                }
            }
        }

        return false;
    }

    public boolean hasCheckForbiddenHook() {
        return !checkForbiddenHookList.isEmpty();
    }

    public void executeCheckForbiddenHook(final CheckForbiddenContext context) throws MQClientException {
        if (hasCheckForbiddenHook()) {
            for (CheckForbiddenHook hook : checkForbiddenHookList) {
                hook.checkForbidden(context);
            }
        }
    }

    public boolean hasSendMessageHook() {
        return !this.sendMessageHookList.isEmpty();
    }

    public void executeSendMessageHookBefore(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageBefore(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookBefore", e);
                }
            }
        }
    }

    public void executeSendMessageHookAfter(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookAfter", e);
                }
            }
        }
    }

    /**
     * DEFAULT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * KERNEL SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueue mq)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueue mq, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //获取开始时间
        long beginStartTime = System.currentTimeMillis();
        //校验服务器是否处于运行中状态
        this.makeSureStateOK();
        //校验消息是否为空，长度是否为0，大小是否超过4M
        Validators.checkMessage(msg, this.defaultMQProducer);

        if (!msg.getTopic().equals(mq.getTopic())) {
            throw new MQClientException("message's topic not equal mq's topic", null);
        }
        //判断处理是否是否超时
        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout < costTime) {
            throw new RemotingTooMuchRequestException("call timeout");
        }
        //同步发送
        return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
    }

    /**
     * KERNEL ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        send(msg, mq, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param mq
     * @param sendCallback
     * @param timeout the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        makeSureStateOK();
                        Validators.checkMessage(msg, defaultMQProducer);

                        if (!msg.getTopic().equals(mq.getTopic())) {
                            throw new MQClientException("message's topic not equal mq's topic", null);
                        }
                        long costTime = System.currentTimeMillis() - beginStartTime;
                        if (timeout > costTime) {
                            try {
                                sendKernelImpl(msg, mq, CommunicationMode.ASYNC, sendCallback, null,
                                    timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknown exception", e);
                            }
                        } else {
                            sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                        }
                    } catch (Exception e) {
                        sendCallback.onException(e);
                    }

                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    /**
     * KERNEL ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg,
        MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        try {
            this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * SELECT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
    }

    private SendResult sendSelectImpl(
        Message msg,
        MessageQueueSelector selector,
        Object arg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback, final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            try {
                //选择消息队列
                List<MessageQueue> messageQueueList =
                    mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                Message userMessage = MessageAccessor.cloneMessage(msg);
                String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mQClientFactory.getClientConfig().getNamespace());
                userMessage.setTopic(userTopic);

                mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
            } catch (Throwable e) {
                throw new MQClientException("select message queue throwed exception.", e);
            }

            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout < costTime) {
                throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
            }
            if (mq != null) {
                // 发送消息
                return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - costTime);
            } else {
                throw new MQClientException("select message queue return null.", null);
            }
        }

        validateNameServerSetting();
        throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
    }

    /**
     * SELECT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        send(msg, selector, arg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param selector
     * @param arg
     * @param sendCallback
     * @param timeout the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueueSelector selector, final Object arg,
        final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            try {
                                sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, sendCallback,
                                    timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknownn exception", e);
                            }
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {
                        sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("exector rejected ", e);
        }
    }

    /**
     * SELECT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * 发送事物消息
     * @param msg 消息
     * @param localTransactionExecuter 本地事物 执行器
     * @param arg 本地事物执行器参数
     * @return 事物发送结果
     * @throws MQClientException 当clinet发生异常时
     */
    public TransactionSendResult sendMessageInTransaction(final Message msg,
        final LocalTransactionExecuter localTransactionExecuter, final Object arg)
        throws MQClientException {
        TransactionListener transactionListener = getCheckListener();
        if (null == localTransactionExecuter && null == transactionListener) {
            throw new MQClientException("tranExecutor is null", null);
        }

        // ignore DelayTimeLevel parameter
        if (msg.getDelayTimeLevel() != 0) {
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        }

        Validators.checkMessage(msg, this.defaultMQProducer);

        // 发送【Half消息】
        SendResult sendResult = null;
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
        try {
            sendResult = this.send(msg);
        } catch (Exception e) {
            throw new MQClientException("send message Exception", e);
        }

        // 处理发送【Half消息】结果
        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;
        switch (sendResult.getSendStatus()) {
            // 发送【Half消息】成功，执行【本地事务】逻辑
            case SEND_OK: {
                try {
                    if (sendResult.getTransactionId() != null) {
                        // 事务编号。目前开源版本暂时没用到，猜想ONS在使用。
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                    if (null != transactionId && !"".equals(transactionId)) {
                        msg.setTransactionId(transactionId);
                    }
                    // 执行【本地事务】逻辑
                    if (null != localTransactionExecuter) {
                        localTransactionState = localTransactionExecuter.executeLocalTransactionBranch(msg, arg);
                    } else if (transactionListener != null) {
                        log.debug("Used new transaction API");
                        localTransactionState = transactionListener.executeLocalTransaction(msg, arg);
                    }
                    if (null == localTransactionState) {
                        localTransactionState = LocalTransactionState.UNKNOW;
                    }

                    if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                        log.info("executeLocalTransactionBranch return {}", localTransactionState);
                        log.info(msg.toString());
                    }
                } catch (Throwable e) {
                    log.info("executeLocalTransactionBranch exception", e);
                    log.info(msg.toString());
                    localException = e;
                }
            }
            break;
            // 发送【Half消息】失败，标记【本地事务】状态为回滚
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
            default:
                break;
        }

        // 结束事务：提交消息 COMMIT / ROLLBACK
        try {
            this.endTransaction(sendResult, localTransactionState, localException);
        } catch (Exception e) {
            log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
        }

        // 返回【事务发送结果】
        TransactionSendResult transactionSendResult = new TransactionSendResult();
        transactionSendResult.setSendStatus(sendResult.getSendStatus());
        transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
        transactionSendResult.setMsgId(sendResult.getMsgId());
        transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
        transactionSendResult.setTransactionId(sendResult.getTransactionId());
        transactionSendResult.setLocalTransactionState(localTransactionState);
        return transactionSendResult;
    }

    /**
     * DEFAULT SYNC -------------------------------------------------------
     */
    public SendResult send(
        Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * 事物结束：提交或回滚
     * @param sendResult 发送【Half消息】结果
     * @param localTransactionState 【本地事务】状态
     * @param localException 执行【本地事务】逻辑产生的异常
     * @throws RemotingException 当远程调用发生异常时
     * @throws MQBrokerException 当 Broker 发生异常时
     * @throws InterruptedException 当线程中断时
     * @throws UnknownHostException 当解码消息编号失败是
     */
    public void endTransaction(
        final SendResult sendResult,
        final LocalTransactionState localTransactionState,
        final Throwable localException) throws RemotingException, MQBrokerException, InterruptedException, UnknownHostException {
        // 解码消息编号
        final MessageId id;
        if (sendResult.getOffsetMsgId() != null) {
            id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
        } else {
            id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
        }
        // 创建请求
        String transactionId = sendResult.getTransactionId();
        final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        requestHeader.setTransactionId(transactionId);
        requestHeader.setCommitLogOffset(id.getOffset());
        switch (localTransactionState) {
            case COMMIT_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            case ROLLBACK_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                break;
            case UNKNOW:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                break;
            default:
                break;
        }

        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
        requestHeader.setMsgId(sendResult.getMsgId());
        String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
        // 提交消息 COMMIT / ROLLBACK。！！！通信方式为：Oneway！！！
        this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark,
            this.defaultMQProducer.getSendMsgTimeout());
    }

    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.mQClientFactory.getMQClientAPIImpl().getRemotingClient().setCallbackExecutor(callbackExecutor);
    }

    public ExecutorService getAsyncSenderExecutor() {
        return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
    }

    public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor) {
        this.asyncSenderExecutor = asyncSenderExecutor;
    }

    public SendResult send(Message msg,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
    }

    public Message request(Message msg,
        long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendReqeustOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendReqeustOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
            if (responseMessage == null) {
                if (requestResponseFuture.isSendRequestOk()) {
                    throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                        "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
                } else {
                    throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
                }
            }
            return responseMessage;
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(Message msg, final RequestCallback requestCallback, long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendReqeustOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);
    }

    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendReqeustOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendReqeustOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
            if (responseMessage == null) {
                if (requestResponseFuture.isSendRequestOk()) {
                    throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                        "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
                } else {
                    throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
                }
            }
            return responseMessage;
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final RequestCallback requestCallback, final long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendReqeustOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);

    }

    public Message request(final Message msg, final MessageQueue mq, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendReqeustOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendReqeustOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, null, timeout - cost);

            Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
            if (responseMessage == null) {
                if (requestResponseFuture.isSendRequestOk()) {
                    throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                        "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
                } else {
                    throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
                }
            }
            return responseMessage;
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendReqeustOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, null, timeout - cost);
    }

    private void requestFail(final String correlationId) {
        RequestResponseFuture responseFuture = RequestFutureTable.getRequestFutureTable().remove(correlationId);
        if (responseFuture != null) {
            responseFuture.setSendReqeustOk(false);
            responseFuture.putResponseMessage(null);
            try {
                responseFuture.executeRequestCallback();
            } catch (Exception e) {
                log.warn("execute requestCallback in requestFail, and callback throw", e);
            }
        }
    }

    private void prepareSendRequest(final Message msg, long timeout) {
        String correlationId = CorrelationIdUtil.createCorrelationId();
        String requestClientId = this.getmQClientFactory().getClientId();
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

        boolean hasRouteData = this.getmQClientFactory().getTopicRouteTable().containsKey(msg.getTopic());
        if (!hasRouteData) {
            long beginTimestamp = System.currentTimeMillis();
            this.tryToFindTopicPublishInfo(msg.getTopic());
            this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
            long cost = System.currentTimeMillis() - beginTimestamp;
            if (cost > 500) {
                log.warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
            }
        }
    }

    public ConcurrentMap<String, TopicPublishInfo> getTopicPublishInfoTable() {
        return topicPublishInfoTable;
    }

    public int getZipCompressLevel() {
        return zipCompressLevel;
    }

    public void setZipCompressLevel(int zipCompressLevel) {
        this.zipCompressLevel = zipCompressLevel;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public long[] getNotAvailableDuration() {
        return this.mqFaultStrategy.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.mqFaultStrategy.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.mqFaultStrategy.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.mqFaultStrategy.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }
}

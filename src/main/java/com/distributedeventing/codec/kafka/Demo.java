/*
 * Copyright 2020 The DistributedEventing Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.distributedeventing.codec.kafka;

import com.distributedeventing.codec.kafka.messages.requests.ApiVersionsRequest;
import com.distributedeventing.codec.kafka.messages.requests.DeleteTopicsRequest;
import com.distributedeventing.codec.kafka.messages.requests.RequestMessage;
import com.distributedeventing.codec.kafka.messages.requests.createtopics.CreateTopicsRequest;
import com.distributedeventing.codec.kafka.messages.requests.createtopics.CreateableTopic;
import com.distributedeventing.codec.kafka.messages.requests.fetch.FetchPartition;
import com.distributedeventing.codec.kafka.messages.requests.fetch.FetchRequest;
import com.distributedeventing.codec.kafka.messages.requests.fetch.FetchableTopic;
import com.distributedeventing.codec.kafka.messages.requests.metadata.MetadataRequest;
import com.distributedeventing.codec.kafka.messages.requests.produce.PartitionProduceData;
import com.distributedeventing.codec.kafka.messages.requests.produce.ProduceRequest;
import com.distributedeventing.codec.kafka.messages.requests.produce.TopicProduceData;
import com.distributedeventing.codec.kafka.messages.responses.ResponseMessage;
import com.distributedeventing.codec.kafka.messages.responses.apiversions.ApiVersionsResponse;
import com.distributedeventing.codec.kafka.messages.responses.apiversions.ApiVersionsResponseKey;
import com.distributedeventing.codec.kafka.messages.responses.createtopics.CreatableTopicResult;
import com.distributedeventing.codec.kafka.messages.responses.createtopics.CreateTopicsResponse;
import com.distributedeventing.codec.kafka.messages.responses.deletetopics.DeletableTopicResult;
import com.distributedeventing.codec.kafka.messages.responses.deletetopics.DeleteTopicsResponse;
import com.distributedeventing.codec.kafka.messages.responses.fetch.FetchResponse;
import com.distributedeventing.codec.kafka.messages.responses.fetch.FetchablePartitionResponse;
import com.distributedeventing.codec.kafka.messages.responses.fetch.FetchableTopicResponse;
import com.distributedeventing.codec.kafka.messages.responses.listoffset.ListOffsetResponse;
import com.distributedeventing.codec.kafka.messages.responses.metadata.MetadataResponse;
import com.distributedeventing.codec.kafka.messages.responses.produce.PartitionProduceResponse;
import com.distributedeventing.codec.kafka.messages.responses.produce.ProduceResponse;
import com.distributedeventing.codec.kafka.messages.responses.produce.TopicProduceResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.CompactString;
import com.distributedeventing.codec.kafka.values.Int32;
import com.distributedeventing.codec.kafka.values.NvlBytes;
import com.distributedeventing.codec.kafka.values.records.Record;
import com.distributedeventing.codec.kafka.values.records.RecordBatch;
import com.distributedeventing.codec.kafka.values.records.RecordBatchArray;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Demo of the codec. This requires Kafka running on localhost,
 * listening to port {@link Demo#PORT}.
 */
public class Demo {

    private static final String HOST = "localhost";

    private static final int PORT = 9092;

    private static final byte[] CLIENT_NAME_BYTES = "netty-kafka".getBytes(ByteUtils.CHARSET_UTF8);

    private static final byte[] CLIENT_VERSION_BYTES = "0.1".getBytes(ByteUtils.CHARSET_UTF8);

    private static final byte[] TEST_TOPIC = "codec-demo".getBytes(ByteUtils.CHARSET_UTF8);

    private static final int TEST_TOPIC_PARTITION_ID = 0;

    public static void main(final String[] args) {
        final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        try {
            final Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.TCP_NODELAY, true);
            final TerminalHandler terminalHandler = new TerminalHandler();
            terminalHandler.addListener(new KafkaListener());
            final PipelineInitializer pipelineInitializer = new PipelineIniter(1024 * 10,
                                                                               terminalHandler,
                                                                               CLIENT_NAME_BYTES);
            bootstrap.handler(pipelineInitializer);
            final ChannelFuture channelFuture = bootstrap.connect(HOST, PORT);
            final Channel channel = channelFuture.channel();
            channelFuture.addListener(new ConnectFutureListener());
            channelFuture.sync();
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            eventLoopGroup.shutdownGracefully();
        }
    }

    public static final class KafkaListener implements ResponseListener {

        private final StringBuilder sb = new StringBuilder();

        private final Random random = ThreadLocalRandom.current();

        private UsableApiVersions usableApiVersions;

        @Override
        public void onMetadataResponse(final MetadataResponse response,
                                       final Channel channel) {
            printResponse(response);

            /*
             * Received a metadata response. Let's create a topic now.
             * Since we have only one broker in this demo,  all requests
             * will go there.
             */
            final int createTopicsUsableVersion = usableApiVersions.usableVersion(ApiKey.CREATE_TOPICS).value();
            final CreateTopicsRequest createTopicsRequest = CreateTopicsRequest.getInstance(createTopicsUsableVersion);
            final CreateableTopic topic = createTopicsRequest.createTopic();
            topic.name(TEST_TOPIC);
            topic.numPartitions(1);
            topic.replicationFactor((short) 1);
            createTopicsRequest.timeoutMs(60000);
            printRequest(createTopicsRequest);
            final ChannelFuture future = channel.writeAndFlush(createTopicsRequest);
            future.addListener((ChannelFutureListener) future1 -> {
                if (future1.isSuccess()) {
                    System.out.println("Successfully sent a create topics request");
                } else if (future1.isCancelled()) {
                    System.out.println("Create topics request to Kafka was cancelled");
                } else {
                    System.out.println("Failed to send create topics request");
                    future1.cause().printStackTrace();
                }
            });
        }

        @Override
        public void onApiVersionsResponse(final ApiVersionsResponse response,
                                          final Channel channel) {
            printResponse(response);
            if (closeIfBrokerError(response.errorCode().value(), channel)) {
                return;
            }
            usableApiVersions = getUsableApiVersions(response);
            usableApiVersions.printVersions();

            // We know the api versions to use now
            // We will send a metadata request
            final int metadataRequestUsableVersion = usableApiVersions.usableVersion(ApiKey.METADATA).value();
            final MetadataRequest metadataRequest = MetadataRequest.getInstance(metadataRequestUsableVersion);
            metadataRequest.nullifyTopics(); // get metadata for all available topics
            metadataRequest.allowAutoTopicCreation(false);
            metadataRequest.includeClusterAuthorizedOperations(true);
            metadataRequest.includeTopicAuthorizedOperations(true);
            printRequest(metadataRequest);
            final ChannelFuture metadataRequestFuture = channel.writeAndFlush(metadataRequest);
            metadataRequestFuture.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    System.out.println("Successfully sent metadata request");
                } else if (future.isCancelled()){
                    System.out.println("Metadata request to Kafka was cancelled");
                } else {
                    System.out.println("Failed to send metadata request");
                    future.cause().printStackTrace();
                }
            });
        }

        @Override
        public void onProduceResponse(final ProduceResponse response,
                                      final Channel channel) {
            printResponse(response);
            final Array<TopicProduceResponse>.ElementIterator topicProduceResponses = response.responses();
            while (topicProduceResponses.hasNext()) {
                final TopicProduceResponse topicProduceResponse = topicProduceResponses.next();
                final Array<PartitionProduceResponse>.ElementIterator partitionProduceResponses = topicProduceResponse.partitions();
                while (partitionProduceResponses.hasNext()) {
                    final PartitionProduceResponse partitionProduceResponse = partitionProduceResponses.next();
                    if (closeIfBrokerError(partitionProduceResponse.errorCode().value(), channel)) {
                        return;
                    }
                }
            }
        }

        private void fetchRecords(final Channel channel) {
            /*
             * Ideally we should issue multiple fetch requests to get messages starting with
             * sequence 0 until there are no more messages to fetch. But in this demo, we
             * will only issue a single fetch request with large enough {@link FetchRequest#MAX_BYTES}
             * so that all produced messages are returned in a single fetch response.
             */
            final int usableFetchVersion = usableApiVersions.usableVersion(ApiKey.FETCH).value();
            final FetchRequest fetchRequest = FetchRequest.getInstance(usableFetchVersion);
            fetchRequest.maxBytes(1024 * 1024);
            final FetchableTopic fetchableTopic = fetchRequest.createFetchableTopic();
            fetchableTopic.name(TEST_TOPIC);
            final FetchPartition fetchPartition = fetchableTopic.createFetchPartition();
            fetchPartition.partitionIndex(TEST_TOPIC_PARTITION_ID);
            fetchPartition.fetchOffset(0); // fetch from the beginning
            fetchPartition.maxBytes(1024 * 1024);
            final ChannelFuture requestFuture = channel.writeAndFlush(fetchRequest);
            requestFuture.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    System.out.println("Successfully sent a fetch request");
                } else if (future.isCancelled()) {
                    System.out.println("Fetch request to Kafka was cancelled");
                } else {
                    System.out.println("Failed to send a fetch request");
                    future.cause().printStackTrace();
                }
            });
        }

        @Override
        public void onFetchResponse(final FetchResponse response,
                                    final Channel channel) {
            printResponse(response);
            final short topLevelError = response.errorCode().value();
            if (closeIfBrokerError(topLevelError, channel)) {
                return;
            }
            final Array<FetchableTopicResponse>.ElementIterator topics = response.topics();
            while (topics.hasNext()) {
                final FetchableTopicResponse topic = topics.next();
                final Array<FetchablePartitionResponse>.ElementIterator partitions = topic.partitions();
                while (partitions.hasNext()) {
                    final FetchablePartitionResponse partition = partitions.next();
                    final short partitionError = partition.errorCode().value();
                    if (closeIfBrokerError(partitionError, channel)) {
                        return;
                    }
                    printRecordBatches(partition.records());
                }
            }

            // Demo is over
            // We will clean up after ourselves by deleting the created topic.
            final int deleteTopicsUsableVersion = usableApiVersions.usableVersion(ApiKey.DELETE_TOPICS).value();
            final DeleteTopicsRequest deleteTopicsRequest = DeleteTopicsRequest.getInstance(deleteTopicsUsableVersion);
            final CompactString testTopic = new CompactString();
            testTopic.value(TEST_TOPIC);
            deleteTopicsRequest.topicNames(testTopic);
            deleteTopicsRequest.timeoutMs(60000);
            printRequest(deleteTopicsRequest);
            final ChannelFuture future = channel.writeAndFlush(deleteTopicsRequest);
            future.addListener((ChannelFutureListener) future1 -> {
                if (future1.isSuccess()) {
                    System.out.println("Successfully sent a delete topics request");
                } else if (future1.isCancelled()) {
                    System.out.println("Delete topics request to Kafka was cancelled");
                } else {
                    System.out.println("Failed to send delete topics request");
                    future1.cause().printStackTrace();
                }
            });
        }

        private void printRecordBatches(final RecordBatchArray.ElementIterator batchIterator) {
            while (batchIterator.hasNext()) {
                System.out.println("\tRecord Batch -> ");
                final RecordBatch batch = batchIterator.next();
                Array<Record>.ElementIterator records = batch.records();
                while (records.hasNext()) {
                    printRecord(records.next());
                }
            }
        }

        private void printRecord(final Record record) {
            sb.setLength(0);
            sb.append("\t\tRecord -> ");
            final NvlBytes recordValue = record.value();
            sb.append(new String(recordValue.value(),
                                 0,
                                 recordValue.length(),
                                 ByteUtils.CHARSET_UTF8));
            System.out.println(sb.toString());
        }

        @Override
        public void onCreateTopicsResponse(final CreateTopicsResponse response,
                                           final Channel channel) {
            printResponse(response);
            final Array<CreatableTopicResult>.ElementIterator topicsIterator = response.topics();
            while (topicsIterator.hasNext()) {
                if (closeIfBrokerError(topicsIterator.next().errorCode().value(), channel)) {
                    return;
                }
            }

            // Topic's been created
            // We will send multiple produce requests now
            for (int i=0; i<10; ++i) {
                produceRecords(i, channel);
            }

            // We have produced multiple messages to Kafka
            // Now let's get them back
            fetchRecords(channel);
        }

        private void produceRecords(final int batchNumber,
                                    final Channel channel) {
            final int usableProduceVersion = usableApiVersions.usableVersion(ApiKey.PRODUCE).value();
            final ProduceRequest produceRequest = ProduceRequest.getInstance(usableProduceVersion);
            produceRequest.acks(ProduceRequest.Acks.LEADER_ONLY);
            final TopicProduceData topic = produceRequest.createTopic();
            topic.name(TEST_TOPIC);
            final PartitionProduceData partition = topic.createPartition();
            partition.partitionIndex(TEST_TOPIC_PARTITION_ID);
            final RecordBatch recordBatch = partition.createRecordBatch();
            // Random number of records in the range [1, 10] per batch
            final int numRecord = 1 + random.nextInt(10);
            for (int i=0; i<numRecord; ++i) {
                final Record record = recordBatch.createRecord();
                final byte[] msg = String.format("Message %d, batch %d from netty client at %s",
                                                 i,
                                                 batchNumber,
                                                 new Date().toString()).getBytes(ByteUtils.CHARSET_UTF8);
                record.value(msg);
            }
            printRequest(produceRequest);
            final ChannelFuture future = channel.writeAndFlush(produceRequest);
            future.addListener((ChannelFutureListener) future1 -> {
                if (future1.isSuccess()) {
                    System.out.println("Successfully sent a produce request");
                } else if (future1.isCancelled()) {
                    System.out.println("Produce request to Kafka was cancelled");
                } else {
                    System.out.println("Failed to send produce request");
                    future1.cause().printStackTrace();
                }
            });
        }

        @Override
        public void onDeleteTopicsResponse(final DeleteTopicsResponse response,
                                           final Channel channel) {
            printResponse(response);
            final Array<DeletableTopicResult>.ElementIterator topicResponses = response.responses();
            while (topicResponses.hasNext()) {
                final DeletableTopicResult topicResult = topicResponses.next();
                final BrokerError brokerError = BrokerError.getError(topicResult.errorCode().value());
                if (brokerError != null &&
                    brokerError != BrokerError.NONE &&
                    brokerError != BrokerError.UNKNOWN_TOPIC_OR_PARTITION) {
                    printErrorAndCloseChannels(brokerError, channel);
                    return;
                }
            }

            // Demo is done
            // Shutdown now
            channel.close();
        }

        @Override
        public void onListOffsetsResponse(final ListOffsetResponse response,
                                          final Channel channel) {
            printResponse(response);
        }

        private void printRequest(final RequestMessage request) {
            sb.setLength(0);
            sb.append("Sending the following request: ");
            request.appendTo(sb);
            System.out.println(sb.toString());
        }

        private void printResponse(final ResponseMessage response) {
            sb.setLength(0);
            sb.append("Received the following response: ");
            response.appendTo(sb);
            System.out.println(sb.toString());
        }

        private boolean closeIfBrokerError(final short errorCode,
                                           final Channel channel) {
            final BrokerError brokerError = BrokerError.getError(errorCode);
            if (brokerError != null && brokerError != BrokerError.NONE) {
                printErrorAndCloseChannels(brokerError, channel);
                return true;
            }
            return false;
        }

        private void printErrorAndCloseChannels(final BrokerError brokerError,
                                                final Channel channel) {
            System.out.println("Broker returned the following error in response: " +
                               brokerError.description() + ". Not proceeding any further");
            channel.close();
        }

        private UsableApiVersions getUsableApiVersions(final ApiVersionsResponse response) {
            final UsableApiVersions usableApiVersions = new UsableApiVersions();
            final Array<ApiVersionsResponseKey>.ElementIterator apiKeyIterator = response.apiKeys();
            while (apiKeyIterator.hasNext()) {
                ApiVersionsResponseKey key = apiKeyIterator.next();
                final ApiKey apiKey = ApiKey.getKey(key.apiKey().value());
                if (apiKey == null) {
                    // We don't know about this api yet
                    continue;
                }
                final Int32 ourMax = usableApiVersions.usableVersion(apiKey);
                if (ourMax == null) {
                    // We don't support this api yet
                    continue;
                }
                if (key.minVersion().value() > ourMax.value()) {
                    throw new RuntimeException(String.format("Broker's min version (%d) of API (%s) exceeds our " +
                                                             "maximum version (%d)",
                                                             key.minVersion().value(),
                                                             apiKey.name(),
                                                             ourMax.value()));
                }
                usableApiVersions.brokerMax(apiKey, key.maxVersion().value());
            }
            return usableApiVersions;
        }
    }

    public static final class ConnectFutureListener implements ChannelFutureListener {

        @Override
        public void operationComplete(final ChannelFuture future) {
            if (future.isSuccess()) {
                System.out.println("Sent a connect request to Kafka");
            } else if (future.isCancelled()){
                System.out.println("Connecting to Kafka was cancelled");
            } else {
                System.out.println("Failed to connect to Kafka");
                future.cause().printStackTrace();
            }
        }
    }

    public static final class PipelineIniter extends PipelineInitializer {

        public PipelineIniter(final int maxFrameLength,
                              final TerminalHandler terminalHandler,
                              final byte[] clientId) {
            super(maxFrameLength, terminalHandler, clientId);
        }

        @Override
        protected void initChannel(final SocketChannel channel) throws Exception {
            super.initChannel(channel);
            channel.pipeline().addLast(new ConnectionListener());
        }
    }

    public static final class ConnectionListener extends ChannelInboundHandlerAdapter {

        private final StringBuilder sb = new StringBuilder();

        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            System.out.println("Successfully connected to Kafka");
            // Connected to broker
            // We will send an API versions request now
            sendApiVersionsRequest(ctx.channel());
            ctx.fireChannelActive();
        }

        private void sendApiVersionsRequest(final Channel channel) {
            final ApiVersionsRequest apiVersionsRequest = ApiVersionsRequest.getInstance(3);
            apiVersionsRequest.clientSoftwareName(CLIENT_NAME_BYTES);
            apiVersionsRequest.clientSoftwareVersion(CLIENT_VERSION_BYTES);
            sb.setLength(0);
            sb.append("Sending the following request: ");
            apiVersionsRequest.appendTo(sb);
            System.out.println(sb.toString());
            final ChannelFuture versionsRequestFuture = channel.writeAndFlush(apiVersionsRequest);
            versionsRequestFuture.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    System.out.println("Successfully sent an api versions request");
                } else if (future.isCancelled()) {
                    System.out.println("Api versions request to Kafka was cancelled");
                } else {
                    System.out.println("Failed to send api versions request");
                    future.cause().printStackTrace();
                }
            });
        }
    }

    public static final class UsableApiVersions {

        private final Map<ApiKey, Int32> keyToVersion = new HashMap<>();

        public UsableApiVersions() {
            // initialize usable api versions with the max that we support
            keyToVersion.put(ApiKey.CREATE_TOPICS, new Int32(MaxSupportedApiVersions.CREATE_TOPICS));
            keyToVersion.put(ApiKey.FETCH, new Int32(MaxSupportedApiVersions.FETCH));
            keyToVersion.put(ApiKey.LIST_OFFSETS, new Int32(MaxSupportedApiVersions.LIST_OFFSET));
            keyToVersion.put(ApiKey.METADATA, new Int32(MaxSupportedApiVersions.METADATA));
            keyToVersion.put(ApiKey.PRODUCE, new Int32(MaxSupportedApiVersions.PRODUCE));
            keyToVersion.put(ApiKey.API_VERSIONS, new Int32(MaxSupportedApiVersions.API_VERSIONS));
            keyToVersion.put(ApiKey.DELETE_TOPICS, new Int32(MaxSupportedApiVersions.DELETE_TOPICS));
        }

        public void brokerMax(final ApiKey key,
                              final int version) {
            // update usable api versions with the minimum of ours and brokers
            final Int32 maxVersion = keyToVersion.get(key);
            if (maxVersion == null) {
                // We don't support this api yet
                return;
            }
            maxVersion.value(Math.min(maxVersion.value(),
                                      version));
        }

        public Int32 usableVersion(final ApiKey key) {
            return keyToVersion.get(key);
        }

        public void printVersions() {
            System.out.println("Usable api versions: ");
            for (Map.Entry<ApiKey, Int32> entry: keyToVersion.entrySet()) {
                System.out.println(String.format("\t%s -> %d",
                                                 entry.getKey().name(),
                                                 entry.getValue().value()));
            }
        }
    }

    public static final class MaxSupportedApiVersions {

        public static final int CREATE_TOPICS = 5;

        public static final int FETCH = 11;

        public static final int LIST_OFFSET = 5;

        public static final int METADATA = 9;

        public static final int PRODUCE = 8;

        public static final int API_VERSIONS = 3;

        public static final int DELETE_TOPICS = 4;
    }
}
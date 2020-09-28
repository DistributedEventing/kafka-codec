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
package com.distributedeventing.codec.kafka.messages.responses.metadata;

import com.distributedeventing.codec.kafka.BrokerError;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;

public final class MetadataResponsePartition extends CompositeValue {

    /**
     * The error code associated with the topic partition. Will be
     * {@link BrokerError#NONE} if no error
     * occurred.
     */
    public static final Field ERROR_CODE = new Field("error_code");

    /** Index of the partition **/
    public static final Field PARTITION_INDEX = new Field("partition_index");

    /** Broker ID of the leader of the topic parition **/
    public static final Field LEADER_ID = new Field("leader_id");

    /** The epoch of the leader broker **/
    public static final Field LEADER_EPOCH = new Field("leader_epoch");

    /** All replicas of this partition **/
    public static final Field REPLICA_NODES = new Field("replica_nodes");

    /** The replicas of the partition that are in-sync with the leader **/
    public static final Field ISR_NODES = new Field("isr_nodes");

    /** Offline replicas of this partition **/
    public static final Field OFFLINE_REPLICAS = new Field("offline_replicas");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V9 = new Schema(ERROR_CODE,
                                                      PARTITION_INDEX,
                                                      LEADER_ID,
                                                      LEADER_EPOCH,
                                                      REPLICA_NODES,
                                                      ISR_NODES,
                                                      OFFLINE_REPLICAS,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V9};

    private final Int16 errorcode = new Int16();

    private final Int32 partitionIndex = new Int32();

    private final Int32 leaderId = new Int32();

    private final Int32 leaderEpoch = new Int32();

    private final CompactArray<Int32> replicaNodes = new CompactArray<>(Int32.FACTORY);

    private final CompactArray<Int32> isrNodes = new CompactArray<>(Int32.FACTORY);

    private final CompactArray<Int32> offlineReplicas = new CompactArray<>(Int32.FACTORY);

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static MetadataResponsePartition getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new MetadataResponsePartition(SCHEMAS[version]);
    }

    public Int16 errorcode() {
        return errorcode;
    }

    public Int32 partitionIndex() {
        return partitionIndex;
    }

    public Int32 leaderId() {
        return leaderId;
    }

    public Int32 leaderEpoch() {
        return leaderEpoch;
    }

    public Array<Int32>.ElementIterator replicaNodes() {
        return replicaNodes.iterator();
    }

    public Array<Int32>.ElementIterator isrNodes() {
        return isrNodes.iterator();
    }

    public Array<Int32>.ElementIterator offlineReplicas() {
        return offlineReplicas.iterator();
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(ERROR_CODE, errorcode);
        fieldValueBindings.put(PARTITION_INDEX, partitionIndex);
        fieldValueBindings.put(LEADER_ID, leaderId);
        fieldValueBindings.put(LEADER_EPOCH, leaderEpoch);
        fieldValueBindings.put(REPLICA_NODES, replicaNodes);
        fieldValueBindings.put(ISR_NODES, isrNodes);
        fieldValueBindings.put(OFFLINE_REPLICAS, offlineReplicas);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private MetadataResponsePartition(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class MetadataResponsePartitionFactory implements ValueFactory<MetadataResponsePartition> {

        private final short version;

        public MetadataResponsePartitionFactory(final short version) {
            this.version = version;
        }

        @Override
        public MetadataResponsePartition createInstance() {
            return MetadataResponsePartition.getInstance(version);
        }
    }
}
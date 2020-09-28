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
package com.distributedeventing.codec.kafka.values.records;

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.values.*;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.Field;

import java.util.zip.Checksum;
import java.util.zip.CRC32C;

public final class RecordBatch extends CompositeValue {

    static {
        final java.lang.String javaVersion = System.getProperty("java.version");
        final int major = Integer.parseInt(javaVersion.split("\\.")[0]);
        if (major < 9) {
            throw new UnsupportedVersionException(java.lang.String.format("Java version %s is not supported, need at " +
                                                                          "least version 9",
                                                                          javaVersion));
        }
    }

    public static RecordBatchFactory FACTORY = new RecordBatchFactory();

    /**
     * Offset of the first record in the batch. The offset delta of each record
     * in the batch must be computed relative to this. Codec takes care of it
     * as records are created in the batch.
     */
    public static final Field BASE_OFFSET = new Field("base_offset");

    /**
     * The number of bytes in the record batch starting at {@link RecordBatch#PARTITION_LEADER_EPOCH}
     * till the end of the batch. This is calculated by the codec while serializing a record batch.
     */
    public static final Field LENGTH = new Field("length");

    /**
     * Epoch of the leader of the partition. Clients of Kafka need not worry about
     * setting this value.
     */
    public static final Field PARTITION_LEADER_EPOCH = new Field("partition_leader_epoch");

    /**
     * Version of the record batch. Only version {@link Magic#V2} is presently supported
     * and will be set by the codec.
     */
    public static final Field MAGIC = new Field("magic");

    /**
     * CRC of data from attributes till the end of the batch. This is calculated by the codec
     * when serializing a record batch.
     */
    public static final Field CRC = new Field("crc");

    /**
     * Attributes of the record batch.
     *
     * Bits and their usages:
     * <ol>
     *     <li>
     *         0-2: specify the compression type of the record batch. The possible values
     *              are.
     *              <ul>
     *                  <li>0: no compression</li>
     *                  <li>1: GZIP compression</li>
     *                  <li>2: Snappy compression</li>
     *              </ul>
     *     </li>
     *     <li>
     *         3: timestamp type. 0 = create time, 1 = log append time. Producers must
     *            always set this to 0.
     *     </li>
     *     <li>
     *         4: whether the record batch is a part of a transaction. 0 = record batch is
     *            not transactional. 1 = record batch is transactional.
     *     </li>
     *     <li>
     *         5: whether the record batch includes a control message. 0 = batch contains no
     *            control message. 1 = batch contains control message. Only brokers produce
     *            control messages. As such clients of Kafka must not set this.
     *     </li>
     *     <li>6-15: unused</li>
     * </ol>
     *
     * A client that does not desire compression or transaction support must set this to 0
     * (which is the default).
     */
    public static final Field ATTRIBUTES = new Field("attributes");

    /**
     * The offset delta (with respect to {@link RecordBatch#BASE_OFFSET}) of the
     * last record in the batch. This is set by the codec when serailizing a record
     * batch.
     */
    public static final Field LAST_OFFSET_DELTA = new Field("last_offset_delta");

    /**
     * Timestamp of the first record in the batch. The timestamp of each record in
     * the batch is it's timestamp delta + first timestamp.
     *
     * The record timestamp stored by the brokers depends upon whether they have been
     * configured to use create timestamp or log append timestamp.
     *
     * This is set by the codec when the first record is created in a batch.
     */
    public static final Field FIRST_TIMESTAMP = new Field("first_timestamp");

    /**
     * Timestamp of the last record in the batch
     *
     * The record timestamp stored by the brokers depends upon whether they have been
     * configured to use create timestamp or log append timestamp.
     *
     * This is set by the codec.
     */
    public static final Field MAX_TIMESTAMP = new Field("max_timestamp");

    /**
     * The PID of the producer if the producer is transactional. Otherwise, this
     * should be set to {@link RecordBatch#NO_PRODUCER_ID}.
     *
     * This defaults to {@link RecordBatch#NO_PRODUCER_ID}.
     */
    public static final Field PRODUCER_ID = new Field("producer_id");

    /**
     * The epoch of the producer if it is transactional. Otherwise, this field
     * should be set to {@link RecordBatch#NO_PRODUCER_EPOCH}.
     *
     * This defaults to {@link RecordBatch#NO_PRODUCER_EPOCH}.
     */
    public static final Field PRODUCER_EPOCH = new Field("producer_epoch");

    /**
     * The sequence number of the first record in the batch if idempotency is desired.
     * The sequence number of each record in the batch is it's offset delta + base
     * sequence. If idempotency is not desired, this should be set to
     * {@link RecordBatch#NO_SEQUENCE}.
     *
     * This defaults to {@link RecordBatch#NO_SEQUENCE}.
     */
    public static final Field BASE_SEQUENCE = new Field("base_sequence");

    /** The records to be produced **/
    public static final Field RECORDS = new Field("records");

    public static final Schema SCHEMA = new Schema(BASE_OFFSET,
                                                   LENGTH,
                                                   PARTITION_LEADER_EPOCH,
                                                   MAGIC,
                                                   CRC,
                                                   ATTRIBUTES,
                                                   LAST_OFFSET_DELTA,
                                                   FIRST_TIMESTAMP,
                                                   MAX_TIMESTAMP,
                                                   PRODUCER_ID,
                                                   PRODUCER_EPOCH,
                                                   BASE_SEQUENCE,
                                                   RECORDS);

    public static final int NO_PRODUCER_ID = -1;

    public static final short NO_PRODUCER_EPOCH = -1;

    public static final int NO_SEQUENCE = -1;

    public static final int NO_PARTITION_LEADER_EPOCH = -1;

    private final Int64 baseOffset = new Int64();

    private final Int32 length = new Int32();

    private final Int32 partitionLeaderEpoch = new Int32();

    private final Int8 magic = new Int8();

    private final UnsignedInt32 crc = new UnsignedInt32();

    private final Int16 attributes = new Int16();

    private final Int32 lastOffsetDelta = new Int32();

    private final Int64 firstTimestamp = new Int64();

    private final Int64 maxTimestamp = new Int64();

    private final Int64 producerId = new Int64();

    private final Int16 producerEpoch = new Int16();

    private final Int32 baseSequence = new Int32();

    private final Record.RecordFactory recordFactory = Record.FACTORY;

    private final Array<Record> records = new Array<Record>(recordFactory);

    private final Checksum checksum = new CRC32C();

    public RecordBatch() {
        super(SCHEMA);
        setFieldValueBindings();
        applyDefaults();
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        baseOffset.writeTo(buffer);
        final int lengthIndex = buffer.writerIndex();
        buffer.writerIndex(lengthIndex + 4);
        partitionLeaderEpoch.writeTo(buffer);
        magic.writeTo(buffer);
        final int crcOffset = buffer.writerIndex();
        final int attributesOffset = crcOffset + 4;
        buffer.writerIndex(attributesOffset);
        attributes.writeTo(buffer);
        lastOffsetDelta.value(records.length() - 1);
        lastOffsetDelta.writeTo(buffer);
        firstTimestamp.writeTo(buffer);
        maxTimestamp.writeTo(buffer);
        producerId.writeTo(buffer);
        producerEpoch.writeTo(buffer);
        baseSequence.writeTo(buffer);
        records.writeTo(buffer);
        final int length = buffer.writerIndex() - lengthIndex - 4;
        buffer.setInt(lengthIndex, length);
        final long crc = computeChecksum(buffer, attributesOffset, buffer.writerIndex() - attributesOffset);
        ByteUtils.writeUnsignedInt(crc, crcOffset, buffer);
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        sb.append("RecordBatch{");
        super.appendTo(sb);
        sb.append("}");
        return sb;
    }

    public Int64 baseOffset() {
        return baseOffset;
    }

    public void baseOffset(final long offset) {
        baseOffset.value(offset);
    }

    public Int32 length() {
        return length;
    }

    public void length(final int length) {
        this.length.value(length);
    }

    public Int32 partitionLeaderEpoch() {
        return partitionLeaderEpoch;
    }

    public void partitionLeaderEpoch(final int epoch) {
        partitionLeaderEpoch.value(epoch);
    }

    public Int8 magic() {
        return magic;
    }

    public void magic(final Magic magic) {
        this.magic.value(magic.value);
    }

    public UnsignedInt32 crc() {
        return crc;
    }

    public void crc(final int crc) {
        this.crc.value(crc);
    }

    public Int16 attributes() {
        return attributes;
    }

    public void attributes(final short attrs) {
        attributes.value(attrs);
    }

    public Int32 lastOffsetDelta() {
        return lastOffsetDelta;
    }

    public void lastOffsetDelta(final int delta) {
        lastOffsetDelta.value(delta);
    }

    public Int64 firstTimestamp() {
        return firstTimestamp;
    }

    public void firstTimestamp(final long timestamp) {
        firstTimestamp.value(timestamp);
    }

    public Int64 maxTimestamp() {
        return maxTimestamp;
    }

    public void maxTimestamp(final long timestamp) {
        maxTimestamp.value(timestamp);
    }

    public Int64 producerId() {
        return producerId;
    }

    public void producerId(final long id) {
        producerId.value(id);
    }

    public Int16 producerEpoch() {
        return producerEpoch;
    }

    public void producerEpoch(final short epoch) {
        producerEpoch.value(epoch);
    }

    public Int32 baseSequence() {
        return baseSequence;
    }

    public void setBaseSequence(final int sequence) {
        baseSequence.value(sequence);
    }

    public Array<Record>.ElementIterator records() {
        return records.iterator();
    }

    public Record createRecord() {
        final Record record = recordFactory.createInstance();
        record.offsetDelta(records.length());
        final long currentTimestamp = System.currentTimeMillis();
        if (records.length() == 0) {
            firstTimestamp.value(currentTimestamp);
        } else {
            record.timestampDelta(currentTimestamp - firstTimestamp.value());
        }
        records.add(record);
        maxTimestamp.value(currentTimestamp);
        return record;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(BASE_OFFSET, baseOffset);
        fieldValueBindings.put(LENGTH, length);
        fieldValueBindings.put(PARTITION_LEADER_EPOCH, partitionLeaderEpoch);
        fieldValueBindings.put(MAGIC, magic);
        fieldValueBindings.put(CRC, crc);
        fieldValueBindings.put(ATTRIBUTES, attributes);
        fieldValueBindings.put(LAST_OFFSET_DELTA, lastOffsetDelta);
        fieldValueBindings.put(FIRST_TIMESTAMP, firstTimestamp);
        fieldValueBindings.put(MAX_TIMESTAMP, maxTimestamp);
        fieldValueBindings.put(PRODUCER_ID, producerId);
        fieldValueBindings.put(PRODUCER_EPOCH, producerEpoch);
        fieldValueBindings.put(BASE_SEQUENCE, baseSequence);
        fieldValueBindings.put(RECORDS, records);
    }

    private void applyDefaults() {
        partitionLeaderEpoch.value(NO_PARTITION_LEADER_EPOCH); // clients don't supply partition leader epoch
        magic.value(Magic.V2.value);
        producerId.value(NO_PRODUCER_ID);  // non transactional producer
        producerEpoch.value(NO_PRODUCER_EPOCH); // non transactional producer
        baseSequence.value(NO_SEQUENCE); // non idempotent producer
    }

    private long computeChecksum(final ByteBuf buffer,
                                 final int offset,
                                 final int length) {
        checksum.reset();
        if (buffer.hasArray()) {
            checksum.update(buffer.array(), buffer.arrayOffset() + offset, length);
        } else {
            for (int i=0; i<length; ++i) {
                checksum.update(buffer.getByte(offset + i));
            }
        }
        return checksum.getValue();
    }

    public static final class RecordBatchFactory implements ValueFactory<RecordBatch> {

        @Override
        public RecordBatch createInstance() {
            return new RecordBatch();
        }

        private RecordBatchFactory() {}
    }

    public static enum Magic {

        V0(0),
        V1(1),
        V2(2);

        private final byte value;

        private Magic(final int value) {
            this.value = (byte) value;
        }
    }
}
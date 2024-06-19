/*
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserve.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.baidu.fsg.uid;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.springframework.util.Assert;

/**
 * Allocate 64 bits for the UID(long)<br>
 * sign (fixed 1bit) -> deltaSecond -> workerId -> sequence(within the same second)
 * 
 * @author yutianbao
 */
public class BitsAllocator {
    /**
     * Total 64 bits
     */
    public static final int TOTAL_BITS = 1 << 6;

    /**
     * Bits for [sign-> second-> workId-> sequence]
     */
    /** 标志位数 */
    private int signBits = 1;
    /** 时间戳位数 */
    private final int timestampBits;
    /** workerId位数 */
    private final int workerIdBits;
    /** 1秒内的自增序号位数 */
    private final int sequenceBits;

    /**
     * Max value for workId & sequence
     */
    /** 时间戳位数能够表示的最大秒数 */
    private final long maxDeltaSeconds;
    /** workerId位数能够表示的最大重启次数 */
    private final long maxWorkerId;
    /** 1秒内的自增序号位数能够表示的最大ID数量 */
    private final long maxSequence;

    /**
     * Shift for timestamp & workerId
     */
    /** 时间戳左移位数 */
    private final int timestampShift;
    /** workerId左移位数 */
    private final int workerIdShift;

    /**
     * Constructor with timestampBits, workerIdBits, sequenceBits<br>
     * The highest bit used for sign, so <code>63</code> bits for timestampBits, workerIdBits, sequenceBits
     */
    public BitsAllocator(int timestampBits, int workerIdBits, int sequenceBits) {
        // make sure allocated 64 bits
        int allocateTotalBits = signBits + timestampBits + workerIdBits + sequenceBits;
        Assert.isTrue(allocateTotalBits == TOTAL_BITS, "allocate not enough 64 bits");

        // initialize bits
        this.timestampBits = timestampBits;
        this.workerIdBits = workerIdBits;
        this.sequenceBits = sequenceBits;

        // initialize max value
        this.maxDeltaSeconds = ~(-1L << timestampBits);
        this.maxWorkerId = ~(-1L << workerIdBits);
        this.maxSequence = ~(-1L << sequenceBits);

        // initialize shift
        this.timestampShift = workerIdBits + sequenceBits;
        this.workerIdShift = sequenceBits;
    }

    /**
     * Allocate bits for UID according to delta seconds & workerId & sequence<br>
     * <b>Note that: </b>The highest bit will always be 0 for sign
     * 
     * @param deltaSeconds
     * @param workerId
     * @param sequence
     * @return
     */
    public long allocate(long deltaSeconds, long workerId, long sequence) {
        return (deltaSeconds << timestampShift) | (workerId << workerIdShift) | sequence;
    }
    
    /**
     * Getters
     */
    public int getSignBits() {
        return signBits;
    }

    public int getTimestampBits() {
        return timestampBits;
    }

    public int getWorkerIdBits() {
        return workerIdBits;
    }

    public int getSequenceBits() {
        return sequenceBits;
    }

    public long getMaxDeltaSeconds() {
        return maxDeltaSeconds;
    }

    public long getMaxWorkerId() {
        return maxWorkerId;
    }

    public long getMaxSequence() {
        return maxSequence;
    }

    public int getTimestampShift() {
        return timestampShift;
    }

    public int getWorkerIdShift() {
        return workerIdShift;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
    
}
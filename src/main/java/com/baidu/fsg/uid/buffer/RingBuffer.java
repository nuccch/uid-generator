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
package com.baidu.fsg.uid.buffer;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.baidu.fsg.uid.utils.PaddedAtomicLong;

/**
 * Represents a ring buffer based on array.<br>
 * Using array could improve read element performance due to the CUP cache line. To prevent 
 * the side effect of False Sharing, {@link PaddedAtomicLong} is using on 'tail' and 'cursor'<p>
 * 
 * A ring buffer is consisted of:
 * <li><b>slots:</b> each element of the array is a slot, which is be set with a UID
 * <li><b>flags:</b> flag array corresponding the same index with the slots, indicates whether can take or put slot
 * <li><b>tail:</b> a sequence of the max slot position to produce 
 * <li><b>cursor:</b> a sequence of the min slot position to consume
 * 
 * @author yutianbao
 */
public class RingBuffer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RingBuffer.class);

    /** Constants */
    private static final int START_POINT = -1;
    private static final long CAN_PUT_FLAG = 0L;
    private static final long CAN_TAKE_FLAG = 1L;
    /** 默认填充比例 */
    public static final int DEFAULT_PADDING_PERCENT = 50;

    /** The size of RingBuffer's slots, each slot hold a UID */
    private final int bufferSize;
    /** 用来使用与运算方式计算槽位索引，值=槽大小-1 */
    /** 参考：<br />
     * https://blog.csdn.net/nlcexiyue/article/details/109327573 取模和与运算的一点关系 <br />
     * https://blog.csdn.net/epwqgdnbrh/article/details/105255652  取模运算%和按位与&
     */
    private final long indexMask;
    /** 真正存放ID的槽 */
    private final long[] slots;
    /** 存放槽位标记的数组，大小与存放ID的槽一致 */
    private final PaddedAtomicLong[] flags;

    /** Tail: last position sequence to produce */
    private final AtomicLong tail = new PaddedAtomicLong(START_POINT);

    /** Cursor: current position sequence to consume */
    private final AtomicLong cursor = new PaddedAtomicLong(START_POINT);

    /** Threshold for trigger padding buffer*/
    private final int paddingThreshold; 
    
    /** Reject put/take buffer handle policy */
    private RejectedPutBufferHandler rejectedPutHandler = this::discardPutBuffer;
    private RejectedTakeBufferHandler rejectedTakeHandler = this::exceptionRejectedTakeBuffer; 
    
    /** Executor of padding buffer */
    private BufferPaddingExecutor bufferPaddingExecutor;

    /**
     * Constructor with buffer size, paddingFactor default as {@value #DEFAULT_PADDING_PERCENT}
     * 
     * @param bufferSize must be positive & a power of 2
     */
    public RingBuffer(int bufferSize) {
        this(bufferSize, DEFAULT_PADDING_PERCENT);
    }
    
    /**
     * Constructor with buffer size & padding factor
     * 
     * @param bufferSize must be positive & a power of 2
     * @param paddingFactor percent in (0 - 100). When the count of rest available UIDs reach the threshold, it will trigger padding buffer<br>
     *        Sample: paddingFactor=20, bufferSize=1000 -> threshold=1000 * 20 /100,  
     *        padding buffer will be triggered when tail-cursor<threshold
     */
    public RingBuffer(int bufferSize, int paddingFactor) {
        // check buffer size is positive & a power of 2; padding factor in (0, 100)
        Assert.isTrue(bufferSize > 0L, "RingBuffer size must be positive");
        Assert.isTrue(Integer.bitCount(bufferSize) == 1, "RingBuffer size must be a power of 2");
        Assert.isTrue(paddingFactor > 0 && paddingFactor < 100, "RingBuffer size must be positive");

        this.bufferSize = bufferSize;
        this.indexMask = bufferSize - 1;
        this.slots = new long[bufferSize];
        this.flags = initFlags(bufferSize);
        
        this.paddingThreshold = bufferSize * paddingFactor / 100;
    }

    /**
     * Put an UID in the ring & tail moved<br>
     * We use 'synchronized' to guarantee the UID fill in slot & publish new tail sequence as atomic operations<br>
     * 
     * <b>Note that: </b> It is recommended to put UID in a serialize way, cause we once batch generate a series UIDs and put
     * the one by one into the buffer, so it is unnecessary put in multi-threads
     *
     * @param uid
     * @return false means that the buffer is full, apply {@link RejectedPutBufferHandler}
     */
    public synchronized boolean put(long uid) {
        long currentTail = tail.get();
        long currentCursor = cursor.get();

        // tail catches the cursor, means that you can't put any cause of RingBuffer is full
        long distance = currentTail - (currentCursor == START_POINT ? 0 : currentCursor);
        if (distance == bufferSize - 1) {
            // 槽已满
            rejectedPutHandler.rejectPutBuffer(this, uid);
            return false;
        }

        // 先计算槽位索引，再检查该槽位是否可以存值
        // 1. pre-check whether the flag is CAN_PUT_FLAG
        int nextTailIndex = calSlotIndex(currentTail + 1);
        if (flags[nextTailIndex].get() != CAN_PUT_FLAG) {
            // 槽位不能存放值
            rejectedPutHandler.rejectPutBuffer(this, uid);
            return false;
        }

        // 2. put UID in the next slot
        // 3. update next slot' flag to CAN_TAKE_FLAG
        // 4. publish tail with sequence increase by one
        slots[nextTailIndex] = uid;
        flags[nextTailIndex].set(CAN_TAKE_FLAG); // 槽位上存入ID之后设置标记为可取，这样就不能往该槽位上再存放新的ID了
        tail.incrementAndGet();

        // The atomicity of operations above, guarantees by 'synchronized'. In another word,
        // the take operation can't consume the UID we just put, until the tail is published(tail.incrementAndGet())
        return true;
    }

    /**
     * Take an UID of the ring at the next cursor, this is a lock free operation by using atomic cursor<p>
     * 
     * Before getting the UID, we also check whether reach the padding threshold, 
     * the padding buffer operation will be triggered in another thread<br>
     * If there is no more available UID to be taken, the specified {@link RejectedTakeBufferHandler} will be applied<br>
     * 
     * @return UID
     * @throws IllegalStateException if the cursor moved back
     */
    public long take() {
        // spin get next available cursor
        long currentCursor = cursor.get();
        long nextCursor = cursor.updateAndGet(old -> old == tail.get() ? old : old + 1);

        // check for safety consideration, it never occurs
        Assert.isTrue(nextCursor >= currentCursor, "Cursor can't move back");

        // trigger padding in an async-mode if reach the threshold
        long currentTail = tail.get();
        if (currentTail - nextCursor < paddingThreshold) { // 当前填充的位置与下一个获取的位置距离小于了填充阈值，开始异步填充
            LOGGER.info("Reach the padding threshold:{}. tail:{}, cursor:{}, rest:{}", paddingThreshold, currentTail,
                    nextCursor, currentTail - nextCursor);
            bufferPaddingExecutor.asyncPadding();
        }

        // cursor catch the tail, means that there is no more available UID to take
        if (nextCursor == currentCursor) { // 槽已经空了，不能再取
            rejectedTakeHandler.rejectTakeBuffer(this);
        }

        // 1. check next slot flag is CAN_TAKE_FLAG
        int nextCursorIndex = calSlotIndex(nextCursor);
        Assert.isTrue(flags[nextCursorIndex].get() == CAN_TAKE_FLAG, "Curosr not in can take status");

        // 2. get UID from next slot
        // 3. set next slot flag as CAN_PUT_FLAG.
        long uid = slots[nextCursorIndex];
        flags[nextCursorIndex].set(CAN_PUT_FLAG); // 取走槽位的ID值之后，需要将该槽位标志设置位可存放，否则无法在该槽位上继续存放新的ID

        // Note that: Step 2,3 can not swap. If we set flag before get value of slot, the producer may overwrite the
        // slot with a new UID, and this may cause the consumer take the UID twice after walk a round the ring
        return uid;
    }

    /**
     * Calculate slot index with the slot sequence (sequence % bufferSize) 
     */
    protected int calSlotIndex(long sequence) {
        return (int) (sequence & indexMask);
    }

    /**
     * Discard policy for {@link RejectedPutBufferHandler}, we just do logging
     */
    protected void discardPutBuffer(RingBuffer ringBuffer, long uid) {
        LOGGER.warn("Rejected putting buffer for uid:{}. {}", uid, ringBuffer);
    }
    
    /**
     * Policy for {@link RejectedTakeBufferHandler}, throws {@link RuntimeException} after logging 
     */
    protected void exceptionRejectedTakeBuffer(RingBuffer ringBuffer) {
        LOGGER.warn("Rejected take buffer. {}", ringBuffer);
        throw new RuntimeException("Rejected take buffer. " + ringBuffer);
    }
    
    /**
     * Initialize flags as CAN_PUT_FLAG
     */
    private PaddedAtomicLong[] initFlags(int bufferSize) {
        PaddedAtomicLong[] flags = new PaddedAtomicLong[bufferSize];
        for (int i = 0; i < bufferSize; i++) {
            flags[i] = new PaddedAtomicLong(CAN_PUT_FLAG);
        }
        
        return flags;
    }

    /**
     * Getters
     */
    public long getTail() {
        return tail.get();
    }

    public long getCursor() {
        return cursor.get();
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Setters
     */
    public void setBufferPaddingExecutor(BufferPaddingExecutor bufferPaddingExecutor) {
        this.bufferPaddingExecutor = bufferPaddingExecutor;
    }

    public void setRejectedPutHandler(RejectedPutBufferHandler rejectedPutHandler) {
        this.rejectedPutHandler = rejectedPutHandler;
    }

    public void setRejectedTakeHandler(RejectedTakeBufferHandler rejectedTakeHandler) {
        this.rejectedTakeHandler = rejectedTakeHandler;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("RingBuffer [bufferSize=").append(bufferSize)
               .append(", tail=").append(tail)
               .append(", cursor=").append(cursor)
               .append(", paddingThreshold=").append(paddingThreshold).append("]");
        
        return builder.toString();
    }

}

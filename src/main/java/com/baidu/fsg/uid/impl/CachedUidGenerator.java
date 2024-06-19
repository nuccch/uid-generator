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
package com.baidu.fsg.uid.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import com.baidu.fsg.uid.BitsAllocator;
import com.baidu.fsg.uid.UidGenerator;
import com.baidu.fsg.uid.buffer.BufferPaddingExecutor;
import com.baidu.fsg.uid.buffer.RejectedPutBufferHandler;
import com.baidu.fsg.uid.buffer.RejectedTakeBufferHandler;
import com.baidu.fsg.uid.buffer.RingBuffer;
import com.baidu.fsg.uid.exception.UidGenerateException;

/**
 * 1.通过“借用未来时间”的方式巧妙地避免了雪花算法中的时钟回拨问题 <br />
 * 这里的“借用未来时间”是指：在计算“下一秒”可产生的ID列表时，“下一秒”是直接在当前秒的基础上通过累加的方式得到的，没有去获取系统时间。 <br />
 *
 * 2.为什么可以“借用未来时间”呢？ <br />
 * 因为ID是提前在内存中计算出来的，所以再计算“下一秒”内可生成的ID列表时不能直接获取当前的系统时间，只能在当期时间的基础上直接累加计算来获取“下一秒”时间戳。 <br />
 *
 * 3.该方式生成ID效率高的原因是：在指定时间秒内生成的ID都是提前计算的，每次获取的时候直接从内存取值，因此效率极高。 <br />
 *
 * 4.如果在独立服务中使用该方式生成ID可能存在业务容量易被猜测到的风险（原因：独立服务通常不会总是重启，因此生成的ID是连续的）。 <br />
 *
 * Represents a cached implementation of {@link UidGenerator} extends
 * from {@link DefaultUidGenerator}, based on a lock free {@link RingBuffer}<p>
 * 
 * The spring properties you can specify as below:<br>
 * <li><b>boostPower:</b> RingBuffer size boost for a power of 2, Sample: boostPower is 3, it means the buffer size 
 *                        will be <code>({@link BitsAllocator#getMaxSequence()} + 1) &lt;&lt;
 *                        {@link #boostPower}</code>, Default as {@value #DEFAULT_BOOST_POWER}
 * <li><b>paddingFactor:</b> Represents a percent value of (0 - 100). When the count of rest available UIDs reach the 
 *                           threshold, it will trigger padding buffer. Default as{@link RingBuffer#DEFAULT_PADDING_PERCENT}
 *                           Sample: paddingFactor=20, bufferSize=1000 -> threshold=1000 * 20 /100, padding buffer will be triggered when tail-cursor<threshold
 * <li><b>scheduleInterval:</b> Padding buffer in a schedule, specify padding buffer interval, Unit as second
 * <li><b>rejectedPutBufferHandler:</b> Policy for rejected put buffer. Default as discard put request, just do logging
 * <li><b>rejectedTakeBufferHandler:</b> Policy for rejected take buffer. Default as throwing up an exception
 * 
 * @author yutianbao
 */
public class CachedUidGenerator extends DefaultUidGenerator implements DisposableBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(CachedUidGenerator.class);
    private static final int DEFAULT_BOOST_POWER = 3;

    /** Spring properties */
    private int boostPower = DEFAULT_BOOST_POWER;
    /** 填充比例 */
    private int paddingFactor = RingBuffer.DEFAULT_PADDING_PERCENT;
    /** 定时填充的时间间隔，单位：秒 */
    private Long scheduleInterval;

    /** 拒绝放入ID处理器 */
    private RejectedPutBufferHandler rejectedPutBufferHandler;
    /** 拒绝获取ID处理器 */
    private RejectedTakeBufferHandler rejectedTakeBufferHandler;

    /** RingBuffer */
    private RingBuffer ringBuffer;
    /** 环形数字填充执行器 */
    private BufferPaddingExecutor bufferPaddingExecutor;

    @Override
    public void afterPropertiesSet() throws Exception {
        // initialize workerId & bitsAllocator
        super.afterPropertiesSet();
        
        // initialize RingBuffer & RingBufferPaddingExecutor
        this.initRingBuffer();
        LOGGER.info("Initialized RingBuffer successfully.");
    }
    
    @Override
    public long getUID() {
        try {
            return ringBuffer.take();
        } catch (Exception e) {
            LOGGER.error("Generate unique id exception. ", e);
            throw new UidGenerateException(e);
        }
    }

    @Override
    public String parseUID(long uid) {
        return super.parseUID(uid);
    }
    
    @Override
    public void destroy() throws Exception {
        bufferPaddingExecutor.shutdown();
    }

    /**
     * Get the UIDs in the same specified second under the max sequence
     * 一次性生成指定时间秒内可产生的ID列表
     * @param currentSecond
     * @return UID list, size of {@link BitsAllocator#getMaxSequence()} + 1
     */
    protected List<Long> nextIdsForOneSecond(long currentSecond) {
        // Initialize result list size of (max sequence + 1)
        int listSize = (int) bitsAllocator.getMaxSequence() + 1;
        List<Long> uidList = new ArrayList<>(listSize);

        // Allocate the first sequence of the second, the others can be calculated with the offset
        long firstSeqUid = bitsAllocator.allocate(currentSecond - epochSeconds, workerId, 0L);
        for (int offset = 0; offset < listSize; offset++) {
            uidList.add(firstSeqUid + offset);
        }

        return uidList;
    }
    
    /**
     * Initialize RingBuffer & RingBufferPaddingExecutor
     */
    private void initRingBuffer() {
        // initialize RingBuffer
        int bufferSize = ((int) bitsAllocator.getMaxSequence() + 1) << boostPower;
        this.ringBuffer = new RingBuffer(bufferSize, paddingFactor);
        LOGGER.info("Initialized ring buffer size:{}, paddingFactor:{}", bufferSize, paddingFactor);

        // initialize RingBufferPaddingExecutor
        boolean usingSchedule = (scheduleInterval != null);
        this.bufferPaddingExecutor = new BufferPaddingExecutor(ringBuffer, this::nextIdsForOneSecond, usingSchedule);
        if (usingSchedule) {
            bufferPaddingExecutor.setScheduleInterval(scheduleInterval);
        }
        
        LOGGER.info("Initialized BufferPaddingExecutor. Using schdule:{}, interval:{}", usingSchedule, scheduleInterval);
        
        // set rejected put/take handle policy
        this.ringBuffer.setBufferPaddingExecutor(bufferPaddingExecutor);
        if (rejectedPutBufferHandler != null) {
            this.ringBuffer.setRejectedPutHandler(rejectedPutBufferHandler);
        }
        if (rejectedTakeBufferHandler != null) {
            this.ringBuffer.setRejectedTakeHandler(rejectedTakeBufferHandler);
        }
        
        // fill in all slots of the RingBuffer
        // 初始化的时候填充一次buffer
        bufferPaddingExecutor.paddingBuffer();
        
        // start buffer padding threads
        bufferPaddingExecutor.start();
    }

    /**
     * Setters for spring property
     */
    public void setBoostPower(int boostPower) {
        Assert.isTrue(boostPower > 0, "Boost power must be positive!");
        this.boostPower = boostPower;
    }
    
    public void setRejectedPutBufferHandler(RejectedPutBufferHandler rejectedPutBufferHandler) {
        Assert.notNull(rejectedPutBufferHandler, "RejectedPutBufferHandler can't be null!");
        this.rejectedPutBufferHandler = rejectedPutBufferHandler;
    }

    public void setRejectedTakeBufferHandler(RejectedTakeBufferHandler rejectedTakeBufferHandler) {
        Assert.notNull(rejectedTakeBufferHandler, "RejectedTakeBufferHandler can't be null!");
        this.rejectedTakeBufferHandler = rejectedTakeBufferHandler;
    }

    public void setScheduleInterval(long scheduleInterval) {
        Assert.isTrue(scheduleInterval > 0, "Schedule interval must positive!");
        this.scheduleInterval = scheduleInterval;
    }

}

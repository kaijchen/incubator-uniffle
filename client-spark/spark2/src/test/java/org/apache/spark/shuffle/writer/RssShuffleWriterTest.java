/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.spark.shuffle.writer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssClientConfig;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.util.EventLoop;
import org.hamcrest.core.StringStartsWith;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import scala.Product2;
import scala.Tuple2;
import scala.collection.mutable.MutableList;

import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RssShuffleWriterTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void checkBlockSendResultTest() {
    SparkConf conf = new SparkConf();
    String taskId = "checkBlockSendResultTest_taskId";
    conf.setAppName("testApp")
        .setMaster("local[2]")
        .set(RssClientConfig.RSS_TEST_FLAG, "true")
        .set(RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT, "10000")
        .set(RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL, "1000")
        .set(RssClientConfig.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name())
        .set(RssClientConfig.RSS_COORDINATOR_QUORUM, "127.0.0.1:12345,127.0.0.1:12346");
    // init SparkContext
    SparkContext sc = SparkContext.getOrCreate(conf);
    RssShuffleManager manager = new RssShuffleManager(conf, false);

    Serializer kryoSerializer = new KryoSerializer(conf);
    ShuffleWriteClient mockShuffleWriteClient = mock(ShuffleWriteClient.class);
    Partitioner mockPartitioner = mock(Partitioner.class);
    ShuffleDependency mockDependency = mock(ShuffleDependency.class);
    RssShuffleHandle mockHandle = mock(RssShuffleHandle.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);
    when(mockPartitioner.numPartitions()).thenReturn(2);
    when(mockHandle.getPartitionToServers()).thenReturn(Maps.newHashMap());
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager bufferManager = new WriteBufferManager(
        0, 0, bufferOptions, kryoSerializer,
        Maps.newHashMap(), mockTaskMemoryManager, new ShuffleWriteMetrics());
    WriteBufferManager bufferManagerSpy = spy(bufferManager);
    doReturn(1000000L).when(bufferManagerSpy).acquireMemory(anyLong());

    RssShuffleWriter rssShuffleWriter = new RssShuffleWriter("appId", 0, taskId, 1L,
        bufferManagerSpy, (new TaskMetrics()).shuffleWriteMetrics(),
        manager, conf, mockShuffleWriteClient, mockHandle);

    // case 1: all blocks are sent successfully
    manager.addSuccessBlockIds(taskId, Sets.newHashSet(1L, 2L, 3L));
    rssShuffleWriter.checkBlockSendResult(Sets.newHashSet(1L, 2L, 3L));
    manager.clearTaskMeta(taskId);

    // case 2: partial blocks aren't sent before spark.rss.writer.send.check.timeout,
    // Runtime exception will be thrown
    manager.addSuccessBlockIds(taskId, Sets.newHashSet(1L, 2L));
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(StringStartsWith.startsWith("Timeout:"));
    rssShuffleWriter.checkBlockSendResult(Sets.newHashSet(1L, 2L, 3L));

    manager.clearTaskMeta(taskId);

    // case 3: partial blocks are sent failed, Runtime exception will be thrown
    manager.addSuccessBlockIds(taskId, Sets.newHashSet(1L, 2L));
    manager.addFailedBlockIds(taskId, Sets.newHashSet(3L));
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(StringStartsWith.startsWith("Send failed:"));
    rssShuffleWriter.checkBlockSendResult(Sets.newHashSet(1L, 2L, 3L));
    manager.clearTaskMeta(taskId);
    assertTrue(manager.getSuccessBlockIds(taskId).isEmpty());
    assertTrue(manager.getFailedBlockIds(taskId).isEmpty());
    assertNull(manager.getTaskToBufferManager().get(taskId));

    sc.stop();
  }

  @Test
  public void writeTest() throws Exception {
    SparkConf conf = new SparkConf();
    conf.setAppName("testApp").setMaster("local[2]")
        .set(RssClientConfig.RSS_TEST_FLAG, "true")
        .set(RssClientConfig.RSS_WRITER_BUFFER_SIZE, "32")
        .set(RssClientConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE, "32")
        .set(RssClientConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE, "64")
        .set(RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE, "128")
        .set(RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT, "10000")
        .set(RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL, "1000")
        .set(RssClientConfig.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name())
        .set(RssClientConfig.RSS_COORDINATOR_QUORUM, "127.0.0.1:12345,127.0.0.1:12346");
    // init SparkContext
    SparkContext sc = SparkContext.getOrCreate(conf);
    RssShuffleManager manager = new RssShuffleManager(conf, false);
    List<ShuffleBlockInfo> shuffleBlockInfos = Lists.newArrayList();

    manager.setEventLoop(new EventLoop<AddBlockEvent>("test") {
      @Override
      public void onReceive(AddBlockEvent event) {
        assertEquals("taskId", event.getTaskId());
        shuffleBlockInfos.addAll(event.getShuffleDataInfoList());
        Set<Long> blockIds = event.getShuffleDataInfoList().parallelStream()
            .map(sdi -> sdi.getBlockId()).collect(Collectors.toSet());
        manager.addSuccessBlockIds(event.getTaskId(), blockIds);
      }

      @Override
      public void onError(Throwable e) {
      }
    });
    manager.getEventLoop().start();

    Partitioner mockPartitioner = mock(Partitioner.class);
    ShuffleDependency mockDependency = mock(ShuffleDependency.class);
    ShuffleWriteClient mockShuffleWriteClient = mock(ShuffleWriteClient.class);
    RssShuffleHandle mockHandle = mock(RssShuffleHandle.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    Serializer kryoSerializer = new KryoSerializer(conf);
    when(mockDependency.serializer()).thenReturn(kryoSerializer);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);
    when(mockPartitioner.numPartitions()).thenReturn(2);

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    List<ShuffleServerInfo> ssi12 = Arrays.asList(new ShuffleServerInfo("id1", "0.0.0.1", 100),
        new ShuffleServerInfo("id2", "0.0.0.2", 100));
    partitionToServers.put(0, ssi12);
    List<ShuffleServerInfo> ssi34 = Arrays.asList(new ShuffleServerInfo("id3", "0.0.0.3", 100),
        new ShuffleServerInfo("id4", "0.0.0.4", 100));
    partitionToServers.put(1, ssi34);
    List<ShuffleServerInfo> ssi56 = Arrays.asList(new ShuffleServerInfo("id5", "0.0.0.5", 100),
        new ShuffleServerInfo("id6", "0.0.0.6", 100));
    partitionToServers.put(2, ssi56);
    when(mockPartitioner.getPartition("testKey1")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey2")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey3")).thenReturn(2);
    when(mockPartitioner.getPartition("testKey4")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey5")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey6")).thenReturn(2);
    when(mockPartitioner.getPartition("testKey7")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey8")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey9")).thenReturn(2);

    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    ShuffleWriteMetrics shuffleWriteMetrics = new ShuffleWriteMetrics();
    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager bufferManager = new WriteBufferManager(
        0, 0, bufferOptions, kryoSerializer,
        partitionToServers, mockTaskMemoryManager, shuffleWriteMetrics);
    WriteBufferManager bufferManagerSpy = spy(bufferManager);
    doReturn(1000000L).when(bufferManagerSpy).acquireMemory(anyLong());

    RssShuffleWriter rssShuffleWriter = new RssShuffleWriter("appId", 0, "taskId", 1L,
        bufferManagerSpy, shuffleWriteMetrics, manager, conf, mockShuffleWriteClient, mockHandle);

    RssShuffleWriter<String, String, String> rssShuffleWriterSpy = spy(rssShuffleWriter);
    doNothing().when(rssShuffleWriterSpy).sendCommit();

    // case 1
    MutableList<Product2<String, String>> data = new MutableList();
    data.appendElem(new Tuple2("testKey1", "testValue1"));
    data.appendElem(new Tuple2("testKey2", "testValue2"));
    data.appendElem(new Tuple2("testKey3", "testValue3"));
    data.appendElem(new Tuple2("testKey4", "testValue4"));
    data.appendElem(new Tuple2("testKey5", "testValue5"));
    data.appendElem(new Tuple2("testKey6", "testValue6"));
    rssShuffleWriterSpy.write(data.iterator());

    assertTrue(rssShuffleWriterSpy.getShuffleWriteMetrics().shuffleWriteTime() > 0);
    assertEquals(6, rssShuffleWriterSpy.getShuffleWriteMetrics().shuffleRecordsWritten());
    assertEquals(144, rssShuffleWriterSpy.getShuffleWriteMetrics().shuffleBytesWritten());

    assertEquals(6, shuffleBlockInfos.size());
    for (ShuffleBlockInfo shuffleBlockInfo : shuffleBlockInfos) {
      assertEquals(0, shuffleBlockInfo.getShuffleId());
      assertEquals(24, shuffleBlockInfo.getLength());
      assertEquals(22, shuffleBlockInfo.getUncompressLength());
      if (shuffleBlockInfo.getPartitionId() == 0) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi12);
      } else if (shuffleBlockInfo.getPartitionId() == 1) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi34);
      } else if (shuffleBlockInfo.getPartitionId() == 2) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi56);
      } else {
        throw new Exception("Shouldn't be here");
      }
    }
    Map<Integer, Set<Long>> partitionToBlockIds = rssShuffleWriterSpy.getPartitionToBlockIds();
    assertEquals(2, partitionToBlockIds.get(0).size());
    assertEquals(2, partitionToBlockIds.get(1).size());
    assertEquals(2, partitionToBlockIds.get(2).size());
    partitionToBlockIds.clear();

    sc.stop();
  }

  @Test
  public void postBlockEventTest() throws Exception {
    WriteBufferManager mockBufferManager = mock(WriteBufferManager.class);
    ShuffleWriteMetrics mockMetrics = mock(ShuffleWriteMetrics.class);
    ShuffleDependency mockDependency = mock(ShuffleDependency.class);
    Partitioner mockPartitioner = mock(Partitioner.class);
    RssShuffleManager mockShuffleManager = mock(RssShuffleManager.class);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);
    when(mockPartitioner.numPartitions()).thenReturn(2);
    List<AddBlockEvent> events = Lists.newArrayList();

    EventLoop<AddBlockEvent> eventLoop = new EventLoop<AddBlockEvent>("test") {
      @Override
      public void onReceive(AddBlockEvent event) {
        events.add(event);
      }

      @Override
      public void onError(Throwable e) {
      }
    };
    eventLoop.start();

    when(mockShuffleManager.getEventLoop()).thenReturn(eventLoop);
    RssShuffleHandle mockHandle = mock(RssShuffleHandle.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    ShuffleWriteClient mockWriteClient = mock(ShuffleWriteClient.class);
    SparkConf conf = new SparkConf();
    conf.set(RssClientConfig.RSS_CLIENT_SEND_SIZE_LIMIT, "64")
        .set(RssClientConfig.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());

    RssShuffleWriter writer = new RssShuffleWriter("appId", 0, "taskId", 1L,
        mockBufferManager, mockMetrics, mockShuffleManager, conf, mockWriteClient, mockHandle);
    List<ShuffleBlockInfo> shuffleBlockInfoList = createShuffleBlockList(1, 31);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(1, events.size());
    assertEquals(1, events.get(0).getShuffleDataInfoList().size());
    events.clear();

    shuffleBlockInfoList = createShuffleBlockList(1, 33);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(1, events.size());
    assertEquals(1, events.get(0).getShuffleDataInfoList().size());
    events.clear();

    shuffleBlockInfoList = createShuffleBlockList(2, 15);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(1, events.size());
    assertEquals(2, events.get(0).getShuffleDataInfoList().size());
    events.clear();

    shuffleBlockInfoList = createShuffleBlockList(2, 16);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(1, events.size());
    assertEquals(2, events.get(0).getShuffleDataInfoList().size());
    events.clear();

    shuffleBlockInfoList = createShuffleBlockList(2, 15);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(1, events.size());
    assertEquals(2, events.get(0).getShuffleDataInfoList().size());
    events.clear();

    shuffleBlockInfoList = createShuffleBlockList(2, 17);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(1, events.size());
    assertEquals(2, events.get(0).getShuffleDataInfoList().size());
    events.clear();

    shuffleBlockInfoList = createShuffleBlockList(2, 32);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(1, events.size());
    assertEquals(2, events.get(0).getShuffleDataInfoList().size());
    events.clear();

    shuffleBlockInfoList = createShuffleBlockList(2, 33);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(2, events.size());
    assertEquals(1, events.get(0).getShuffleDataInfoList().size());
    assertEquals(1, events.get(1).getShuffleDataInfoList().size());
    events.clear();

    shuffleBlockInfoList = createShuffleBlockList(3, 17);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(2, events.size());
    assertEquals(2, events.get(0).getShuffleDataInfoList().size());
    assertEquals(1, events.get(1).getShuffleDataInfoList().size());
    events.clear();
  }

  private List<ShuffleBlockInfo> createShuffleBlockList(int blockNum, int blockLength) {
    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList();
    for (int i = 0; i < blockNum; i++) {
      shuffleBlockInfoList.add(new ShuffleBlockInfo(
          0, 0, 10, blockLength, 10, new byte[]{1},
          shuffleServerInfoList, blockLength, 10, 0));
    }
    return shuffleBlockInfoList;
  }
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.statemachine.BucketState;
import org.apache.fluss.server.entity.RegisterTableBucketLeadAndIsrInfo;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;
import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess;
import static org.apache.fluss.server.zk.ZooKeeperUtils.startZookeeperClient;
import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.builder;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests retrying NewBucket initialization through real coordinator events.
 *
 * <p>This test verifies the full event chain for buckets whose first initialization failed because
 * LeaderAndIsr registration failed: the bucket stays in {@code NewBucket}, a real new tablet server
 * registration event triggers a rescan of {@code NewBucket} buckets, and the bucket recovers to
 * {@code OnlineBucket} with a real LeaderAndIsr write to ZooKeeper.
 */
class CoordinatorEventProcessorRetryTest extends CoordinatorEventProcessorTestBase {

    private RetryZooKeeperClient retryZookeeperClient;

    @Override
    protected CoordinatorEventProcessor buildCoordinatorEventProcessor() {
        // Create the custom client only after the ZooKeeper test extension is initialized.
        retryZookeeperClient = new RetryZooKeeperClient(remoteDataDir);

        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        conf.set(ConfigOptions.COORDINATOR_OFFLINE_LEADER_RETRY_DELAY, Duration.ofDays(1));

        return new CoordinatorEventProcessor(
                retryZookeeperClient,
                serverMetadataCache,
                testCoordinatorChannelManager,
                new CoordinatorContext(zkEpoch),
                replicaCapacityController,
                autoPartitionManager,
                lakeTableTieringManager,
                TestingMetricGroups.COORDINATOR_METRICS,
                conf,
                Executors.newFixedThreadPool(1, new ExecutorThreadFactory("test-coordinator-io")),
                metadataManager,
                kvSnapshotLeaseManager,
                scheduler,
                SystemClock.getInstance());
    }

    @AfterEach
    @Override
    void afterEach() throws Exception {
        try {
            // Shut down the processor before closing the client it uses. The processor's
            // shutdown() does not close the ZooKeeperClient it was given.
            super.afterEach();
        } finally {
            if (retryZookeeperClient != null) {
                try {
                    // Clean up the tablet server registered by the test so it does not leak
                    // into other tests sharing the test ZooKeeper.
                    ZOO_KEEPER_EXTENSION_WRAPPER
                            .getCustomExtension()
                            .cleanupPath(ZkData.ServerIdZNode.path(3));
                } finally {
                    retryZookeeperClient.close();
                    retryZookeeperClient = null;
                }
            }
        }
    }

    @Test
    void testNewBucketRecoversAfterNewTabletServerEvent() throws Exception {
        int newServerId = 3;

        // Make requests to the existing tablet servers succeed.
        makeSendLeaderAndStopRequestAlwaysSuccess(
                testCoordinatorChannelManager,
                new HashSet<>(Arrays.asList(0, 1, 2)),
                Collections.emptySet());

        TablePath tablePath = TablePath.of(defaultDatabase, "new_bucket_event_retry");

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build()
                        .withReplicationFactor(2);

        TableAssignment assignment =
                TableAssignment.builder().add(0, BucketAssignment.of(0, 1)).build();

        long tableId =
                metadataManager.createTable(
                        tablePath, remoteDataDir, tableDescriptor, assignment, false);

        TableBucket bucket = new TableBucket(tableId, 0);

        // Wait until the create-table event has attempted LeaderAndIsr registration.
        retry(
                Duration.ofSeconds(30),
                () -> {
                    assertThat(retryZookeeperClient.batchRegisterAttempts.get())
                            .isGreaterThanOrEqualTo(1);
                    assertThat(retryZookeeperClient.individualRegisterAttempts.get())
                            .isGreaterThanOrEqualTo(1);
                });

        // The first registration attempt failed. The bucket should remain NewBucket,
        // and no LeaderAndIsr node should have been written to ZooKeeper.
        assertThat(retryZookeeperClient.getLeaderAndIsr(bucket)).isEmpty();
        assertThat(getBucketState(bucket)).isEqualTo(BucketState.NewBucket);

        // Allow individual registration to succeed. Batch registration still fails,
        // so the production fallback path must perform the real ZooKeeper write.
        retryZookeeperClient.failIndividualRegister = false;

        TabletServerRegistration registration =
                new TabletServerRegistration(
                        "rack3",
                        Endpoint.fromListenersString(DEFAULT_LISTENER_NAME + "://host3:1234"),
                        System.currentTimeMillis(),
                        TabletServerResource.unknown(),
                        null);

        // Register a new server in the same ZooKeeper cluster.
        // This must cause the real NewTabletServerEvent to be processed.
        retryZookeeperClient.registerTabletServer(newServerId, registration);

        // Wait for the event to trigger another scan and for the bucket to become online.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(retryZookeeperClient.getLeaderAndIsr(bucket)).isPresent();
                    assertThat(getBucketState(bucket)).isEqualTo(BucketState.OnlineBucket);
                });

        assertThat(retryZookeeperClient.batchRegisterAttempts.get()).isGreaterThanOrEqualTo(2);
        assertThat(retryZookeeperClient.individualRegisterAttempts.get()).isGreaterThanOrEqualTo(2);

        // Verify the LeaderAndIsr was really written to ZooKeeper.
        assertThat(retryZookeeperClient.getLeaderAndIsr(bucket)).isPresent();
    }

    private BucketState getBucketState(TableBucket bucket) throws Exception {
        AccessContextEvent<BucketState> event =
                new AccessContextEvent<>(ctx -> ctx.getBucketState(bucket));

        eventProcessor.getCoordinatorEventManager().put(event);

        return event.getResultFuture().get(30, TimeUnit.SECONDS);
    }

    /**
     * A ZooKeeper client that can simulate failed LeaderAndIsr registration.
     *
     * <p>Batch registration always fails so that the production fallback path (individual
     * registration) is exercised. Individual registration fails while the failure switch is on;
     * once it is turned off, registration goes through {@code super.registerLeaderAndIsr} and is
     * written to the real test ZooKeeper.
     */
    private static final class RetryZooKeeperClient extends ZooKeeperClient {

        private final AtomicInteger batchRegisterAttempts = new AtomicInteger();
        private final AtomicInteger individualRegisterAttempts = new AtomicInteger();

        // Accessed by the coordinator event-processing thread and the test thread.
        private volatile boolean failIndividualRegister = true;

        private RetryZooKeeperClient(String remoteDataDir) {
            super(createCuratorFrameworkWrapper(), createConfiguration(remoteDataDir));
        }

        @Override
        public void batchRegisterLeaderAndIsrForTablePartition(
                List<RegisterTableBucketLeadAndIsrInfo> registerList, int expectedZkVersion)
                throws Exception {
            batchRegisterAttempts.incrementAndGet();
            throw new RuntimeException("Expected batch registration failure");
        }

        @Override
        public void registerLeaderAndIsr(
                TableBucket tableBucket, LeaderAndIsr leaderAndIsr, int expectedZkVersion)
                throws Exception {
            individualRegisterAttempts.incrementAndGet();

            if (failIndividualRegister) {
                throw new RuntimeException("Expected individual registration failure");
            }

            // Use the real ZooKeeper write when the failure switch is disabled.
            super.registerLeaderAndIsr(tableBucket, leaderAndIsr, expectedZkVersion);
        }

        private static CuratorFrameworkWithUnhandledErrorListener createCuratorFrameworkWrapper() {
            // Replicate the curator namespace (zk root) of the shared config-based test client
            // so that both clients see the same znodes.
            Configuration conf = new Configuration();
            conf.setString(
                    ConfigOptions.ZOOKEEPER_ADDRESS,
                    ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
            Builder builder =
                    builder()
                            .connectString(conf.getValue(ConfigOptions.ZOOKEEPER_ADDRESS))
                            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                            .namespace(
                                    ZooKeeperUtils.trimStartingSlash(
                                            ZooKeeperUtils.generateZookeeperPath(
                                                    conf.getValue(ConfigOptions.ZOOKEEPER_ROOT))));
            return startZookeeperClient(builder, NOPErrorHandler.INSTANCE);
        }

        private static Configuration createConfiguration(String remoteDataDir) {
            Configuration configuration = new Configuration();
            configuration.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
            return configuration;
        }
    }
}

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
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.coordinator.statemachine.BucketState;
import org.apache.fluss.server.entity.RegisterTableBucketLeadAndIsrInfo;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperTestUtils;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.fluss.server.zk.ZooKeeperUtils.startZookeeperClient;
import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.builder;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mini-cluster fault-injection experiment for the NewBucket retry gap: a real ZooKeeper, a real
 * tablet server, and a coordinator event processor whose ZooKeeper client fails both LeaderAndIsr
 * registration paths on demand.
 *
 * <p>Phase A proves that once the first LeaderAndIsr registration fails, the bucket stays in {@code
 * NewBucket} and is not re-attempted within a bounded observation window as long as no new tablet
 * server event arrives. Phase B proves that a real tablet server registration event (a real
 * ephemeral ZK node seen by the real watcher) triggers a rescan and recovers the bucket to {@code
 * OnlineBucket} with a real LeaderAndIsr write to ZooKeeper.
 */
class CoordinatorEventProcessorFaultInjectionTest {

    private TestingServer testingServer;
    private Path tempDir;
    private String remoteDataDir;
    private FaultInjectionZooKeeperClient coordinatorZkClient;
    private CoordinatorEventProcessor eventProcessor;
    private CoordinatorChannelManager channelManager;
    private RpcClient rpcClient;
    private Scheduler scheduler;
    private KvSnapshotLeaseManager kvSnapshotLeaseManager;
    private CoordinatorContext coordinatorContext;
    private MetadataManager metadataManager;
    private final Map<Integer, TabletServer> tabletServers = new HashMap<>();

    @BeforeEach
    void beforeEach() throws Exception {
        testingServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
        tempDir = Files.createTempDirectory("newbucket-retry-gap");
        remoteDataDir =
                LocalFileSystem.getLocalFsURI().getScheme()
                        + "://"
                        + tempDir.resolve("remote-data-dir").toAbsolutePath();

        coordinatorZkClient =
                new FaultInjectionZooKeeperClient(testingServer.getConnectString(), remoteDataDir);

        // A real tablet server joins before the coordinator exists (like a real cluster boot).
        startTabletServer(0, "CLIENT://localhost:10092,FLUSS://localhost:10093");
    }

    @AfterEach
    void afterEach() throws Exception {
        if (eventProcessor != null) {
            eventProcessor.shutdown();
            eventProcessor = null;
        }
        if (channelManager != null) {
            channelManager.close();
            channelManager = null;
        }
        if (rpcClient != null) {
            rpcClient.close();
            rpcClient = null;
        }
        for (TabletServer tabletServer : tabletServers.values()) {
            tabletServer.close();
        }
        tabletServers.clear();
        if (scheduler != null) {
            scheduler.shutdown();
            scheduler = null;
        }
        if (kvSnapshotLeaseManager != null) {
            kvSnapshotLeaseManager.close();
            kvSnapshotLeaseManager = null;
        }
        if (coordinatorZkClient != null) {
            coordinatorZkClient.close();
            coordinatorZkClient = null;
        }
        if (testingServer != null) {
            testingServer.close();
            testingServer = null;
        }
    }

    @Test
    void testNewBucketWithoutRetryMechanismRecoversOnlyViaNewTabletServerEvent() throws Exception {
        startCoordinatorProcessor(Duration.ofDays(1));

        // ---------- Phase A1: both registration paths are triggered and fail ----------
        TablePath tablePath = TablePath.of("fault_injection_db", "retry_gap");
        metadataManager.createDatabase(
                "fault_injection_db", DatabaseDescriptor.builder().build(), false);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build()
                        .withReplicationFactor(1);
        TableAssignment assignment =
                TableAssignment.builder().add(0, BucketAssignment.of(0)).build();
        long tableId =
                metadataManager.createTable(
                        tablePath, remoteDataDir, descriptor, assignment, false);
        TableBucket bucket = new TableBucket(tableId, 0);

        // Wait until the create-table event attempted registration through both paths.
        retry(
                Duration.ofSeconds(30),
                () -> {
                    assertThat(coordinatorZkClient.batchRegisterAttempts.get())
                            .isGreaterThanOrEqualTo(1);
                    assertThat(coordinatorZkClient.individualRegisterAttempts.get())
                            .isGreaterThanOrEqualTo(1);
                });

        // Both paths failed: the bucket must remain NewBucket and ZooKeeper must not hold a
        // LeaderAndIsr for it.
        assertThat(coordinatorZkClient.getLeaderAndIsr(bucket)).isEmpty();
        assertThat(getBucketState(bucket)).isEqualTo(BucketState.NewBucket);

        // ---------- Phase A2: bounded observation window with no new tablet server event
        // ----------
        int batchAttemptsBeforeWindow = coordinatorZkClient.batchRegisterAttempts.get();
        int individualAttemptsBeforeWindow = coordinatorZkClient.individualRegisterAttempts.get();

        // Bounded observation window: if any automatic retry mechanism existed for NewBucket
        // buckets, a registration attempt would be expected within this window. The counters
        // prove that no event-driven rescan and no retry happens at all.
        Thread.sleep(Duration.ofSeconds(5).toMillis());

        assertThat(coordinatorZkClient.batchRegisterAttempts.get())
                .isEqualTo(batchAttemptsBeforeWindow);
        assertThat(coordinatorZkClient.individualRegisterAttempts.get())
                .isEqualTo(individualAttemptsBeforeWindow);
        assertThat(getBucketState(bucket)).isEqualTo(BucketState.NewBucket);
        assertThat(coordinatorZkClient.getLeaderAndIsr(bucket)).isEmpty();

        // ---------- Phase B: a real new tablet server event triggers the recovery ----------
        // Allow individual registration to succeed; batch registration keeps failing, so the
        // production fallback path must perform the real ZooKeeper write.
        coordinatorZkClient.failIndividualRegister = false;
        startTabletServer(1, "CLIENT://localhost:10102,FLUSS://localhost:10103");

        // The real watcher must produce a NewTabletServerEvent; the rescan must elect a leader
        // for the bucket, register LeaderAndIsr in real ZooKeeper, and go online.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(coordinatorZkClient.getLeaderAndIsr(bucket)).isPresent();
                    assertThat(getBucketState(bucket)).isEqualTo(BucketState.OnlineBucket);
                });

        assertThat(coordinatorZkClient.batchRegisterAttempts.get()).isGreaterThanOrEqualTo(2);
        assertThat(coordinatorZkClient.individualRegisterAttempts.get()).isGreaterThanOrEqualTo(2);

        // Verify real ZooKeeper state: the LeaderAndIsr is present, its leader is the live
        // tablet server 0, and the new tablet server 1 is really registered.
        LeaderAndIsr leaderAndIsr = coordinatorZkClient.getLeaderAndIsr(bucket).orElse(null);
        assertThat(leaderAndIsr).isNotNull();
        assertThat(leaderAndIsr.leader()).isEqualTo(0);
        assertThat(leaderAndIsr.isr()).contains(0);
        assertThat(coordinatorZkClient.getTabletServer(1)).isPresent();
    }

    @Test
    void testNewBucketAutoRecoversWithoutNewTabletServerEvent() throws Exception {
        // A short retry delay so the self-perpetuating retry chain runs quickly.
        startCoordinatorProcessor(Duration.ofSeconds(1));

        // ---------- Phase A: both registration paths are triggered and fail ----------
        TablePath tablePath = TablePath.of("fault_injection_db", "auto_retry_gap");
        metadataManager.createDatabase(
                "fault_injection_db", DatabaseDescriptor.builder().build(), false);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build()
                        .withReplicationFactor(1);
        TableAssignment assignment =
                TableAssignment.builder().add(0, BucketAssignment.of(0)).build();
        long tableId =
                metadataManager.createTable(
                        tablePath, remoteDataDir, descriptor, assignment, false);
        TableBucket bucket = new TableBucket(tableId, 0);

        retry(
                Duration.ofSeconds(30),
                () -> {
                    assertThat(coordinatorZkClient.batchRegisterAttempts.get())
                            .isGreaterThanOrEqualTo(1);
                    assertThat(coordinatorZkClient.individualRegisterAttempts.get())
                            .isGreaterThanOrEqualTo(1);
                });
        assertThat(getBucketState(bucket)).isEqualTo(BucketState.NewBucket);
        assertThat(coordinatorZkClient.getLeaderAndIsr(bucket)).isEmpty();

        // With the retry mechanism in place, the coordinator must re-attempt the
        // initialization on its own: the counters must grow within a few retry intervals
        // even though no tablet-server event arrives.
        int attemptsBefore = coordinatorZkClient.individualRegisterAttempts.get();
        retry(
                Duration.ofSeconds(10),
                () ->
                        assertThat(coordinatorZkClient.individualRegisterAttempts.get())
                                .isGreaterThanOrEqualTo(attemptsBefore + 2));

        // The failure switch is still on, so every automatic retry must fail: the bucket
        // must remain NewBucket with no LeaderAndIsr in ZooKeeper.
        assertThat(getBucketState(bucket)).isEqualTo(BucketState.NewBucket);
        assertThat(coordinatorZkClient.getLeaderAndIsr(bucket)).isEmpty();

        // The live tablet servers must not have changed: recovery must not depend on
        // new tablet-server events.
        Set<Integer> liveServers = accessContext(ctx -> ctx.liveTabletServerSet());
        assertThat(liveServers).containsExactly(0);

        // ---------- Phase B: allow registration to succeed, still without new events ----------
        coordinatorZkClient.failIndividualRegister = false;
        retry(
                Duration.ofSeconds(30),
                () -> {
                    assertThat(coordinatorZkClient.getLeaderAndIsr(bucket)).isPresent();
                    assertThat(getBucketState(bucket)).isEqualTo(BucketState.OnlineBucket);
                });

        LeaderAndIsr leaderAndIsr = coordinatorZkClient.getLeaderAndIsr(bucket).orElse(null);
        assertThat(leaderAndIsr).isNotNull();
        assertThat(leaderAndIsr.leader()).isEqualTo(0);
    }

    private void startCoordinatorProcessor(Duration newBucketRetryDelay) throws Exception {
        // ---------- coordinator wiring (same production order as the real-rpc precheck) ----------
        coordinatorZkClient.registerCoordinatorLeader(
                new CoordinatorAddress(
                        "0", Endpoint.fromListenersString("CLIENT://localhost:10112")));
        ZkEpoch zkEpoch = coordinatorZkClient.fenceBecomeCoordinatorLeader("0");
        coordinatorContext = new CoordinatorContext(zkEpoch);

        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        conf.set(ConfigOptions.COORDINATOR_OFFLINE_LEADER_RETRY_DELAY, Duration.ofDays(1));
        conf.set(ConfigOptions.COORDINATOR_NEW_BUCKET_RETRY_DELAY, newBucketRetryDelay);

        metadataManager =
                new MetadataManager(
                        coordinatorZkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        CoordinatorMetadataCache serverMetadataCache = new CoordinatorMetadataCache();
        ReplicaCapacityController replicaCapacityController =
                new ReplicaCapacityController(conf, serverMetadataCache);
        AutoPartitionManager autoPartitionManager =
                new AutoPartitionManager(
                        serverMetadataCache,
                        metadataManager,
                        new RemoteDirDynamicLoader(conf),
                        conf,
                        replicaCapacityController);
        LakeTableTieringManager lakeTableTieringManager =
                new LakeTableTieringManager(TestingMetricGroups.LAKE_TIERING_METRICS);
        kvSnapshotLeaseManager =
                new KvSnapshotLeaseManager(
                        Duration.ofMinutes(10).toMillis(),
                        coordinatorZkClient,
                        remoteDataDir,
                        SystemClock.getInstance(),
                        TestingMetricGroups.COORDINATOR_METRICS);
        kvSnapshotLeaseManager.start();
        scheduler = new FlussScheduler(1);
        scheduler.startup();
        rpcClient =
                RpcClient.create(
                        conf,
                        new ClientMetricGroup(
                                MetricRegistry.create(conf, null), "newbucket-retry-gap"));
        channelManager =
                new CoordinatorChannelManager(
                        rpcClient,
                        coordinatorContext::getCoordinatorEpoch,
                        conf,
                        TestingMetricGroups.COORDINATOR_METRICS);
        eventProcessor =
                new CoordinatorEventProcessor(
                        coordinatorZkClient,
                        serverMetadataCache,
                        channelManager,
                        coordinatorContext,
                        replicaCapacityController,
                        autoPartitionManager,
                        lakeTableTieringManager,
                        TestingMetricGroups.COORDINATOR_METRICS,
                        conf,
                        Executors.newFixedThreadPool(
                                1, new ExecutorThreadFactory("newbucket-retry-gap-io")),
                        metadataManager,
                        kvSnapshotLeaseManager,
                        scheduler,
                        SystemClock.getInstance());
        eventProcessor.startup();
    }

    private void startTabletServer(int serverId, String bindListeners) throws Exception {
        Configuration tabletServerConf = new Configuration();
        tabletServerConf.set(ConfigOptions.TABLET_SERVER_ID, serverId);
        tabletServerConf.set(ConfigOptions.TABLET_SERVER_RACK, "rack-" + serverId);
        tabletServerConf.set(
                ConfigOptions.DATA_DIR,
                tempDir.resolve("data-" + serverId).toAbsolutePath().toString());
        tabletServerConf.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS, testingServer.getConnectString());
        tabletServerConf.setString(ConfigOptions.BIND_LISTENERS, bindListeners);
        tabletServerConf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        TabletServer tabletServer = new TabletServer(tabletServerConf, SystemClock.getInstance());
        tabletServer.start();
        tabletServers.put(serverId, tabletServer);
    }

    private BucketState getBucketState(TableBucket bucket) throws Exception {
        return accessContext(ctx -> ctx.getBucketState(bucket));
    }

    private <T> T accessContext(Function<CoordinatorContext, T> function) throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(function);
        eventProcessor.getCoordinatorEventManager().put(event);
        return event.getResultFuture().get(30, TimeUnit.SECONDS);
    }

    /**
     * A ZooKeeper client whose LeaderAndIsr registration paths can be made to fail on demand.
     *
     * <p>Batch registration always fails so that the production fallback path (individual
     * registration) is exercised. Individual registration fails while the failure switch is on;
     * once it is turned off, registration goes through {@code super.registerLeaderAndIsr} and is
     * written to the real ZooKeeper.
     */
    private static final class FaultInjectionZooKeeperClient extends ZooKeeperClient {

        private final AtomicInteger batchRegisterAttempts = new AtomicInteger();
        private final AtomicInteger individualRegisterAttempts = new AtomicInteger();

        // Accessed by the coordinator event-processing thread and the test thread.
        private volatile boolean failIndividualRegister = true;

        private FaultInjectionZooKeeperClient(String zkConnectString, String remoteDataDir) {
            super(
                    createCuratorFrameworkWrapper(zkConnectString),
                    createConfiguration(remoteDataDir));
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

        private static CuratorFrameworkWithUnhandledErrorListener createCuratorFrameworkWrapper(
                String zkConnectString) {
            // Replicate the curator namespace (zk root) of config-based clients so that all
            // clients of the mini cluster see the same znodes.
            Configuration conf = new Configuration();
            conf.setString(ConfigOptions.ZOOKEEPER_ADDRESS, zkConnectString);
            Builder builder =
                    builder()
                            .connectString(zkConnectString)
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

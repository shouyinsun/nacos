/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.misc;

import java.util.concurrent.*;

/**
 * @author nacos
 */
public class GlobalExecutor {

    //心跳超时时间 5s
    public static final long HEARTBEAT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5L);
    //leader超时时间 15s
    public static final long LEADER_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(15L);

    public static final long RANDOM_MS = TimeUnit.SECONDS.toMillis(5L);

    //tick 周期 500ms
    public static final long TICK_PERIOD_MS = TimeUnit.MILLISECONDS.toMillis(500L);

    //server list 刷新间隔 5s
    private static final long NACOS_SERVER_LIST_REFRESH_INTERVAL = TimeUnit.SECONDS.toMillis(5);

    //分区
    private static final long PARTITION_DATA_TIMED_SYNC_INTERVAL = TimeUnit.SECONDS.toMillis(5);

    //local server 状态更新周期 5s
    private static final long SERVER_STATUS_UPDATE_PERIOD = TimeUnit.SECONDS.toMillis(5);

    private static ScheduledExecutorService executorService =
        new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);

                t.setDaemon(true);
                t.setName("com.alibaba.nacos.naming.timer");

                return t;
            }
        });

    //任务分发线程池
    private static ScheduledExecutorService taskDispatchExecutor =
        new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);

                t.setDaemon(true);
                t.setName("com.alibaba.nacos.naming.distro.task.dispatcher");

                return t;
            }
        });


    //data同步线程池
    private static ScheduledExecutorService dataSyncExecutor =
        new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);

                t.setDaemon(true);
                t.setName("com.alibaba.nacos.naming.distro.data.syncer");

                return t;
            }
        });

    //server list 变更通知线程
    private static ScheduledExecutorService notifyServerListExecutor =
        new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);

                t.setDaemon(true);
                t.setName("com.alibaba.nacos.naming.server.list.notifier");

                return t;
            }
        });

    //local server status 更新
    private static final ScheduledExecutorService SERVER_STATUS_EXECUTOR
        = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("nacos.naming.status.worker");
            t.setDaemon(true);
            return t;
        }
    });

    /**
     * thread pool that processes getting service detail from other server asynchronously
     */
    //其他server的服务信息
    private static ExecutorService serviceUpdateExecutor
        = Executors.newFixedThreadPool(2, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("com.alibaba.nacos.naming.service.update.http.handler");
            t.setDaemon(true);
            return t;
        }
    });

    public static void submitDataSync(Runnable runnable, long delay) {
        dataSyncExecutor.schedule(runnable, delay, TimeUnit.MILLISECONDS);
    }

    //每个server只负责一部分数据,但是同步其他server的数据
    //5s
    public static void schedulePartitionDataTimedSync(Runnable runnable) {
        dataSyncExecutor.scheduleWithFixedDelay(runnable, PARTITION_DATA_TIMED_SYNC_INTERVAL,
            PARTITION_DATA_TIMED_SYNC_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public static void registerMasterElection(Runnable runnable) {
        //500ms fixed rate
        executorService.scheduleAtFixedRate(runnable, 0, TICK_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    public static void registerServerListUpdater(Runnable runnable) {
        //5s fixed rate
        executorService.scheduleAtFixedRate(runnable, 0, NACOS_SERVER_LIST_REFRESH_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public static void registerServerStatusReporter(Runnable runnable, long delay) {
        SERVER_STATUS_EXECUTOR.schedule(runnable, delay, TimeUnit.MILLISECONDS);
    }

    public static void registerServerStatusUpdater(Runnable runnable) {
        executorService.scheduleAtFixedRate(runnable, 0, SERVER_STATUS_UPDATE_PERIOD, TimeUnit.MILLISECONDS);
    }

    public static void registerHeartbeat(Runnable runnable) {
        //500ms fixed delay
        executorService.scheduleWithFixedDelay(runnable, 0, TICK_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    public static void schedule(Runnable runnable, long period) {
        executorService.scheduleAtFixedRate(runnable, 0, period, TimeUnit.MILLISECONDS);
    }

    public static void schedule(Runnable runnable, long initialDelay, long period) {
        executorService.scheduleAtFixedRate(runnable, initialDelay, period, TimeUnit.MILLISECONDS);
    }

    public static void notifyServerListChange(Runnable runnable) {
        //right now
        notifyServerListExecutor.submit(runnable);
    }

    public static void submitTaskDispatch(Runnable runnable) {
        taskDispatchExecutor.submit(runnable);
    }

    public static void submit(Runnable runnable) {
        executorService.submit(runnable);
    }

    public static void submitServiceUpdate(Runnable runnable) {
        serviceUpdateExecutor.execute(runnable);
    }
}

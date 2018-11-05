/*
 * Copyright 2018 Shanghai Junzheng Network Technology Co.,Ltd.
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
package com.hellobike.base.tunnel.ha;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author machunxiao create at 2018-12-26
 */
public class ZkDistributedLock implements DistributedLock {

    private static final Logger LOG = LoggerFactory.getLogger(ZkDistributedLock.class.getName());

    private final CuratorFramework zkClient;
    private final String lockPath;

    private final AtomicBoolean aborted = new AtomicBoolean(false);
    private CountDownLatch syncPoint;
    private boolean holdsLock = false;
    private String currentId;
    private String currentNode;
    private String watchedNode;

    /**
     * Creates a distributed lock using the given {@code zkClient} to coordinate locking.
     *
     * @param zkClient The ZooKeeper client to use.
     * @param lockPath The path used to manage the lock under.
     * @param acl      The acl to apply to newly created lock nodes.
     */
    public ZkDistributedLock(CuratorFramework zkClient, String lockPath, Iterable<ACL> acl) {
        this.zkClient = zkClient;
        this.lockPath = lockPath;
        this.syncPoint = new CountDownLatch(1);
    }

    @Override
    public synchronized void lock() throws LockingException {
        if (holdsLock) {
            throw new LockingException("Error, already holding a lock. Call unlock first!");
        }
        try {
            prepare();
            syncPoint.await();
            if (!holdsLock) {
                throw new LockingException("Error, couldn't acquire the lock!");
            }
        } catch (InterruptedException e) {
            cancelAttempt();
            throw new LockingException("InterruptedException while trying to acquire lock!", e);
        } catch (Exception e) {
            // No need to clean up since the node wasn't created yet.
            throw new LockingException("ZkException while trying to acquire lock!", e);
        }
    }

    @Override
    public synchronized boolean tryLock(long timeout, TimeUnit unit) {
        if (holdsLock) {
            throw new LockingException("Error, already holding a lock. Call unlock first!");
        }
        try {
            prepare();
            boolean success = syncPoint.await(timeout, unit);
            if (!success) {
                return false;
            }
            if (!holdsLock) {
                throw new LockingException("Error, couldn't acquire the lock!");
            }
        } catch (InterruptedException e) {
            cancelAttempt();
            return false;
        } catch (Exception e) {
            // No need to clean up since the node wasn't created yet.
            throw new LockingException("ZkException while trying to acquire lock!", e);
        }
        return true;
    }

    @Override
    public synchronized void unlock() throws LockingException {
        if (currentId == null) {
            throw new LockingException("Error, neither attempting to lock nor holding a lock!");
        }
        Preconditions.checkNotNull(currentId);
        // Try aborting!
        if (!holdsLock) {
            aborted.set(true);
        } else {
            cleanup();
        }
    }

    private synchronized void prepare() throws Exception {
        this.zkClient.checkExists().forPath(lockPath);

        // Create an EPHEMERAL_SEQUENTIAL node.
        this.currentNode = this.zkClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(lockPath + "/member_");

        // We only care about our actual id since we want to compare ourselves to siblings.
        if (currentNode.contains("/")) {
            currentId = currentNode.substring(currentNode.lastIndexOf("/") + 1);
        }
    }

    private synchronized void cancelAttempt() {
        cleanup();
        // Bubble up failure...
        holdsLock = false;
        syncPoint.countDown();
    }

    private void cleanup() {
        LOG.info("Cleaning up!");
        Preconditions.checkNotNull(currentId);
        try {
            Stat stat = zkClient.checkExists().forPath(currentNode);
            if (stat != null) {
                zkClient.delete().forPath(currentNode);
            } else {
                LOG.info("Called cleanup but nothing to cleanup!");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        holdsLock = false;
        aborted.set(false);
        currentId = null;
        currentNode = null;
        syncPoint = new CountDownLatch(1);
    }

}

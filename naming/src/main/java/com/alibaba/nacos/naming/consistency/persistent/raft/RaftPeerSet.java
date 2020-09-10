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
package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.apache.commons.collections.SortedBag;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.nacos.core.utils.SystemUtils.STANDALONE_MODE;

/**
 * @author nacos
 */

//维护当前节点持有的同伴节点信息
@Component
@DependsOn("serverListManager")
public class RaftPeerSet implements ServerChangeListener, ApplicationContextAware {

    @Autowired
    private ServerListManager serverListManager;

    private ApplicationContext applicationContext;

    //当前本机任期
    private AtomicLong localTerm = new AtomicLong(0L);

    //leader 节点
    private RaftPeer leader = null;

    // ip -> RaftPeer
    private Map<String, RaftPeer> peers = new HashMap<>();//同伴信息

    private Set<String> sites = new HashSet<>();

    private boolean ready = false;

    public RaftPeerSet() {

    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void init() {//serverListManager 加本身这个监听
        serverListManager.listen(this);
    }

    public RaftPeer getLeader() {
        if (STANDALONE_MODE) {//单机模式
            return local();//本机
        }
        return leader;
    }

    public Set<String> allSites() {
        return sites;
    }

    public boolean isReady() {
        return ready;
    }

    public void remove(List<String> servers) {
        for (String server : servers) {
            peers.remove(server);
        }
    }

    public RaftPeer update(RaftPeer peer) {
        peers.put(peer.ip, peer);
        return peer;
    }

    public boolean isLeader(String ip) {
        if (STANDALONE_MODE) {
            return true;
        }

        if (leader == null) {
            Loggers.RAFT.warn("[IS LEADER] no leader is available now!");
            return false;
        }

        return StringUtils.equals(leader.ip, ip);
    }

    public Set<String> allServersIncludeMyself() {
        return peers.keySet();
    }

    public Set<String> allServersWithoutMySelf() {
        Set<String> servers = new HashSet<String>(peers.keySet());

        // exclude myself
        servers.remove(local().ip);

        return servers;
    }

    public Collection<RaftPeer> allPeers() {
        return peers.values();
    }

    public int size() {
        return peers.size();
    }

    public RaftPeer decideLeader(RaftPeer candidate) {
        //peers add candidate
        peers.put(candidate.ip, candidate);

        SortedBag ips = new TreeBag();
        //最大的vote同意次数
        int maxApproveCount = 0;
        String maxApprovePeer = null;
        for (RaftPeer peer : peers.values()) {
            if (StringUtils.isEmpty(peer.voteFor)) {
                continue;
            }

            ips.add(peer.voteFor);
            if (ips.getCount(peer.voteFor) > maxApproveCount) {
                maxApproveCount = ips.getCount(peer.voteFor);
                //投票内容 ip
                maxApprovePeer = peer.voteFor;
            }
        }

        if (maxApproveCount >= majorityCount()) {
            RaftPeer peer = peers.get(maxApprovePeer);
            //选举出 leader
            peer.state = RaftPeer.State.LEADER;

            if (!Objects.equals(leader, peer)) {
                leader = peer;
                //发布选举完成事件
                applicationContext.publishEvent(new LeaderElectFinishedEvent(this, leader));
                Loggers.RAFT.info("{} has become the LEADER", leader.ip);
            }
        }

        return leader;
    }

    public RaftPeer makeLeader(RaftPeer candidate) {//设置 leader
        if (!Objects.equals(leader, candidate)) {
            leader = candidate;
            applicationContext.publishEvent(new MakeLeaderEvent(this, leader));
            Loggers.RAFT.info("{} has become the LEADER, local: {}, leader: {}",
                leader.ip, JSON.toJSONString(local()), JSON.toJSONString(leader));
        }

        for (final RaftPeer peer : peers.values()) {
            Map<String, String> params = new HashMap<>(1);
            //旧的 leader
            if (!Objects.equals(peer, candidate) && peer.state == RaftPeer.State.LEADER) {
                try {
                    String url = RaftCore.buildURL(peer.ip, RaftCore.API_GET_PEER);
                    HttpClient.asyncHttpGet(url, null, params, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.error("[NACOS-RAFT] get peer failed: {}, peer: {}",
                                    response.getResponseBody(), peer.ip);
                                peer.state = RaftPeer.State.FOLLOWER;
                                return 1;
                            }

                            update(JSON.parseObject(response.getResponseBody(), RaftPeer.class));

                            return 0;
                        }
                    });
                } catch (Exception e) {
                    peer.state = RaftPeer.State.FOLLOWER;
                    Loggers.RAFT.error("[NACOS-RAFT] error while getting peer from peer: {}", peer.ip);
                }
            }
        }

        return update(candidate);
    }

    public RaftPeer local() {
        RaftPeer peer = peers.get(NetUtils.localServer());
        if (peer == null && SystemUtils.STANDALONE_MODE) {
            RaftPeer localPeer = new RaftPeer();
            localPeer.ip = NetUtils.localServer();
            localPeer.term.set(localTerm.get());
            peers.put(localPeer.ip, localPeer);
            return localPeer;
        }
        if (peer == null) {
            throw new IllegalStateException("unable to find local peer: " + NetUtils.localServer() + ", all peers: "
                + Arrays.toString(peers.keySet().toArray()));
        }

        return peer;
    }

    public RaftPeer get(String server) {
        return peers.get(server);
    }

    public int majorityCount() {//过半
        return peers.size() / 2 + 1;
    }

    public void reset() {

        //leader null
        leader = null;

        for (RaftPeer peer : peers.values()) {
            //all peer's voteFor set null
            peer.voteFor = null;
        }
    }

    public void setTerm(long term) {
        localTerm.set(term);
    }

    public long getTerm() {
        return localTerm.get();
    }

    public boolean contains(RaftPeer remote) {
        return peers.containsKey(remote.ip);
    }

    @Override
    public void onChangeServerList(List<Server> latestMembers) {//监听 serverList更新,

        Map<String, RaftPeer> tmpPeers = new HashMap<>(8);
        for (Server member : latestMembers) {

            if (peers.containsKey(member.getKey())) {
                tmpPeers.put(member.getKey(), peers.get(member.getKey()));
                continue;
            }

            RaftPeer raftPeer = new RaftPeer();
            raftPeer.ip = member.getKey();

            // first time meet the local server:
            if (NetUtils.localServer().equals(member.getKey())) {
                raftPeer.term.set(localTerm.get());
            }

            tmpPeers.put(member.getKey(), raftPeer);
        }

        // replace raft peer set:
        peers = tmpPeers;

        if (RunningConfig.getServerPort() > 0) {//端口启用了,ready状态
            ready = true;
        }

        Loggers.RAFT.info("raft peers changed: " + latestMembers);
    }

    @Override
    public void onChangeHealthyServerList(List<Server> latestReachableMembers) {

    }
}

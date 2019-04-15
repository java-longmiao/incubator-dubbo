/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.rpc.cluster.loadbalance;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.support.RpcUtils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ConsistentHashLoadBalance
 *
 * 可以通过< dubbo:service loadbalance="consistenthash" />或< dubbo:provider loadbalance = "consistenthash" />
 * 负载均衡算法：一致性Hash算法，在AbstractClusterInvoker中从多个服务提供者中选择一个服务提供者时被调用。
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {

    private final ConcurrentMap<String, ConsistentHashSelector<?>> selectors = new ConcurrentHashMap<String, ConsistentHashSelector<?>>();

    @SuppressWarnings("unchecked")
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String methodName = RpcUtils.getMethodName(invocation);
        String key = invokers.get(0).getUrl().getServiceKey() + "." + methodName;
        // 根据所有的调用者生成一个HashCode，用该HashCode值来判断服务提供者是否发生了变化。
        int identityHashCode = System.identityHashCode(invokers);
        ConsistentHashSelector<T> selector = (ConsistentHashSelector<T>) selectors.get(key);
        if (selector == null || selector.identityHashCode != identityHashCode) {
            selectors.put(key, new ConsistentHashSelector<T>(invokers, methodName, identityHashCode));
            selector = (ConsistentHashSelector<T>) selectors.get(key);
        }
        return selector.select(invocation);
    }

    private static final class ConsistentHashSelector<T> {

        private final TreeMap<Long, Invoker<T>> virtualInvokers;

        private final int replicaNumber;

        private final int identityHashCode;

        private final int[] argumentIndex;

        ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {
            this.virtualInvokers = new TreeMap<Long, Invoker<T>>();
            this.identityHashCode = identityHashCode;
            URL url = invokers.get(0).getUrl();
            // 获取服务提供者< dubbo:method/>标签的hash.nodes属性，如果为空，默认为160，表示一致性hash算法中虚拟节点数量。其配置方式如下：
//                < dubbo:method ... >
//                    < dubbo:parameter key="hash.nodes" value="160" />
//                    < dubbo:parameter key="hash.arguments" value="0,1" />
//                < /dubbo:method/>
            this.replicaNumber = url.getMethodParameter(methodName, "hash.nodes", 160);
            // 一致性Hash算法，在dubbo中，相同的服务调用参数走固定的节点，hash.arguments表示哪些参数参与hashcode，默认值“0”，表示第一个参数。
            String[] index = Constants.COMMA_SPLIT_PATTERN.split(url.getMethodParameter(methodName, "hash.arguments", "0"));
            argumentIndex = new int[index.length];
            for (int i = 0; i < index.length; i++) {
                argumentIndex[i] = Integer.parseInt(index[i]);
            }
            // 为每一个Invoker创建replicaNumber 个虚拟节点，每一个节点的Hashcode不同。同一个Invoker不同hashcode的创建逻辑为：
            // invoker.getUrl().toFullString() + i (0-39)的值，对其md5,然后用该值+h(0-3)的值取hash。一致性hash实现的一个关键是如果将一个Invoker创建的replicaNumber个虚拟节点(hashcode)能够均匀分布在Hash环上
            for (Invoker<T> invoker : invokers) {
                String address = invoker.getUrl().getAddress();
                for (int i = 0; i < replicaNumber / 4; i++) {
                    byte[] digest = md5(address + i);
                    for (int h = 0; h < 4; h++) {
                        long m = hash(digest, h);
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
        }

        public Invoker<T> select(Invocation invocation) {
            // 根据调用参数，并根据hash.arguments配置值，获取指定的位置的参数值，追加一起返回。
            String key = toKey(invocation.getArguments());
            // 对Key进行md5签名。
            byte[] digest = md5(key);
            // 根据key进行选择调用者。
            return selectForKey(hash(digest, 0));
        }

        private String toKey(Object[] args) {
            StringBuilder buf = new StringBuilder();
            for (int i : argumentIndex) {
                if (i >= 0 && i < args.length) {
                    buf.append(args[i]);
                }
            }
            return buf.toString();
        }

        /**
         * 这里实现，应该可以不使用tailMap，更改后如下：如果想要了解TreeMap关于这一块的特性(tailMap、ceillingEntry、headMap)等API的详细解释，
         * 可以查看这篇博文：https://blog.csdn.net/prestigeding/article/details/80821576
         *
         * @param hash
         * @return
         */
        private Invoker<T> selectForKey(long hash) {
            // 对虚拟节点，从virtualInvokers中选取一个子集，subMap(hash,ture,lastKey,true),其实就是实现根据待查找hashcode(key)顺时针，选中大于等于指定key的第一个key。
            Map.Entry<Long, Invoker<T>> entry = virtualInvokers.tailMap(hash, true).firstEntry();
            // 如果未找到，则返回virtualInvokers第一个key。
            if (entry == null) {
                entry = virtualInvokers.firstEntry();
            }
            // 根据key返回指定的Invoker即可。
            return entry.getValue();
        }

//        private Invoker<T> selectForKey(long hash) {
//            Map.Entry<Long, Invoker<T>> entry = virtualInvokers.ceilingEntry(hash);
//            if(entry == null ) {
//                entry = virtualInvokers.firstEntry();
//            }
//            return entry.getValue();
//        }

        /**
         * Dubbo给出的hash 实现如下，由于能力有限，目前并未真正理解如下方法的实现依据：
         *
         * @param digest
         * @param number
         * @return
         */
        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                    | (digest[number * 4] & 0xFF))
                    & 0xFFFFFFFFL;
        }

        private byte[] md5(String value) {
            MessageDigest md5;
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            md5.reset();
            byte[] bytes;
            try {
                bytes = value.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            md5.update(bytes);
            return md5.digest();
        }

    }

}

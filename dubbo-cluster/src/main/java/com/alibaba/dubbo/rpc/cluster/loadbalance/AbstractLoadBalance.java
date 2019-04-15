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
import com.alibaba.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

/**
 * AbstractLoadBalance
 *
 */
public abstract class AbstractLoadBalance implements LoadBalance {

    /**
     * 该方法在uptime < warmup时被调用该方法的实现，就是在预热期间，根据启动时间，动态返回该服务提供者的权重，
     * 并且启动时间越长，返回的权重越接近weight，启动时间超过预热时间，则直接返回weight。
     *
     * @param uptime 服务提供者启动时间
     * @param warmup 设置的预热时间
     * @param weight 服务提供者的权重
     * @return weight
     */
    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
        int ww = (int) ((float) uptime / ((float) warmup / (float) weight));
        return ww < 1 ? 1 : (ww > weight ? weight : ww);
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        if (invokers == null || invokers.isEmpty())
            return null;
        if (invokers.size() == 1)
            return invokers.get(0);
        return doSelect(invokers, url, invocation);
    }

    protected abstract <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation);

    /**
     * dubbo 预热机制(权重)
     *
     * @param invoker
     * @param invocation
     * @return
     */
    protected int getWeight(Invoker<?> invoker, Invocation invocation) {
        // 获取服务提供者的权重weight
        int weight = invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.WEIGHT_KEY, Constants.DEFAULT_WEIGHT);
        if (weight > 0) {
            // 获取服务提供者的启动时间，在服务提供者启动时，会将启动时间戳存储在服务提供者的URL中，在服务发现(RegistryDirecotry)服务发现时，
            // 会将服务提供者的时间戳KEY，换成REMOTE_TIMESTAMP_KEY，避免与服务消费者的启动时间戳冲突。
            long timestamp = invoker.getUrl().getParameter(Constants.REMOTE_TIMESTAMP_KEY, 0L);
            if (timestamp > 0L) {
                int uptime = (int) (System.currentTimeMillis() - timestamp);
                // 获取服务提供者是否开启预热机制，通过服务提供者< dubbo:service warmup=""/>参数来设置，如果未设置，去默认值10 * 60 * 1000（10分钟）。
                int warmup = invoker.getUrl().getParameter(Constants.WARMUP_KEY, Constants.DEFAULT_WARMUP);
                if (uptime > 0 && uptime < warmup) {
                    // 如果服务提供者启动时间小于预热时间（预热期间），需要根据启动时间，来计算预热期间服务提供者的权重。
                    weight = calculateWarmupWeight(uptime, warmup, weight);
                }
            }
        }
        return weight;
    }

}

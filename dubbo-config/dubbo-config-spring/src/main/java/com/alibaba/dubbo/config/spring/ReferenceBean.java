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
package com.alibaba.dubbo.config.spring;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ConsumerConfig;
import com.alibaba.dubbo.config.ModuleConfig;
import com.alibaba.dubbo.config.MonitorConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.dubbo.config.spring.extension.SpringExtensionFactory;
import com.alibaba.dubbo.config.support.Parameter;

import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ReferenceFactoryBean
 *
 * ReferenceBean(服务消费者)比较特殊，实现了Spring与Bean生命周期相关的接口。
 *
 * InitializingBean，其声明的接口为afterPropertiesSet方法，顾名思义，就是在bean初始化所有属性之后调用。
 *
 * DisposableBean：其声明的接口为destroy()方法，在Spring BeanFactory销毁一个单例实例之前调用。
 *
 * ApplicationContextAware：其声明的接口为void setApplicationContext(ApplicationContext applicationContext)，
 *  实现了该接口，Spring容器在初始化Bean时会调用该方法，注入ApplicationContext，已方便该实例可以直接调用applicationContext获取其他Bean。
 *
 * FactoryBean：Spring初始化Bean的另外一种方式，例如dubbo:reference，需要返回的对象并不是ReferenceBean,
 *  而是要返回ref指定的代理类来执行业务操作，故这里使用FactoryBean非常合适。
 *  FactoryBean定义了如下三个方法：
 *  1、T getObject() throws Exception：获取需要返回的结果对象。
 *  2、Class getObjectType()：获取返回对象的类型。
 *  3、boolean isSingleton()：返回是否是单例。
 *
 * @export
 */
public class ReferenceBean<T> extends ReferenceConfig<T> implements FactoryBean, ApplicationContextAware, InitializingBean, DisposableBean {

    private static final long serialVersionUID = 213195494150089726L;

    private transient ApplicationContext applicationContext;

    public ReferenceBean() {
        super();
    }

    public ReferenceBean(Reference reference) {
        super(reference);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        SpringExtensionFactory.addApplicationContext(applicationContext);
    }

    @Override
    public Object getObject() throws Exception {
        return get();
    }

    @Override
    public Class<?> getObjectType() {
        return getInterfaceClass();
    }

    @Override
    @Parameter(excluded = true)
    public boolean isSingleton() {
        return true;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public void afterPropertiesSet() throws Exception {
        // 如果consumer为空，说明dubbo:reference标签未设置consumer属性，如果一个dubbo:consumer标签，则取该实例，如果存在多个dubbo:consumer 配置，
        // 则consumer必须设置，否则会抛出异常："Duplicate consumer configs"
        if (getConsumer() == null) {
            Map<String, ConsumerConfig> consumerConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ConsumerConfig.class, false, false);
            if (consumerConfigMap != null && consumerConfigMap.size() > 0) {
                ConsumerConfig consumerConfig = null;
                for (ConsumerConfig config : consumerConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault().booleanValue()) {
                        if (consumerConfig != null) {
                            throw new IllegalStateException("Duplicate consumer configs: " + consumerConfig + " and " + config);
                        }
                        consumerConfig = config;
                    }
                }
                if (consumerConfig != null) {
                    setConsumer(consumerConfig);
                }
            }
        }
        // 如果application为空,则尝试从BeanFactory中查询dubbo:application实例，如果存在多个dubbo:application配置，则抛出异常："Duplicate application configs"。
        if (getApplication() == null
                && (getConsumer() == null || getConsumer().getApplication() == null)) {
            Map<String, ApplicationConfig> applicationConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ApplicationConfig.class, false, false);
            if (applicationConfigMap != null && applicationConfigMap.size() > 0) {
                ApplicationConfig applicationConfig = null;
                for (ApplicationConfig config : applicationConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault().booleanValue()) {
                        if (applicationConfig != null) {
                            throw new IllegalStateException("Duplicate application configs: " + applicationConfig + " and " + config);
                        }
                        applicationConfig = config;
                    }
                }
                if (applicationConfig != null) {
                    setApplication(applicationConfig);
                }
            }
        }
        // 如果ServiceBean的module为空，则尝试从BeanFactory中查询dubbo:module实例，如果存在多个dubbo:module，则抛出异常："Duplicate module configs: "。
        if (getModule() == null
                && (getConsumer() == null || getConsumer().getModule() == null)) {
            Map<String, ModuleConfig> moduleConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ModuleConfig.class, false, false);
            if (moduleConfigMap != null && moduleConfigMap.size() > 0) {
                ModuleConfig moduleConfig = null;
                for (ModuleConfig config : moduleConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault().booleanValue()) {
                        if (moduleConfig != null) {
                            throw new IllegalStateException("Duplicate module configs: " + moduleConfig + " and " + config);
                        }
                        moduleConfig = config;
                    }
                }
                if (moduleConfig != null) {
                    setModule(moduleConfig);
                }
            }
        }
        // 尝试从BeanFactory中加载所有的注册中心，注意ServiceBean的List< RegistryConfig> registries属性，为注册中心集合
        if ((getRegistries() == null || getRegistries().isEmpty())
                && (getConsumer() == null || getConsumer().getRegistries() == null || getConsumer().getRegistries().isEmpty())
                && (getApplication() == null || getApplication().getRegistries() == null || getApplication().getRegistries().isEmpty())) {
            Map<String, RegistryConfig> registryConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, RegistryConfig.class, false, false);
            if (registryConfigMap != null && registryConfigMap.size() > 0) {
                List<RegistryConfig> registryConfigs = new ArrayList<RegistryConfig>();
                for (RegistryConfig config : registryConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault().booleanValue()) {
                        registryConfigs.add(config);
                    }
                }
                if (registryConfigs != null && !registryConfigs.isEmpty()) {
                    super.setRegistries(registryConfigs);
                }
            }
        }
        // 尝试从BeanFacotry中加载一个监控中心，填充ServiceBean的MonitorConfig monitor属性，如果存在多个dubbo:monitor配置，则抛出"Duplicate monitor configs: "。
        if (getMonitor() == null
                && (getConsumer() == null || getConsumer().getMonitor() == null)
                && (getApplication() == null || getApplication().getMonitor() == null)) {
            Map<String, MonitorConfig> monitorConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, MonitorConfig.class, false, false);
            if (monitorConfigMap != null && monitorConfigMap.size() > 0) {
                MonitorConfig monitorConfig = null;
                for (MonitorConfig config : monitorConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault().booleanValue()) {
                        if (monitorConfig != null) {
                            throw new IllegalStateException("Duplicate monitor configs: " + monitorConfig + " and " + config);
                        }
                        monitorConfig = config;
                    }
                }
                if (monitorConfig != null) {
                    setMonitor(monitorConfig);
                }
            }
        }
        // 判断是否初始化，如果为初始化，则调用getObject()方法
        Boolean b = isInit();
        if (b == null && getConsumer() != null) {
            b = getConsumer().isInit();
        }
        if (b != null && b.booleanValue()) {
            // 该方法也是FactoryBean定义的方法，ReferenceBean是dubbo:reference所真实引用的类(interface)的实例工程，
            // getObject方法返回的是interface的实例，而不是ReferenceBean实例。
            // ReferenceBean#getObject()方法直接调用其父类的get方法,get方法内部调用init()方法进行初始化
            getObject();
        }
    }

    @Override
    public void destroy() {
        // do nothing
    }
}

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
import com.alibaba.dubbo.config.ModuleConfig;
import com.alibaba.dubbo.config.MonitorConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.ProviderConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.ServiceConfig;
import com.alibaba.dubbo.config.annotation.Service;
import com.alibaba.dubbo.config.spring.context.event.ServiceBeanExportedEvent;
import com.alibaba.dubbo.config.spring.extension.SpringExtensionFactory;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.support.AbstractApplicationContext;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ServiceFactoryBean
 *
 * ServiceBean(服务提供者)比较特殊，实现了Spring与Bean生命周期相关的接口。
 *
 * InitializingBean，其声明的接口为afterPropertiesSet方法，顾名思义，就是在bean初始化所有属性之后调用。
 *
 * DisposableBean：其声明的接口为destroy()方法，在Spring BeanFactory销毁一个单例实例之前调用。
 *
 * ApplicationContextAware：其声明的接口为void setApplicationContext(ApplicationContext applicationContext)，
 *  实现了该接口，Spring容器在初始化Bean时会调用该方法，注入ApplicationContext，已方便该实例可以直接调用applicationContext获取其他Bean。
 *
 * ApplicationListener：容器重新刷新时执行事件函数。
 *
 * BeanNameAware：其声明的接口为：void setBeanName(String name)，实现该接口的Bean，其实例可以获取该实例在BeanFactory的id或name。
 *
 * @export
 */
public class ServiceBean<T> extends ServiceConfig<T> implements InitializingBean, DisposableBean,
        ApplicationContextAware, ApplicationListener<ContextRefreshedEvent>, BeanNameAware,
        ApplicationEventPublisherAware {

    private static final long serialVersionUID = 213195494150089726L;

    private static transient ApplicationContext SPRING_CONTEXT;

    private final transient Service service;

    private transient ApplicationContext applicationContext;

    private transient String beanName;

    private transient boolean supportedApplicationListener;

    private ApplicationEventPublisher applicationEventPublisher;

    public ServiceBean() {
        super();
        this.service = null;
    }

    public ServiceBean(Service service) {
        super(service);
        this.service = service;
    }

    public static ApplicationContext getSpringContext() {
        return SPRING_CONTEXT;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        SpringExtensionFactory.addApplicationContext(applicationContext);
        if (applicationContext != null) {
            SPRING_CONTEXT = applicationContext;
            try {
                Method method = applicationContext.getClass().getMethod("addApplicationListener", new Class<?>[]{ApplicationListener.class}); // backward compatibility to spring 2.0.1
                method.invoke(applicationContext, new Object[]{this});
                supportedApplicationListener = true;
            } catch (Throwable t) {
                if (applicationContext instanceof AbstractApplicationContext) {
                    try {
                        Method method = AbstractApplicationContext.class.getDeclaredMethod("addListener", new Class<?>[]{ApplicationListener.class}); // backward compatibility to spring 2.0.1
                        if (!method.isAccessible()) {
                            method.setAccessible(true);
                        }
                        method.invoke(applicationContext, new Object[]{this});
                        supportedApplicationListener = true;
                    } catch (Throwable t2) {
                    }
                }
            }
        }
    }

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    /**
     * Gets associated {@link Service}
     *
     * @return associated {@link Service}
     */
    public Service getService() {
        return service;
    }

    /**
     * delay=-1的处理逻辑（基于Spring事件机制触发）。
     *
     * @param event
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (isDelay() && !isExported() && !isUnexported()) {
            if (logger.isInfoEnabled()) {
                logger.info("The service ready on spring started. service: " + getInterface());
            }
            export();
        }
    }

    /**
     * 如果有设置dubbo:service或dubbo:provider的属性delay，或配置delay为-1,都表示启用延迟机制，单位为毫秒，
     * 设置为-1，表示等到Spring容器初始化后再暴露服务。从这里也可以看出，Dubbo暴露服务的处理入口为ServiceBean#export->ServiceConfig#export。
     * @return
     */
    private boolean isDelay() {
        Integer delay = getDelay();
        ProviderConfig provider = getProvider();
        if (delay == null && provider != null) {
            delay = provider.getDelay();
        }
        return supportedApplicationListener && (delay == null || delay == -1);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // 如果provider为空，说明dubbo:service标签未设置provider属性，如果一个dubbo:provider标签，则取该实例，
        // 如果存在多个dubbo:provider配置则provider属性不能为空，否则抛出异常："Duplicate provider configs"。
        if (getProvider() == null) {
            Map<String, ProviderConfig> providerConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ProviderConfig.class, false, false);
            if (providerConfigMap != null && providerConfigMap.size() > 0) {
                Map<String, ProtocolConfig> protocolConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ProtocolConfig.class, false, false);
                if ((protocolConfigMap == null || protocolConfigMap.size() == 0)
                        && providerConfigMap.size() > 1) { // backward compatibility
                    List<ProviderConfig> providerConfigs = new ArrayList<ProviderConfig>();
                    for (ProviderConfig config : providerConfigMap.values()) {
                        if (config.isDefault() != null && config.isDefault().booleanValue()) {
                            providerConfigs.add(config);
                        }
                    }
                    if (!providerConfigs.isEmpty()) {
                        setProviders(providerConfigs);
                    }
                } else {
                    ProviderConfig providerConfig = null;
                    for (ProviderConfig config : providerConfigMap.values()) {
                        if (config.isDefault() == null || config.isDefault().booleanValue()) {
                            if (providerConfig != null) {
                                throw new IllegalStateException("Duplicate provider configs: " + providerConfig + " and " + config);
                            }
                            providerConfig = config;
                        }
                    }
                    if (providerConfig != null) {
                        setProvider(providerConfig);
                    }
                }
            }
        }
        // 如果application为空,则尝试从BeanFactory中查询dubbo:application实例，
        // 如果存在多个dubbo:application配置，则抛出异常："Duplicate application configs"。
        if (getApplication() == null
                && (getProvider() == null || getProvider().getApplication() == null)) {
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
        // 如果ServiceBean的module为空，则尝试从BeanFactory中查询dubbo:module实例，
        // 如果存在多个dubbo:module，则抛出异常："Duplicate module configs: "。
        if (getModule() == null
                && (getProvider() == null || getProvider().getModule() == null)) {
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
        // 尝试从BeanFactory中加载所有的注册中心，注意ServiceBean 继承自AbstractInterfaceConfig 的List< RegistryConfig> registries属性，为注册中心集合
        if ((getRegistries() == null || getRegistries().isEmpty())
                && (getProvider() == null || getProvider().getRegistries() == null || getProvider().getRegistries().isEmpty())
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
        // 尝试从BeanFacotry中加载一个监控中心，填充ServiceBean的MonitorConfig monitor属性，
        // 如果存在多个dubbo:monitor配置，则抛出"Duplicate monitor configs: "。
        if (getMonitor() == null
                && (getProvider() == null || getProvider().getMonitor() == null)
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
        // 尝试从BeanFactory中加载所有的协议，注意：ServiceBean的父类AbstractServiceConfigList< ProtocolConfig> protocols是一个集合，也即一个服务可以通过多种协议暴露给消费者。
        if ((getProtocols() == null || getProtocols().isEmpty())
                && (getProvider() == null || getProvider().getProtocols() == null || getProvider().getProtocols().isEmpty())) {
            Map<String, ProtocolConfig> protocolConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ProtocolConfig.class, false, false);
            if (protocolConfigMap != null && protocolConfigMap.size() > 0) {
                List<ProtocolConfig> protocolConfigs = new ArrayList<ProtocolConfig>();
                for (ProtocolConfig config : protocolConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault().booleanValue()) {
                        protocolConfigs.add(config);
                    }
                }
                if (protocolConfigs != null && !protocolConfigs.isEmpty()) {
                    super.setProtocols(protocolConfigs);
                }
            }
        }
        // 设置ServiceBean的path属性，path属性存放的是dubbo:service的beanName（dubbo:service id)。
        if (getPath() == null || getPath().length() == 0) {
            if (beanName != null && beanName.length() > 0
                    && getInterface() != null && getInterface().length() > 0
                    && beanName.startsWith(getInterface())) {
                setPath(beanName);
            }
        }
        // 如果为启用延迟暴露机制，则调用export暴露服务。首先看一下isDelay的实现，然后重点分析export的实现原理（服务暴露的整个实现原理）。
        if (!isDelay()) {
            // 调用链：ServiceBean#afterPropertiesSet->ServiceConfig#export
            export();
        }
    }

    /**
     * Get the name of {@link ServiceBean}
     *
     * @return {@link ServiceBean}'s name
     * @since 2.6.5
     */
    public String getBeanName() {
        return this.beanName;
    }

    /**
     * @since 2.6.5
     */
    @Override
    public void export() {
        super.export();
        // Publish ServiceBeanExportedEvent
        publishExportEvent();
    }

    /**
     * @since 2.6.5
     */
    private void publishExportEvent() {
        ServiceBeanExportedEvent exportEvent = new ServiceBeanExportedEvent(this);
        applicationEventPublisher.publishEvent(exportEvent);
    }

    @Override
    public void destroy() throws Exception {
        // This will only be called for singleton scope bean, and expected to be called by spring shutdown hook when BeanFactory/ApplicationContext destroys.
        // We will guarantee dubbo related resources being released with dubbo shutdown hook.
        //unexport();
    }

    // merged from dubbox
    @Override
    protected Class getServiceClass(T ref) {
        if (AopUtils.isAopProxy(ref)) {
            return AopUtils.getTargetClass(ref);
        }
        return super.getServiceClass(ref);
    }

    /**
     * @param applicationEventPublisher
     * @since 2.6.5
     */
    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }
}

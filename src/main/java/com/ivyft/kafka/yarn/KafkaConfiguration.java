/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.kafka.yarn;

import com.ivyft.kafka.yarn.util.PropertyUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

/**
 *
 * Kafka 的一个配置
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-13
 * Time: 上午8:58
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KafkaConfiguration extends PropertiesConfiguration {


    /**
     * Log
     */
    private final static Logger LOG = LoggerFactory.getLogger(KafkaConfiguration.class);



    public KafkaConfiguration() {
    }


    public KafkaConfiguration(File file) {
        this();

        setFile(file);
        try {
            load(file);
        } catch (ConfigurationException e) {
            throw new RuntimeException("unable to load kafka-yarn.properties", e);
        }
    }


    public KafkaConfiguration(String path) {
        this();

        URL resource = PropertyUtil.getClasspathResource(path);
        setURL(resource);
        try {
            load(resource);
        } catch (ConfigurationException e) {
            throw new RuntimeException("unable to load kafka-yarn.properties", e);
        }
    }

    public KafkaConfiguration(Properties properties, String filePath) {
        this();

        if(filePath != null) {
            File file = new File(filePath);
            setFile(file);
            try {
                load(file);
            } catch (ConfigurationException e) {
                throw new RuntimeException("unable to load kafka-yarn.properties", e);
            }
        }

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            setProperty(entry.getKey().toString(), entry.getValue().toString());
        }
    }


    public boolean containsProperty(String key) {
        return this.containsKey(key);
    }


    public String getProperty(String key, String defaultValue) {
        Object value = getProperty(key);
        if (value == null) {
            value = defaultValue;
        }
        return value.toString();
    }


    public File getFile(String key) {
        String property = System.getProperty(key);
        if(StringUtils.isBlank(property)) {
            property = getString(key);
        }
        File file = new File(property);
        LOG.info(key + " dir: " + file.getAbsolutePath());
        if(!file.exists()) {
            if(!file.mkdirs()) {
                throw new IllegalArgumentException("can not mkdir: " + file.getAbsolutePath());
            }
        }
        return file;
    }
}

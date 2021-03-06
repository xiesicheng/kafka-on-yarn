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
package com.ivyft.kafka.yarn.util;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;


/**
 *
 *
 *
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
public class PropertyUtil {

    public static Properties loadProperties(String path) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if(classLoader == null) {
            classLoader = PropertyUtil.class.getClassLoader();
        }

        URL resource =  classLoader.getResource(path);
        if(resource == null) {
            try {
                resource = new File(new File("./conf"), path).toURL();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }

        final Properties properties = new Properties();
        try {
            InputStream inputStream = resource.openStream();
            properties.load(inputStream);
            IOUtils.closeQuietly(inputStream);
            return properties;
        } catch (final Exception e) {
            throw new RuntimeException("unable to load kata.properties", e);
        }
    }



    public static void printListFile(File dir, int floor) {
        File[] files = dir.listFiles();
        if(files == null) {
            return;
        }
        for (File file : files) {
            if(file.isFile()) {
                System.out.println(getTree(floor) + file.getAbsolutePath());
            } else {
                printListFile(file, ++floor);
            }
        }
    }


    public static String getTree(int f) {
        StringBuilder str = new StringBuilder("|");
        for (int i = 0; i < f; i++) {
            str.append("--");
        }
        return str.toString();
    }


    public static String getPropertiesFilePath(final String path) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if(classLoader == null) {
            classLoader = PropertyUtil.class.getClassLoader();
        }

        URL resource =  classLoader.getResource(path);
        if(resource == null) {
            try {
                resource = new File(new File("./conf"), path).toURL();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }

        if (resource == null) {
            throw new RuntimeException(path + " not in classpath");
        }
        return resource.toString();
    }

    public static Properties loadProperties(final File file) {
        final Properties properties = new Properties();
        try {
            FileInputStream inStream = new FileInputStream(file);
            properties.load(inStream);
            return properties;
        } catch (final IOException e) {
            throw new RuntimeException("unable to load kata.properties", e);
        }
    }


    public static ClassLoader getClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if(classLoader == null) {
            classLoader = PropertyUtil.class.getClassLoader();
        }
        return classLoader;
    }



    public static String getClasspath() {
        return getClassLoader().getResource("").getPath();
    }



    public static URL getClasspathResource(String path) {
        return getClassLoader().getResource(path);
    }

}

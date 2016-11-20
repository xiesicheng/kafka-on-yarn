/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.ivyft.kafka.yarn;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.*;
import java.net.URL;
import java.util.*;


/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/28
 * Time: 19:51
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class Util {
    private static final String JETTY_CONF_PATH_STRING = "conf" + Path.SEPARATOR + "kafka.xml";


    public static List<String> getJettyHomeJars(FileSystem fs, Path kafkaHome, String kafkaVersion) throws IOException, RuntimeException {
        List<String> jars = new ArrayList<String>();
        PathFilter jarFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith(".jar");
            }
        };
        FileStatus[] statuses = fs.listStatus(kafkaHome, jarFilter);

        if(statuses != null) {
            for (FileStatus statuse : statuses) {
                String path = statuse.getPath().toUri().getPath();
                jars.add(path.replaceAll(kafkaHome.toString(), ""));
            }
        }

        statuses = fs.listStatus(new Path(kafkaHome, "lib"), jarFilter);

        if(statuses != null) {
            for (FileStatus statuse : statuses) {
                String path = statuse.getPath().toUri().getPath();
                jars.add(path.replaceAll(kafkaHome.toString(), ""));
            }
        }
        return jars;
    }


    public static LocalResource newYarnAppResource(FileSystem fs, Path path)
            throws IOException {
        return Util.newYarnAppResource(fs, path, LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION);
    }


    public static LocalResource newYarnAppResource(FileSystem fs,
                                            Path path,
                                            LocalResourceType type,
                                            LocalResourceVisibility vis) throws IOException {
        Path qualified = fs.makeQualified(path);
        FileStatus status = fs.getFileStatus(qualified);
        LocalResource resource = Records.newRecord(LocalResource.class);
        resource.setType(type);
        resource.setVisibility(vis);
        resource.setResource(ConverterUtils.getYarnUrlFromPath(qualified));
        resource.setTimestamp(status.getModificationTime());
        resource.setSize(status.getLen());
        return resource;
    }


    public static Path copyClasspathConf(FileSystem fs,
                                            String appHome)
            throws IOException {
        Path confDst = new Path(fs.getHomeDirectory(), appHome);
        fs.mkdirs(confDst);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if(classLoader == null) {
            classLoader = Util.class.getClassLoader();
        }

        URL classpath = classLoader.getResource("");

        File file = new File(classpath.getPath());
        File[] files = file.listFiles();
        if(files != null) {
            for (File file1 : files) {
                if(file1.isDirectory()) {
                    continue;
                }
                fs.copyFromLocalFile(new Path(file1.getAbsolutePath()), new Path(confDst, file1.getName()));
            }
        }

        return confDst;
    }


    private static List<String> buildCommandPrefix(KafkaConfiguration conf)
            throws IOException {
        List<String> toRet = new ArrayList<String>();
        String java_home = conf.getProperty("katta.yarn.java_home", "");
        if (StringUtils.isNotBlank(java_home)) {
            toRet.add(java_home + "/bin/java");
        } else {
            toRet.add(ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java");
        }
        return toRet;
    }



    public static List<String> buildNodeCommands(KafkaConfiguration conf,
                                                 int mb,
                                                 int brokerId,
                                                 String kafkaConf) throws IOException {
        List<String> toRet = buildCommandPrefix(conf);
        toRet.add("-Xmx" + mb + "m");
        toRet.add("-Xms" + mb + "m");
        toRet.add(conf.getProperty("kafka.yarn.instance.jvm.opts", ""));
        toRet.add("-Dkafka.root.logger=INFO,DRFA");
        toRet.add("-Dkafka.home=" + ApplicationConstants.Environment.PWD.$());
        toRet.add("-Dkafka.base=" + ApplicationConstants.Environment.PWD.$());
        toRet.add("-Dkafka.host=" + ApplicationConstants.Environment.NM_HOST.$());
        toRet.add("-Dkafka.port=" + conf.getProperty("kafka.port", "8080"));
        toRet.add("-Dkafka.user=" + ApplicationConstants.Environment.LOGNAME.$());
        toRet.add("-Dkafka.logs=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        toRet.add("-Dkafka.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        toRet.add("-Dkafka.log.file=" + "kafka-" + ApplicationConstants.Environment.USER.$()
                + "-" +
                ApplicationConstants.Environment.NM_HOST.$() + ".log");

        //start-kafka -app_attempt_id appattempt_1479611954389_0001_000001 -id 0 -pfile /Volumes/Study/Work/IntelliJWork/kafka-yarn/conf/server.properties
        toRet.add(KafkaOnYarn.class.getName());
        toRet.add("start-kafka");

        toRet.add("-id");
        toRet.add(String.valueOf(brokerId));
        toRet.add("-logdir");
        toRet.add(ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        toRet.add("-pfile");
        toRet.add(ApplicationConstants.Environment.PWD.$() + File.separator + "conf" + File.separator + kafkaConf);
        toRet.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        toRet.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        return toRet;
    }


    private static interface FileVisitor {
        public void visit(File file);
    }

    private static List<String> findAllJarsInPaths(String... pathStrs) {
        final LinkedHashSet<String> pathSet = new LinkedHashSet<String>();

        FileVisitor visitor = new FileVisitor() {

            @Override
            public void visit(File file) {
                String name = file.getName();
                if (name.endsWith(".jar")) {
                    pathSet.add(file.getPath());
                }
            }
        };

        for (String path : pathStrs) {
            File file = new File(path);
            traverse(file, visitor);
        }

        final List<String> toRet = new ArrayList<String>();
        for (String p : pathSet) {
            toRet.add(p);
        }
        return toRet;
    }

    private static void traverse(File file, FileVisitor visitor) {
        if (file.isDirectory()) {
            File childs[] = file.listFiles();
            if (childs.length > 0) {
                for (int i = 0; i < childs.length; i++) {
                    File child = childs[i];
                    traverse(child, visitor);
                }
            }
        } else {
            visitor.visit(file);
        }
    }

    public static String getApplicationHomeForId(String id) {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException(
                    "The ID of the application cannot be empty.");
        }
        return ".KafkaOnYarn" + Path.SEPARATOR + id;
    }

    /**
     * Returns a boolean to denote whether a cache file is visible to all(public)
     * or not
     *
     * @param fs   Hadoop file system
     * @param path file path
     * @return true if the path is visible to all, false otherwise
     * @throws IOException
     */
    public static boolean isPublic(FileSystem fs, Path path) throws IOException {
        //the leaf level file should be readable by others
        if (!checkPermissionOfOther(fs, path, FsAction.READ)) {
            return false;
        }
        return ancestorsHaveExecutePermissions(fs, path.getParent());
    }

    /**
     * Checks for a given path whether the Other permissions on it
     * imply the permission in the passed FsAction
     *
     * @param fs
     * @param path
     * @param action
     * @return true if the path in the uri is visible to all, false otherwise
     * @throws IOException
     */
    private static boolean checkPermissionOfOther(FileSystem fs, Path path,
                                                  FsAction action) throws IOException {
        FileStatus status = fs.getFileStatus(path);
        FsPermission perms = status.getPermission();
        FsAction otherAction = perms.getOtherAction();
        if (otherAction.implies(action)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true if all ancestors of the specified path have the 'execute'
     * permission set for all users (i.e. that other users can traverse
     * the directory hierarchy to the given path)
     */
    public static boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path) throws IOException {
        Path current = path;
        while (current != null) {
            //the subdirs in the path should have execute permissions for others
            if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE)) {
                return false;
            }
            current = current.getParent();
        }
        return true;
    }

    public static void redirectStreamAsync(final InputStream input, final PrintStream output) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Scanner scanner = new Scanner(input);
                while (scanner.hasNextLine()) {
                    output.println(scanner.nextLine());
                }
            }
        }).start();
    }
}

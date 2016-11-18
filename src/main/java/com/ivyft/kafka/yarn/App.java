package com.ivyft.kafka.yarn;

import org.apache.commons.cli.ParseException;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/15
 * Time: 12:03
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class App {


    public static void main(String[] args) throws ParseException {
        KafkaOnYarn.main(new String[]{"yarn-stop", "--all",
                "-appid", "application_1449726747657_0008",
                "-n", "container_1449726747657_0008_01_000001"});
    }
}

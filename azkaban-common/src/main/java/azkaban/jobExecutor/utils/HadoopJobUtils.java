package azkaban.jobExecutor.utils;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by bianzexin on 16/5/6.
 */
public class HadoopJobUtils {

    public static String MATCH_ALL_REGEX = ".*";

    public static String MATCH_NONE_REGEX = ".^";

    public static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";

    // the regex to look for while looking for application id's in the hadoop log
    public static final Pattern APPLICATION_ID_PATTERN = Pattern
            .compile("^(application_\\d+_\\d+).*");

    /**
     * <pre>
     * Takes in a log file, will grep every line to look for the application_id pattern.
     * If it finds multiple, it will return all of them, de-duped (this is possible in the case of pig jobs)
     * This can be used in conjunction with the @killJobOnCluster method in this file.
     * </pre>
     *
     * @param logFilePath
     * @return a Set. May be empty, but will never be null
     */
    public static Set<String> findApplicationIdFromLog(String logFilePath, Logger log) {

        File logFile = new File(logFilePath);

        if (!logFile.exists()) {
            throw new IllegalArgumentException("the logFilePath does not exist: " + logFilePath);
        }
        if (!logFile.isFile()) {
            throw new IllegalArgumentException("the logFilePath specified  is not a valid file: "
                    + logFilePath);
        }
        if (!logFile.canRead()) {
            throw new IllegalArgumentException("unable to read the logFilePath specified: " + logFilePath);
        }

        BufferedReader br = null;
        Set<String> applicationIds = new HashSet<String>();

        try {
            br = new BufferedReader(new FileReader(logFile));
            String line;

            // finds all the application IDs
            while ((line = br.readLine()) != null) {
                String [] inputs = line.split("\\s");
                if (inputs != null) {
                    for (String input : inputs) {
                        Matcher m = APPLICATION_ID_PATTERN.matcher(input);
                        if (m.find()) {
                            String appId = m.group(1);
                            applicationIds.add(appId);
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("Error while trying to find applicationId for log", e);
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (Exception e) {
                // do nothing
            }
        }
        return applicationIds;
    }

    public static Set<String> killAllSpawnedHadoopJobs(String logFilePath, Logger log) {
        Set<String> allSpawnedJobs = HadoopJobUtils.findApplicationIdFromLog(logFilePath, log);
        log.info("applicationIds to kill: " + allSpawnedJobs);

        for (String appId : allSpawnedJobs) {
            try {
                killJobOnCluster(appId, log);
            } catch (Throwable t) {
                log.warn("something happened while trying to kill this job: " + appId, t);
            }
        }

        return allSpawnedJobs;
    }


    public static void killJobOnCluster(String applicationId, Logger log) throws YarnException,
            IOException {

        YarnConfiguration yarnConf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        String[] split = applicationId.split("_");
        ApplicationId aid = ApplicationId.newInstance(Long.parseLong(split[1]),
                Integer.parseInt(split[2]));

        log.info("start klling application: " + aid);
        yarnClient.killApplication(aid);
        log.info("successfully killed application: " + aid);
        yarnClient.close();
    }



    /**
     * Based on the HADOOP_SECURITY_MANAGER_CLASS_PARAM setting in the incoming props, finds the
     * correct HadoopSecurityManager Java class
     *
     * @param props
     * @param log
     * @return a HadoopSecurityManager object. Will throw exception if any errors occur (including not
     *         finding a class)
     * @throws RuntimeException
     *           : If any errors happen along the way.
     */
    public static HadoopSecurityManager loadHadoopSecurityManager(Props props, Logger log)
            throws RuntimeException {

        Class<?> hadoopSecurityManagerClass = props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true,
                HadoopJobUtils.class.getClassLoader());
        log.info("Loading hadoop security manager " + hadoopSecurityManagerClass.getName());
        HadoopSecurityManager hadoopSecurityManager = null;

        try {
            Method getInstanceMethod = hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
            hadoopSecurityManager = (HadoopSecurityManager) getInstanceMethod.invoke(
                    hadoopSecurityManagerClass, props);
        } catch (InvocationTargetException e) {
            String errMsg = "Could not instantiate Hadoop Security Manager "
                    + hadoopSecurityManagerClass.getName() + e.getCause();
            log.error(errMsg);
            throw new RuntimeException(errMsg, e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return hadoopSecurityManager;

    }
}

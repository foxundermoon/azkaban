package azkaban.jobExecutor.utils;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import azkaban.utils.Props;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.ProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;

/**
 * Created by bianzexin on 16/5/6.
 */
public class HadoopSecureWrapperUtils {

    /**
     * Perform all the magic required to get the proxyUser in a securitized grid
     *
     * @param userToProxy
     * @return a UserGroupInformation object for the specified userToProxy, which will also contain
     *         the logged in user's tokens
     * @throws IOException
     */
    private static UserGroupInformation createSecurityEnabledProxyUser(String userToProxy, String filelocation, Logger log
    ) throws IOException {

        if (!new File(filelocation).exists()) {
            throw new RuntimeException("hadoop token file doesn't exist.");
        }

        log.info("Found token file.  Setting " + HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY
                + " to " + filelocation);
        System.setProperty(HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY, filelocation);

        UserGroupInformation loginUser = null;

        loginUser = UserGroupInformation.getLoginUser();
        log.info("Current logged in user is " + loginUser.getUserName());

        UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(userToProxy, loginUser);

        for (Token<?> token : loginUser.getTokens()) {
            proxyUser.addToken(token);
        }
        return proxyUser;
    }

    /**
     * Sets up the UserGroupInformation proxyUser object so that calling code can do doAs returns null
     * if the jobProps does not call for a proxyUser
     *
     * @param jobPropsIn
     * @param tokenFile
     *          pass tokenFile if known. Pass null if the tokenFile is in the environmental variable
     *          already.
     * @param log
     * @return returns null if no need to run as proxyUser, otherwise returns valid proxyUser that can
     *         doAs
     */
    public static UserGroupInformation setupProxyUser(Properties jobProps,
                                                      String tokenFile, Logger log) {
        UserGroupInformation proxyUser = null;

        if (!HadoopSecureWrapperUtils.shouldProxy(jobProps)) {
            log.info("submitting job as original submitter, not proxying");
            return proxyUser;
        }

        // set up hadoop related configurations
        final Configuration conf = new Configuration();
        UserGroupInformation.setConfiguration(conf);
        boolean securityEnabled = UserGroupInformation.isSecurityEnabled();

        // setting up proxy user if required
        try {
            String userToProxy = null;
            userToProxy = jobProps.getProperty(HadoopSecurityManager.USER_TO_PROXY);
            if (securityEnabled) {
                proxyUser =
                        HadoopSecureWrapperUtils.createSecurityEnabledProxyUser(
                                userToProxy, tokenFile, log);
                log.info("security enabled, proxying as user " + userToProxy);
            } else {
                proxyUser = UserGroupInformation.createRemoteUser(userToProxy);
                log.info("security not enabled, proxying as user " + userToProxy);
            }
        } catch (IOException e) {
            log.error("HadoopSecureWrapperUtils.setupProxyUser threw an IOException",
                    e);
        }

        return proxyUser;
    }

    /**
     * Loading the properties file, which is a combination of the jobProps file and sysProps file
     *
     * @return a Property file, which is the combination of the jobProps file and sysProps file
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static Properties loadAzkabanProps() throws IOException, FileNotFoundException {
        String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
        Properties props = new Properties();
        props.load(new BufferedReader(new FileReader(propsFile)));
        return props;
    }

    /**
     * Looks for particular properties inside the Properties object passed in, and determines whether
     * proxying should happen or not
     *
     * @param props
     * @return a boolean value of whether the job should proxy or not
     */
    public static boolean shouldProxy(Properties props) {
        String shouldProxy = props.getProperty(HadoopSecurityManager.ENABLE_PROXYING);
        return shouldProxy != null && shouldProxy.equals("true");
    }

    public static File getHadoopTokens(HadoopSecurityManager hadoopSecurityManager, Props props,
                                       Logger log) throws HadoopSecurityManagerException {

        File tokenFile = null;
        try {
            tokenFile = File.createTempFile("mr-azkaban", ".token");
        } catch (Exception e) {
            throw new HadoopSecurityManagerException("Failed to create the token file.", e);
        }

        hadoopSecurityManager.prefetchToken(tokenFile, props, log);

        return tokenFile;
    }
}

package azkaban.utils;

import azkaban.server.AzkabanServer;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;

/**
 * @author lcs
 * @class AzkabanConfigFileMonitor
 * @date 2016/11/21.
 * @describe 监听配置文件，如果配置文件修改，则重新加载配置文件
 */
public class AzkabanConfigFileMonitor {
    private static final Logger logger = Logger.getLogger(AzkabanConfigFileMonitor.class);
    private static final String AZKABAN_CONFIG_FILE_MONIITE_INTERVAL = "azkaban.config.file.monite.interval";
    private static final String AZKABAN_CONFIG_FILE_PATH = "azkaban.config.file.path";
    private static String configFilePath = "/conf";
    public static Props props;
    public static final String monitorFileName = "azkaban.properties";

    public static void azkabanConfigFileMonitor(String[] args, Props props1) {
        try {
            props = props1;
            configFilePath = props.getString(AZKABAN_CONFIG_FILE_PATH);//配置文件路径
            long interval = props.getLong(AZKABAN_CONFIG_FILE_MONIITE_INTERVAL, 5000);//监听间隔
            String dirPath = System.getProperty("user.dir") + configFilePath;
            logger.error("azkabanConfigFileMonitor method, monitor path= " + dirPath);
            //构造观察类:主要提供要观察的文件或目录，当然还有详细信息的filter
            FileAlterationObserver observer = new FileAlterationObserver(new File(dirPath), new FileFilterImpl());
            //构造收听类
            FileListenerAdaptor listener = new FileListenerAdaptor(args);
            //为观察对象添加收听对象
            observer.addListener(listener);
            //配置Monitor，第一个参数单位是毫秒，是监听的间隔；第二个参数就是绑定我们之前的观察对象。
            FileAlterationMonitor fileMonitor = new FileAlterationMonitor(interval, new FileAlterationObserver[]{observer});
            //启动开始监听
            fileMonitor.start();
        } catch (Exception e) {
            logger.error("azkabanConfigFileMonitor method error", e);
        }

    }

    public static Props loadProps(String[] args) {
        props = AzkabanServer.loadProps(args);//加载配置文件
        return props;
    }

    public static Props getProps() {
        return props;
    }

    static class FileFilterImpl implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            if (pathname.getName().equals(monitorFileName)) {
                return true;
            }
            return false;
        }
    }

    static class FileListenerAdaptor extends FileAlterationListenerAdaptor {
        private String[] args;
        public static Props props;

        public FileListenerAdaptor(String[] args) {
            this.args = args;
        }

        @Override
        public void onStart(FileAlterationObserver observer) {
            super.onStart(observer);
        }

        @Override
        public void onDirectoryCreate(File directory) {
            logger.error("onDirectoryCreate method,file=" + directory);
            super.onDirectoryCreate(directory);
        }

        @Override
        public void onDirectoryChange(File directory) {
            logger.error("onDirectoryChange method,file=" + directory);
            super.onDirectoryChange(directory);
        }

        @Override
        public void onDirectoryDelete(File directory) {
            logger.error("onDirectoryDelete method,file=" + directory);
            super.onDirectoryDelete(directory);
        }

        @Override
        public void onFileCreate(File file) {
            logger.error("onFileCreate method,file=" + file);
            super.onFileCreate(file);
        }

        @Override
        public void onFileChange(File file) {
            logger.error("onFileChange method,file=" + file + " has updated,last modified time=" + file.lastModified());
            loadProps(args);//重新加载配置文件
            // super.onFileChange(file);
        }

        @Override
        public void onFileDelete(File file) {
            logger.error("onFileDelete method,file=" + file);
            super.onFileDelete(file);
        }

        @Override
        public void onStop(FileAlterationObserver observer) {
            super.onStop(observer);
        }


    }
}

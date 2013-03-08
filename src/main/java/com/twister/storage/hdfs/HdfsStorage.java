package com.twister.storage.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import com.twister.utils.AppsConfig;

/**
 * @author guoqing
 */
public class HdfsStorage {
    public static final String  STORAGE_HDFS_DIR = AppsConfig.getInstance().getValue("hdfs.dir");
    public static final String  STORAGE_HDFS_PATH =AppsConfig.getInstance().getValue("hdfs.path");
    public static final String  STORAGE_HDFS_USER =AppsConfig.getInstance().getValue("hdfs.user");
    private FileSystem fs;
    private String dir; 
    
    public void init() {
        try {
            String pathString =STORAGE_HDFS_PATH;

            String user =STORAGE_HDFS_USER;
            if (user != null && user.trim().length() > 0) {
                System.setProperty("HADOOP_USER_NAME", user.trim());
            }
            fs = new Path(pathString).getFileSystem(new Configuration());

            String dirString =STORAGE_HDFS_DIR;
            if (dirString != null && dirString.trim().length() > 0) {
                dir = dirString.trim();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public InputStream open(String path) {
        try {
            return fs.open(new Path(prependDir(path)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

  
    public OutputStream create(String path) {
        try {
            return fs.create(new Path(prependDir(path)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

  
    public List<String> list(String path) {
        try {
            FileStatus[] fileStatuses = fs.listStatus(new Path(prependDir(path)));
            if (fileStatuses == null) {
                return Collections.emptyList();
            }

            List<String> result = new ArrayList<String>();
            for (FileStatus fileStatus : fileStatuses) {
                result.add(fileStatus.getPath().getName());
            }

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

  
    public void delete(String path) {
        try {
            fs.delete(new Path(prependDir(path)), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

  
    public void mkdirs(String path) {
        try {
            fs.mkdirs(new Path(prependDir(path)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public boolean isSupportDistributed() {
        return true;
    }

    private String prependDir(String path) {
        if (!dir.endsWith("/") && !path.startsWith("/")) {
            path = "/" + path;
        }
        return dir + path;
    }
}
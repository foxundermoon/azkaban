/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.execapp;

import azkaban.flow.CommonJobProperties;
import azkaban.project.ProjectFileHandler;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.zip.ZipFile;

public class ProjectVersion implements Comparable<ProjectVersion> {
  private final int projectId;
  private final int version;
  private File installedDir;

  public ProjectVersion(int projectId, int version) {
    this.projectId = projectId;
    this.version = version;
  }

  public ProjectVersion(int projectId, int version, File installedDir) {
    this.projectId = projectId;
    this.version = version;
    this.installedDir = installedDir;
  }

  public int getProjectId() {
    return projectId;
  }

  public int getVersion() {
    return version;
  }

  public synchronized void setupProjectFiles(ProjectLoader projectLoader, File projectDir, Logger logger) throws ProjectManagerException,
          IOException {
    String projectVersion = String.valueOf(projectId) + "." + String.valueOf(version);
    if (installedDir == null) {
      installedDir = new File(projectDir, projectVersion);
    }

    if (!installedDir.exists()) {
      logger.info("First time executing new project. Setting up in directory " + installedDir.getPath());
      File tempDir = new File(projectDir, "_temp." + projectVersion + "." + System.currentTimeMillis());
      tempDir.mkdirs();
      ProjectFileHandler projectFileHandler = null;
      try {
        projectFileHandler = projectLoader.getUploadedFile(projectId, version);
        if ("zip".equals(projectFileHandler.getFileType())) {
          logger.info("Downloading zip file.");
          ZipFile zip = new ZipFile(projectFileHandler.getLocalFile());
          Utils.unzip(zip, tempDir);
          tempDir.renameTo(installedDir);
          processInitParameter();
        } else {
          throw new IOException("The file type hasn't been decided yet.");
        }
      } finally {
        if (projectFileHandler != null) {
          projectFileHandler.deleteLocalFile();
        }
      }
    } else {
      processInitParameter();
    }
  }

  private void processInitParameter() throws IOException {
    /**
     * 修改多job使用相同参数的问题
     * -----start-----
     */
    int folderSize = installedDir.list().length;
    if (folderSize > 0) {
      String folderName = "";
      for (String name : installedDir.list()) {
        //使用mac进行文件打包时会产生__MACOSX文件夹,需要排除掉
        if ("__MACOSX".equalsIgnoreCase(name) || name.startsWith("_")) {
          continue;
        } else {
          folderName = name;
          break;
        }
      }
      if (folderName.length() > 0) {
        File initParameterFile = new File(installedDir.getAbsolutePath() + File.separator + folderName
                + File.separator + CommonJobProperties.INIT_PARAMETER_SHELL);
        String initPropertiesPath = installedDir.getAbsolutePath() + File.separator + folderName
                + File.separator + CommonJobProperties.INIT_PARAMETER_PROPERTIES;
        if (initParameterFile.exists()) {
          Process pcs = Runtime.getRuntime().exec("sh " + initParameterFile.getAbsolutePath());
          String lineStr;
          BufferedInputStream in = new BufferedInputStream(pcs.getInputStream());
          BufferedReader br = new BufferedReader(new InputStreamReader(in));
          File propertiesFile = new File(initPropertiesPath);
          if (propertiesFile.exists()) {
            propertiesFile.delete();
          }
          while ((lineStr = br.readLine()) != null) {
            FileWriter writer = new FileWriter(initPropertiesPath, true);
            writer.write(lineStr + "\r\n");
            writer.close();
          }
          br.close();
          in.close();
        }
      }
    }
    /**
     * 修改多job使用相同参数的问题
     * -----end-----
     */
  }

  public synchronized void copyCreateSymlinkDirectory(File executionDir)
      throws IOException {
    if (installedDir == null || !installedDir.exists()) {
      throw new IOException("Installed dir doesn't exist: "
          + ((installedDir == null) ? null : installedDir.getAbsolutePath()));
    } else if (executionDir == null || !executionDir.exists()) {
      throw new IOException("Execution dir doesn't exist: "
          + ((executionDir == null) ? null : executionDir.getAbsolutePath()));
    }

    FileIOUtils.createDeepSymlink(installedDir, executionDir);
  }

  public synchronized void deleteDirectory() throws IOException {
    System.out.println("Deleting old unused project versin " + installedDir);
    if (installedDir != null && installedDir.exists()) {
      FileUtils.deleteDirectory(installedDir);
    }
  }

  @Override
  public int compareTo(ProjectVersion o) {
    if (projectId == o.projectId) {
      return version - o.version;
    }

    return projectId - o.projectId;
  }
}

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

package azkaban.webapp.servlet.hdfsviewer;

import azkaban.webapp.servlet.hdfsviewer.Capability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.Set;

public abstract class HdfsFileViewer {

	public Set<Capability> getCapabilities(FileSystem fs, Path path) {
		return EnumSet.noneOf(Capability.class);
	}

	public abstract void displayFile(
			FileSystem fs,
			Path path,
			OutputStream outStream,
			int startLine,
			int endLine) throws IOException;

	public String getSchema(FileSystem fs, Path path) {
		return null;
	}
}

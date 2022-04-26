/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.gdb.java.loader;

/**
 * A {@link LoaderNewTask} describe a new oss data load task to GDB Server
 */

public class LoaderNewTask {
	private String source;
	private String arnRole;
	private String format;
	private String mode;
	private boolean failOnError;

	public LoaderNewTask() {
		mode = "NEW";
		format = "csv";
		failOnError = false;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getArnRole() {
		return arnRole;
	}

	public void setArnRole(String arnRole) {
		this.arnRole = arnRole;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public boolean isFailOnError() {
		return failOnError;
	}

	public void setFailOnError(boolean failOnError) {
		this.failOnError = failOnError;
	}

	public void validation() throws LoaderExecption {
		final String csvFormat = "csv";
		if (!csvFormat.equals(format)) {
			throw new LoaderExecption("task format do not support");
		}

		/**
		 * source format check
		 */
		final String ossProtocol = "oss://";
		final String httpProtocol = "http://";
		if (source == null) {
			throw new LoaderExecption("task source is empty");
		}

		if (source.startsWith(ossProtocol)) {
			if (source.indexOf('/', ossProtocol.length()) == -1) {
				throw new LoaderExecption("source split error,"
					+ " should include bucket and object id");
			}
		} else if (source.startsWith(httpProtocol)) {
			int bpos = -1;
			int epos = source.indexOf('/', httpProtocol.length());
			if (epos != -1) {
				bpos = source.indexOf('/', epos + 1);
			}
			if (epos == -1 || bpos == -1) {
				throw new LoaderExecption("source split error,"
					+ " should include endpoint, bucket and object id");
			}
		}

		/**
		 * arn role format check
		 */
		final String arnStart = "acs::ram::";
		if (arnRole != null) {
			if (!arnRole.startsWith(arnStart)
				|| arnRole.indexOf(':', arnStart.length()) == -1) {
				throw new LoaderExecption("arn role split error,"
					+ " should include userId and role name");
			}
		}
	}
}

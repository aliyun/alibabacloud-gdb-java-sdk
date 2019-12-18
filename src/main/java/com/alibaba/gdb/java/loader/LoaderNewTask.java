/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
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

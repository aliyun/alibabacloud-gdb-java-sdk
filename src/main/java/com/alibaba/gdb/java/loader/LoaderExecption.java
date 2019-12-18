/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.gdb.java.loader;

import org.apache.http.HttpStatus;

import javax.ws.rs.core.Response;

public class LoaderExecption extends Exception {
	private int status;

	public LoaderExecption(String msg) {
		super(msg);
		status = HttpStatus.SC_OK;
	}

	public LoaderExecption(int status, String msg) {
		super(msg);
		this.status = status;
	}

	@Override
	public String toString() {
		return Response.Status.fromStatusCode(status).toString() + " : " + getMessage();
	}
}

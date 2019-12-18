/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.gdb.java.loader;

import java.util.List;
import java.util.stream.Collectors;

public class LoaderList {
	private List<String> loadIds;

	public List<String> getLoadIds() {
		return loadIds;
	}

	public void setLoadIds(List<String> loadIds) {
		this.loadIds = loadIds;
	}

	@Override
	public String toString() {
		return loadIds.stream().collect(Collectors.joining(","));
	}
}

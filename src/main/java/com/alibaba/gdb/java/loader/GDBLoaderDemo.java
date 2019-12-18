/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.gdb.java.loader;

import java.io.File;

public class GDBLoaderDemo {
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("gdb driver config needed");
			System.exit(1);
		}

		String configFile = args[0];
		GDBOSSLoaderDriver driver = new GDBOSSLoaderDriver.Builder(new File(configFile)).create();

		LoaderList list = driver.getLoaderList();
		System.out.println(list.toString());

		String ossSourceNode = "http://oss-cn-shanghai.aliyuncs.com/"
			+ "graphoss-cnshanghai/test2/graph500-scale19-ef16_adj-nodes.csv";
		String ossArnRole = "acs:ram::1928439924892519:role/aliyungdbaccessingossrole";

		/**
		 * add new task
		 */
		LoaderNewTask task = new LoaderNewTask();
		task.setSource(ossSourceNode);
		task.setArnRole(ossArnRole);

		LoaderTaskId taskId = driver.addLoaderTask(task);
		Thread.sleep(1000);

		/**
		 * get task detail
		 */
		LoaderDetail loaderDetail = driver.getLoaderDetail(taskId.getLoadId());
		System.out.println(loaderDetail.toString());

		/**
		 * delete task
		 */
		driver.deleteLoaderTask(taskId.getLoadId());
	}
}

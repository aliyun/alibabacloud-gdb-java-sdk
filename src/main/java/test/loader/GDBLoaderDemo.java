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
package test.loader;

import com.alibaba.gdb.java.loader.GDBOSSLoaderDriver;
import com.alibaba.gdb.java.loader.LoaderDetail;
import com.alibaba.gdb.java.loader.LoaderList;
import com.alibaba.gdb.java.loader.LoaderNewTask;
import com.alibaba.gdb.java.loader.LoaderTaskId;

import java.io.File;

public class GDBLoaderDemo {
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Format: gdb-driver-config ossSourceNode ossArnRole");
			System.err.println("ossSourceNode format: http://oss-cn-shanghai.aliyuncs.com/graphoss-cnshanghai/" +
					"test2/graph500-test-ef16_adj-nodes.csv");
			System.err.println("ossArnRole format: acs::ram::id:role/aliyungdbaccessingossrole");
			System.exit(1);
		}

		String configFile = args[0];
		String ossSourceNode = args[1];
		String ossArnRole = args[2];
		GDBOSSLoaderDriver driver = new GDBOSSLoaderDriver.Builder(new File(configFile)).create();

		LoaderList list = driver.getLoaderList();
		System.out.println(list.toString());


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

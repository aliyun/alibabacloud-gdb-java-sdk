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


import org.apache.tinkerpop.gremlin.driver.GdbSettings;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import static org.apache.http.HttpStatus.SC_OK;

public class GDBOSSLoaderDriver {
	private final String LOADER_ID = "loaderId";
	private static final Logger logger = LoggerFactory.getLogger(GDBOSSLoaderDriver.class);

	private WebTarget loaderClient;

	private GDBOSSLoaderDriver(String host, int port, String username, String password) {
		final String loaderPath = "loader";
		ClientConfig config = new ClientConfig();
		HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(username, password);

		config.register(feature);
		Client client = ClientBuilder.newClient(config);
		loaderClient = client.target("http://" + host + ":" + port).path(loaderPath);

		logger.info("init test.loader driver with {}:{}", host, port);
	}

	/**
	 * get test.loader task list {@link LoaderList}
	 *
	 * @return the loader list containing all loader tasks
	 */
	public LoaderList getLoaderList() {
		logger.debug("request load task list");
		Response response = loaderClient.request(MediaType.APPLICATION_JSON_TYPE).get();
		return response.readEntity(new GenericType<LoaderResponse<LoaderList>>() { }).getPayload();
	}

	/**
	 * get test.loader task detail, include records, errors, time and so on {@link LoaderDetail}
	 * @param loaderId the task id in GDB server
	 * @return the loader detail for the specified task
	 * @throws LoaderExecption when request get errors
	 */
	public LoaderDetail getLoaderDetail(String loaderId) throws LoaderExecption {
		logger.debug("request load task detail : {}", loaderId);
		Response response = loaderClient.queryParam(LOADER_ID, loaderId)
			.request(MediaType.APPLICATION_JSON_TYPE).get();
		if (response.getStatus() != SC_OK) {
			String msg = response.readEntity(String.class);
			logger.error("request detail failed : {}", msg);
			throw new LoaderExecption(response.getStatus(), msg);
		}
		return response.readEntity(new GenericType<LoaderResponse<LoaderDetail>>(){ }).getPayload();
	}

	/**
	 * add a new task {@link LoaderNewTask} to GDB for import graph csv data in OSS
	 * @param task the task to load in GDB
	 * @return the loader task ID of the newly created task
	 * @throws LoaderExecption when fail to add task
	 */
	public LoaderTaskId addLoaderTask(final LoaderNewTask task) throws LoaderExecption {
		task.validation();

		logger.debug("request to add load task");
		Response response = loaderClient.request(MediaType.APPLICATION_JSON_TYPE)
			 .post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));
		if (response.getStatus() != SC_OK) {
			String msg = response.readEntity(String.class);
			logger.error("request to add task failed : {}", msg);
			throw new LoaderExecption(response.getStatus(), msg);
		}
		return response.readEntity(new GenericType<LoaderResponse<LoaderTaskId>>(){ }).getPayload();
	}

	/**
	 * delete a task in GDB server, it's blocked to stop running task
	 * @param loaderId task id in server
	 * @throws LoaderExecption when task is not exist
	 */
	public void deleteLoaderTask(final String loaderId) throws LoaderExecption {
		logger.debug("request to delete task : {}", loaderId);
		Response response = loaderClient.queryParam(LOADER_ID, loaderId).request().delete();
		if (response.getStatus() != SC_OK) {
			String msg = response.readEntity(String.class);
			logger.error("request to delete task failed : {}", msg);
			throw new LoaderExecption(response.getStatus(), msg);
		}
	}

	public final static class Builder {
		private String host;
		private int port;
		private String username;
		private String password;

		public Builder() {
			// empty to prevent direct instantiation
		}

		public Builder(final File configurationFile) throws FileNotFoundException {
			final GdbSettings settings = GdbSettings.read(new FileInputStream(configurationFile));

			port(settings.port);
			host(settings.hosts.get(0));
			auth(settings.username, settings.password);
		}

		public GDBOSSLoaderDriver create() {
			return new GDBOSSLoaderDriver(host, port, username, password);
		}

		public Builder port(final int port) {
			this.port = port;
			return this;
		}

		public Builder host(final String host) {
			this.host = host;
			return this;
		}

		public Builder auth(final String username, final String password) {
			this.username = username;
			this.password = password;
			return this;
		}

	}
}

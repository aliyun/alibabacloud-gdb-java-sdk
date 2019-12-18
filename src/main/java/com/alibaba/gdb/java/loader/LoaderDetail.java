/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.gdb.java.loader;

import java.util.List;

/**
 * A {@link LoaderDetail} is a detail description of load task in GDB Server, included errors during process.
 * It's better to save {@link errorItem} in your own storage as Server do not remain errors after read.
 */

public class LoaderDetail {
	private String loaderId;
	private String fullUri;
	private String status;

	private long datatypeMismatchErrors;
	private long insertErrors;
	private long parsingErrors;
	private long retryNumber;
	private long runNumber;
	private long totalDuplicates;
	private long totalRecords;
	private long totalTimeSpent;
	private long processTaskSize;
	private long totalTaskSize;

	private List<errorItem> errors;

	public String getLoaderId() {
		return loaderId;
	}

	public void setLoaderId(String loaderId) {
		this.loaderId = loaderId;
	}

	public String getFullUri() {
		return fullUri;
	}

	public void setFullUri(String fullUri) {
		this.fullUri = fullUri;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public long getDatatypeMismatchErrors() {
		return datatypeMismatchErrors;
	}

	public void setDatatypeMismatchErrors(long datatypeMismatchErrors) {
		this.datatypeMismatchErrors = datatypeMismatchErrors;
	}

	public long getInsertErrors() {
		return insertErrors;
	}

	public void setInsertErrors(long insertErrors) {
		this.insertErrors = insertErrors;
	}

	public long getParsingErrors() {
		return parsingErrors;
	}

	public void setParsingErrors(long parsingErrors) {
		this.parsingErrors = parsingErrors;
	}

	public long getRetryNumber() {
		return retryNumber;
	}

	public void setRetryNumber(long retryNumber) {
		this.retryNumber = retryNumber;
	}

	public long getRunNumber() {
		return runNumber;
	}

	public void setRunNumber(long runNumber) {
		this.runNumber = runNumber;
	}

	public long getTotalDuplicates() {
		return totalDuplicates;
	}

	public void setTotalDuplicates(long totalDuplicates) {
		this.totalDuplicates = totalDuplicates;
	}

	public long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}

	public long getTotalTimeSpent() {
		return totalTimeSpent;
	}

	public void setTotalTimeSpent(long totalTimeSpent) {
		this.totalTimeSpent = totalTimeSpent;
	}

	public long getProcessTaskSize() {
		return processTaskSize;
	}

	public void setProcessTaskSize(long processTaskSize) {
		this.processTaskSize = processTaskSize;
	}

	public long getTotalTaskSize() {
		return totalTaskSize;
	}

	public void setTotalTaskSize(long totalTaskSize) {
		this.totalTaskSize = totalTaskSize;
	}

	public List<errorItem> getErrors() {
		return errors;
	}

	public void setErrors(List<errorItem> errors) {
		this.errors = errors;
	}

	class errorItem {
		private String errorCode;
		private String errorMessage;
		private String objectId;

		public String getErrorCode() {
			return errorCode;
		}

		public void setErrorCode(String errorCode) {
			this.errorCode = errorCode;
		}

		public String getErrorMessage() {
			return errorMessage;
		}

		public void setErrorMessage(String errorMessage) {
			this.errorMessage = errorMessage;
		}

		public String getObjectId() {
			return objectId;
		}

		public void setObjectId(String objectId) {
			this.objectId = objectId;
		}

		@Override
		public String toString() {
			return "code: " + errorCode + ", message: " + errorMessage + ", loaderId: " + objectId;
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append("loaderId: ").append(loaderId).append('\n');
		builder.append("uri: ").append(fullUri).append('\n');
		builder.append("status: ").append(status).append('\n');
		builder.append("insertErrors: ").append(insertErrors).append('\n');
		builder.append("parsingErrors: ").append(parsingErrors).append('\n');
		builder.append("datatypeMismatchErrors: ").append(datatypeMismatchErrors).append('\n');
		builder.append("totalDuplicates: ").append(totalDuplicates).append('\n');
		builder.append("retryNumber: ").append(retryNumber).append('\n');
		builder.append("runNumber: ").append(runNumber).append('\n');
		builder.append("totalTimeSpent: ").append(totalTimeSpent).append('\n');
		builder.append("processTaskSize: ").append(processTaskSize).append('\n');
		builder.append("totalTaskSize: ").append(totalTaskSize).append('\n');
		builder.append("totalRecords: ").append(totalRecords).append('\n');

		errors.forEach((n) -> builder.append(n.toString()).append('\n'));

		return builder.toString();
	}
}

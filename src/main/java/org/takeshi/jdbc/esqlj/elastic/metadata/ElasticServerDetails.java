package org.takeshi.jdbc.esqlj.elastic.metadata;

import org.elasticsearch.client.core.MainResponse;

public class ElasticServerDetails {
	private String clusterName;
	private String clusterUuid;
	private String nodeName;
	private String buildDate;
	private String buildFlavor;
	private String buildHash;
	private String buildType;
	private String luceneVersion;
	private String minimumIndexCompatibilityVersion;
	private String minimumWireCompatibilityVersion;
	private String number;

	public ElasticServerDetails(MainResponse response) {
		this.clusterName = response.getClusterName();
		this.clusterUuid = response.getClusterUuid();
		this.nodeName = response.getNodeName();
		
		MainResponse.Version version = response.getVersion();
		this.buildDate = version.getBuildDate();
		this.buildFlavor = version.getBuildFlavor();
		this.buildHash = version.getBuildHash();
		this.buildType = version.getBuildType();
		this.luceneVersion = version.getLuceneVersion();
		this.minimumIndexCompatibilityVersion= version.getMinimumIndexCompatibilityVersion();
		this.minimumWireCompatibilityVersion = version.getMinimumWireCompatibilityVersion();
		this.number = version.getNumber();
	}
	
	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getClusterUuid() {
		return clusterUuid;
	}

	public void setClusterUuid(String clusterUuid) {
		this.clusterUuid = clusterUuid;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public String getBuildDate() {
		return buildDate;
	}

	public void setBuildDate(String buildDate) {
		this.buildDate = buildDate;
	}

	public String getBuildFlavor() {
		return buildFlavor;
	}

	public void setBuildFlavor(String buildFlavor) {
		this.buildFlavor = buildFlavor;
	}

	public String getBuildHash() {
		return buildHash;
	}

	public void setBuildHash(String buildHash) {
		this.buildHash = buildHash;
	}

	public String getBuildType() {
		return buildType;
	}

	public void setBuildType(String buildType) {
		this.buildType = buildType;
	}

	public String getLuceneVersion() {
		return luceneVersion;
	}

	public void setLuceneVersion(String luceneVersion) {
		this.luceneVersion = luceneVersion;
	}

	public String getMinimumIndexCompatibilityVersion() {
		return minimumIndexCompatibilityVersion;
	}

	public void setMinimumIndexCompatibilityVersion(String minimumIndexCompatibilityVersion) {
		this.minimumIndexCompatibilityVersion = minimumIndexCompatibilityVersion;
	}

	public String getMinimumWireCompatibilityVersion() {
		return minimumWireCompatibilityVersion;
	}

	public void setMinimumWireCompatibilityVersion(String minimumWireCompatibilityVersion) {
		this.minimumWireCompatibilityVersion = minimumWireCompatibilityVersion;
	}

	public String getNumber() {
		return number;
	}

	public void setNumber(String number) {
		this.number = number;
	}

}

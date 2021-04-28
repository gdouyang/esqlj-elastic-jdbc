package org.fpasti.jdbc.esqlj.elastic.metadata;

import co.elastic.clients.elasticsearch._types.ElasticsearchVersionInfo;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import co.elastic.clients.util.DateTime;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ElasticServerDetails {
	public static long ELASTIC_REL_7_10_0 = 7010000000000L;
	
	private String clusterName;
	private String clusterUuid;
	private String nodeName;
	private DateTime buildDate;
	private String buildFlavor;
	private String buildHash;
	private String buildType;
	private String luceneVersion;
	private String minimumIndexCompatibilityVersion;
	private String minimumWireCompatibilityVersion;
	private String number;
	private Long releaseNumber;

	public ElasticServerDetails(InfoResponse response) {
		this.clusterName = response.clusterName();
		this.clusterUuid = response.clusterUuid();
		this.nodeName = response.name();
		
		ElasticsearchVersionInfo version = response.version();
		this.buildDate = version.buildDate();
		this.buildFlavor = version.buildFlavor();
		this.buildHash = version.buildHash();
		this.buildType = version.buildType();
		this.luceneVersion = version.luceneVersion();
		this.minimumIndexCompatibilityVersion= version.minimumIndexCompatibilityVersion();
		this.minimumWireCompatibilityVersion = version.minimumWireCompatibilityVersion();
		this.number = version.number();
		convertReleaseNumberToInteger();
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
		return buildDate.getString();
	}

	public void setBuildDate(String buildDate) {
		this.buildDate = DateTime.of(buildDate);
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

	public boolean isElasticReleaseEqOrGt(Long elasticReleaseNumber) {
		return releaseNumber >= elasticReleaseNumber;
	}
	
	private void convertReleaseNumberToInteger() {
		String[] splittedNumber = number.split("\\.");
		releaseNumber = 0L;
		for(int i = 0; i < 4; i++) {
			if(splittedNumber.length > i) {
				releaseNumber += Long.valueOf(splittedNumber[i]) * (long)Math.pow(10,  (4 - i) * 3);
			}
		}
	}
}

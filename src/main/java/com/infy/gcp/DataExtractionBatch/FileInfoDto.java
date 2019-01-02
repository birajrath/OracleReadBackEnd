package com.infy.gcp.DataExtractionBatch;

import java.util.ArrayList;



public class FileInfoDto {
	
	
	int feed_id;
	String dataPath;
	ArrayList<FileMetadataDto> fileMetadataArr;
	String project;
	String juniper_user;
	
	
	
	
	
	public String getProject() {
		return project;
	}
	public void setProject(String project) {
		this.project = project;
	}
	
	public String getJuniper_user() {
		return juniper_user;
	}
	public void setJuniper_user(String juniper_user) {
		this.juniper_user = juniper_user;
	}
	public String getDataPath() {
		return dataPath;
	}
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}
	
	public int getFeed_id() {
		return feed_id;
	}
	public void setFeed_id(int feed_id) {
		this.feed_id = feed_id;
	}
	public ArrayList<FileMetadataDto> getFileMetadataArr() {
		return fileMetadataArr;
	}
	public void setFileMetadataArr(ArrayList<FileMetadataDto> fileMetadataArr) {
		this.fileMetadataArr = fileMetadataArr;
	}
	
	

}

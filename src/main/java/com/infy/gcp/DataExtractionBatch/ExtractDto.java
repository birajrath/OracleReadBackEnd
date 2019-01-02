package com.infy.gcp.DataExtractionBatch;

import java.util.ArrayList;


public class ExtractDto {
	
	ConnectionDto connDto;
	FeedDto feedDto;
	TableInfoDto tableInfoDto;
	FileInfoDto fileInfoDto;
	//HDFSMetadataDto hdfsInfoDto;
	ArrayList<TargetDto> targetArr;
	
	
	public FileInfoDto getFileInfoDto() {
		return fileInfoDto;
	}
	public void setFileInfoDto(FileInfoDto fileInfoDto) {
		this.fileInfoDto = fileInfoDto;
	}
	public ArrayList<TargetDto> getTargetArr() {
		return targetArr;
	}
	public void setTargetArr(ArrayList<TargetDto> targetArr) {
		this.targetArr = targetArr;
	}
	public ConnectionDto getConnDto() {
		return connDto;
	}
	public void setConnDto(ConnectionDto connDto) {
		this.connDto = connDto;
	}
	
	public FeedDto getFeedDto() {
		return feedDto;
	}
	public void setFeedDto(FeedDto feedDto) {
		this.feedDto = feedDto;
	}
	public TableInfoDto getTableInfoDto() {
		return tableInfoDto;
	}
	public void setTableInfoDto(TableInfoDto tableInfoDto) {
		this.tableInfoDto = tableInfoDto;
	}
	/*public HDFSMetadataDto getHdfsInfoDto() {
		return hdfsInfoDto;
	}
	public void setHdfsInfoDto(HDFSMetadataDto hdfsInfoDto) {
		this.hdfsInfoDto = hdfsInfoDto;
	}*/

}

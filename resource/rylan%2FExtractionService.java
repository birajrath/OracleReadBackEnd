package com.iig.gcp.extraction.service;

import java.util.ArrayList;

import com.iig.gcp.extraction.dto.SourceSystemDetailBean;
import com.iig.gcp.extraction.dto.DataDetailBean;
import com.iig.gcp.extraction.dto.DriveMaster;
import com.iig.gcp.extraction.dto.ConnectionMaster;
import com.iig.gcp.extraction.dto.CountryMaster;
import com.iig.gcp.extraction.dto.ReservoirMaster;
import com.iig.gcp.extraction.dto.SourceSystemMaster;

public interface ExtractionService {

	public String invokeRest(String json,String url) throws UnsupportedOperationException, Exception ;
	public ArrayList<ConnectionMaster> getConnections(String src_val);
	public ArrayList<String> getTargets();
	public ArrayList<String> getTargets1(String tgt);
	public ConnectionMaster getConnections1(String src_val,int src_sys_id);
	public ConnectionMaster getConnections2(String src_val,int conn_id);
	public String getExtType(int src_sys_id);
	public String getExtType1(String src_unique_name);
	public ArrayList<SourceSystemMaster> getSources(String src_val);
	public ArrayList<SourceSystemDetailBean> getSources1(String src_val,int src_sys_id);
	public ArrayList<String> getTables(String src_val,int conn_id, String schema_name);
	public ArrayList<String> getFields(String id,String src_val,String table_name,int conn_id, String schema_name);
	public ArrayList<CountryMaster> getCountries();
	public ArrayList<ReservoirMaster> getReservoirs();
	public ArrayList<DataDetailBean> getData(int src_sys_id,String src_val, String schema_name);
	public int checkNames(String sun);
	public ArrayList<String> getSchema(String src_val,int conn_id);
	public ArrayList<String> getSystem(String project);
	public ArrayList<DriveMaster> getDrives();
	public ArrayList<DriveMaster> getDrives1(int src_sys_id);
	public String getSchemaData(String src_val,int src_sys_id);
}

package com.infy.gcp.DataExtractionBatch;

import java.sql.Connection;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;



public class DBUtils {
	
	
	
	
	public static ConnectionDto getConnectionObject(String feed_name) throws SQLException {
		
		Connection conn= ConnectionUtil.connectToOracle(MetadataDbConstants.ORACLE_IP_PORT_SID, MetadataDbConstants.ORACLE_USER_NAME, MetadataDbConstants.ORACLE_PASSWORD);
		int connId=getConnectionId(conn,feed_name);
		ConnectionDto connDto=new ConnectionDto();
		String query="select src_conn_type,host_name,port_no,username,password,encrypted_encr_key,database_name,service_name,drive_sequence from "+MetadataDbConstants.CONNECTIONTABLE+ " where src_conn_sequence="+connId;
			try {
				Statement statement=conn.createStatement();
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					connDto.setConn_type(rs.getString(1));
					connDto.setHostName(rs.getString(2));
					connDto.setPort(rs.getString(3));
					connDto.setUserName(rs.getString(4));
					connDto.setEncrypted_password(rs.getBytes(5));
					connDto.setEncr_key(rs.getBytes(6));
					
					connDto.setDbName(rs.getString(7));
					connDto.setServiceName(rs.getString(8));
					String drive_id=rs.getString(9);
					if(!(drive_id==null)) {
						
						connDto.setDrive_id(Integer.parseInt(drive_id));
					}
					
				}

			}catch(SQLException e){
				e.printStackTrace();
				return null;

			}finally {
				conn.close();
			}
			return connDto;
		
		
	}

	public static int getConnectionId (Connection conn,String feed_name) throws SQLException {

		//Connection conn= ConnectionUtil.connectToOracle(MetadataDbConstants.ORACLE_IP_PORT_SID, MetadataDbConstants.ORACLE_USER_NAME, MetadataDbConstants.ORACLE_PASSWORD);
		int connectionId=0;
		String query="select src_conn_sequence from "+MetadataDbConstants.FEEDTABLE+" where feed_unique_name='"+feed_name+"'";
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			connectionId=rs.getInt(1);

		}
		return connectionId;

	}
	
	public static FeedDto getFeedObject(String feed_name) throws SQLException{
		
		Connection conn= ConnectionUtil.connectToOracle(MetadataDbConstants.ORACLE_IP_PORT_SID, MetadataDbConstants.ORACLE_USER_NAME, MetadataDbConstants.ORACLE_PASSWORD);
		FeedDto feedDto=new FeedDto();

		String query=" select feed_sequence,feed_unique_name,country_code,target,table_list,file_list,file_path,project_sequence from "+MetadataDbConstants.FEEDTABLE
				+ " where feed_unique_name='"+feed_name+"'";
		
		try {
			Statement statement=conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				rs.next();
				feedDto.setFeed_id(Integer.parseInt(rs.getString(1)));
				feedDto.setFeed_name(rs.getString(2));
				feedDto.setCountry_code(rs.getString(3));
				
				feedDto.setTarget(rs.getString(4));
				feedDto.setTableList(rs.getString(5));
				feedDto.setFileList(rs.getString(6));
				feedDto.setFilePath(rs.getString(7));
				String projectSequence=rs.getString(8);
				if(!(projectSequence==null)) {
					feedDto.setProject_sequence(Integer.parseInt(projectSequence));
				}
				

			}



		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}

		return feedDto;

	}
	
	
	public static ArrayList<TargetDto> getTargetObject(String targetList) throws SQLException{

		Connection conn= ConnectionUtil.connectToOracle(MetadataDbConstants.ORACLE_IP_PORT_SID, MetadataDbConstants.ORACLE_USER_NAME, MetadataDbConstants.ORACLE_PASSWORD);
		ArrayList<TargetDto> targetArr=new ArrayList<TargetDto>();
		Statement statement=conn.createStatement();
		String query="";
		ResultSet rs;
		String[] targets=targetList.split(",");;
		try {
			for(String target:targets) {
				TargetDto targetDto=new TargetDto();
				query=" select target_unique_name,target_type,gcp_sequence,hdp_knox_url,hdp_user,hdp_encrypted_password,encrypted_key,hdp_hdfs_path,drive_sequence,unix_data_path from "+MetadataDbConstants.TAREGTTABLE
						+ " where target_unique_name='"+target+"'";
				rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					if(rs.getString(2).equalsIgnoreCase("gcs")) {
						targetDto.setTarget_unique_name(rs.getString(1));
						targetDto.setTarget_type(rs.getString(2));
						int gcp_seq=Integer.parseInt(rs.getString(3));
						String query2="select gcp_project,bucket_name,service_account_name from "+MetadataDbConstants.GCPTABLE+" where gcp_sequence="+gcp_seq;
						Statement statement2=conn.createStatement();
						ResultSet rs2=statement2.executeQuery(query2);
						if(rs2.isBeforeFirst()) {
							rs2.next();
							targetDto.setTarget_project(rs2.getString(1));
							targetDto.setTarget_bucket(rs2.getString(2));
							targetDto.setService_account(rs2.getString(3));
						}
						
					}
					if(rs.getString(2).equalsIgnoreCase("hdfs")) {
						targetDto.setTarget_unique_name(rs.getString(1));
						targetDto.setTarget_type(rs.getString(2));
						targetDto.setTarget_knox_url(rs.getString(4));
						targetDto.setTarget_user(rs.getString(5));
						targetDto.setEncrypted_password(rs.getBytes(6));
						targetDto.setEncrypted_key(rs.getBytes(7));
						targetDto.setTarget_hdfs_path(rs.getString(8));
					}
					

				}
				targetArr.add(targetDto);

			}
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}


		return targetArr;
	}
	
	
	public static TableInfoDto getTableInfoObject(String table_list) throws SQLException{
		
		Connection conn= ConnectionUtil.connectToOracle(MetadataDbConstants.ORACLE_IP_PORT_SID, MetadataDbConstants.ORACLE_USER_NAME, MetadataDbConstants.ORACLE_PASSWORD);
		TableInfoDto tableInfoDto = new TableInfoDto();
		ArrayList<TableMetadataDto> tableMetadataArr=new ArrayList<TableMetadataDto>();
		String[] tableIds=table_list.split(",");
		try {
			for(String tableId:tableIds) {
				String query="select table_name,columns,where_clause,fetch_type,incr_col from "+MetadataDbConstants.TABLEDETAILSTABLE+" where table_sequence="+tableId;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					TableMetadataDto tableMetadata=new TableMetadataDto();
					tableMetadata.setTable_name(rs.getString(1));
					tableMetadata.setColumns(rs.getString(2));
					tableMetadata.setWhere_clause( rs.getString(3));
					tableMetadata.setFetch_type(rs.getString(4));
					tableMetadata.setIncr_col(rs.getString(5));
					tableMetadataArr.add(tableMetadata);
					
				}


			}
		for(TableMetadataDto table:tableMetadataArr) {
			if(table.getFetch_type().equalsIgnoreCase("incr")) {
				tableInfoDto.setIncr_flag("Y");
				break;
			}
		}
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}

		tableInfoDto.setTableMetadataArr(tableMetadataArr);
		return tableInfoDto;
	}
	
	
	public static FileInfoDto getFileInfoObject( String fileList) throws SQLException{
		
		Connection conn= ConnectionUtil.connectToOracle(MetadataDbConstants.ORACLE_IP_PORT_SID, MetadataDbConstants.ORACLE_USER_NAME, MetadataDbConstants.ORACLE_PASSWORD);
		FileInfoDto fileInfoDto= new FileInfoDto();
		ArrayList<FileMetadataDto> fileMetadataArr=new ArrayList<FileMetadataDto>();
		
		String[] fileIds=fileList.split(",");
		try {
			for(String fileId:fileIds) {
				StringBuffer fieldList=new StringBuffer();
				FileMetadataDto fileMetadataDto= new FileMetadataDto();
				String query="select file_name,file_type,file_delimiter,header_count,trailer_count,avro_conv_flg,bus_dt_format,bus_dt_loc,bus_dt_start,count_loc,count_start,count_length from "+MetadataDbConstants.FILEDETAILSTABLE+" where file_sequence="+fileId;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					fileMetadataDto.setFile_sequence(Integer.parseInt(fileId));
					fileMetadataDto.setFile_name(rs.getString(1));
					fileMetadataDto.setFile_type(rs.getString(2));
					fileMetadataDto.setFile_delimiter(rs.getString(3));
					fileMetadataDto.setHeader_count(rs.getString(4));
					fileMetadataDto.setTrailer_count(rs.getString(5));
					fileMetadataDto.setAvro_conv_flag(rs.getString(6));
					fileMetadataDto.setBus_dt_format(rs.getString(7));
					fileMetadataDto.setBus_dt_loc(rs.getString(8));
					fileMetadataDto.setBus_dt_start(Integer.parseInt(rs.getString(9)));
					fileMetadataDto.setCount_loc(rs.getString(10));
					fileMetadataDto.setCount_start(Integer.parseInt(rs.getString(11)));
					fileMetadataDto.setCount_legnth(Integer.parseInt(rs.getString(12)));
					
				}
				
				String query2="select fd.field_name from "+MetadataDbConstants.FIELDDETAILSTABLE
						+" fd inner join "+MetadataDbConstants.FILEDETAILSTABLE
						+" fi on fi.file_name=fd.file_name where fi.file_sequence="+fileId;
				ResultSet rs2 = statement.executeQuery(query2);
				if(rs2.isBeforeFirst()) {
					while(rs2.next()) {
						fieldList.append(rs2.getString(1)+",");
					}
					fieldList.setLength(fieldList.length()-1);
					fileMetadataDto.setField_list(fieldList.toString());
					
				
				}
				
			fileMetadataArr.add(fileMetadataDto);
				
			}
		}catch(SQLException e) {
			e.printStackTrace();
			throw e;
		}finally {
			conn.close();
		}
		
		fileInfoDto.setFileMetadataArr(fileMetadataArr);
		return fileInfoDto;
		
	}
	
	public static String getConnectionString(ConnectionDto connDto) {


		if(connDto.getConn_type().equalsIgnoreCase("MSSQL")){

			return "jdbc:sqlserver://"+ connDto.getHostName()+":" +connDto.getPort()+";DatabaseName="+connDto.getDbName() ;
		}
		if(connDto.getConn_type().equalsIgnoreCase("ORACLE"))
		{
			return "jdbc:oracle:thin:@"+ connDto.getHostName()+":" +connDto.getPort()+"/"+connDto.getServiceName() ;
		}
		if(connDto.getConn_type().equalsIgnoreCase("TERADATA"))
		{
			return "jdbc:oracle:thin:@"+ connDto.getHostName()+":" +connDto.getPort()+"/"+connDto.getServiceName() ;
		}

		return null;
	}

	
	
	
public static String updateNifiProcessgroupDetails( ExtractDto extractDto,String path,String date, String run_id,int index) throws SQLException{
		
	Connection conn= ConnectionUtil.connectToOracle(MetadataDbConstants.ORACLE_IP_PORT_SID, MetadataDbConstants.ORACLE_USER_NAME, MetadataDbConstants.ORACLE_PASSWORD);
		String insertQuery=MetadataDbConstants.INSERTQUERY.replace("{$table}", MetadataDbConstants.NIFISTATUSTABLE)
				.replace("{$columns}", "country_code,feed_id,feed_unique_name,run_id,nifi_pg,pg_type,extracted_date,project_sequence,status")
				.replace("{$data}",MetadataDbConstants.QUOTE+extractDto.getFeedDto().getCountry_code()+MetadataDbConstants.QUOTE+MetadataDbConstants.COMMA
						+extractDto.getFeedDto().getFeed_id()+MetadataDbConstants.COMMA
						+MetadataDbConstants.QUOTE+extractDto.getFeedDto().getFeed_name()+MetadataDbConstants.QUOTE+MetadataDbConstants.COMMA
						+MetadataDbConstants.QUOTE+run_id+MetadataDbConstants.QUOTE+MetadataDbConstants.COMMA
						+index+MetadataDbConstants.COMMA
						+MetadataDbConstants.QUOTE+extractDto.getConnDto().getConn_type()+MetadataDbConstants.QUOTE+MetadataDbConstants.COMMA
						+MetadataDbConstants.QUOTE+date+MetadataDbConstants.QUOTE+MetadataDbConstants.COMMA
						+extractDto.getFeedDto().getProject_sequence()+MetadataDbConstants.COMMA
						+MetadataDbConstants.QUOTE+"running"+MetadataDbConstants.QUOTE);
		
		System.out.println("insert query is "+insertQuery);
		try {	
			Statement statement = conn.createStatement();
			statement.execute(insertQuery);
			System.out.println("query executed");
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			return e.getMessage();
		}finally {
			conn.close();
		}
		return "success";
						
	}

	public static String checkProcessGroupStatus( int index, String conn_type) throws SQLException{
		

		Connection conn= ConnectionUtil.connectToOracle(MetadataDbConstants.ORACLE_IP_PORT_SID, MetadataDbConstants.ORACLE_USER_NAME, MetadataDbConstants.ORACLE_PASSWORD);
		Date date = Calendar.getInstance().getTime();
		DateFormat formatter = new SimpleDateFormat("ddMMyyyy");
        String today = formatter.format(date);
		String query= "select status from "+ MetadataDbConstants.NIFISTATUSTABLE +" where nifi_pg="+index+" and extracted_date='"+today+"' and pg_type='"+conn_type+"'";
		try {
			Statement statement=conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				while(rs.next()) {
					if(rs.getString(1).equalsIgnoreCase("RUNNING")) {
						return "Not Free";
					}
				}
		}else {
			return "Free";
		}
		}catch(SQLException e) {
			throw e;
		}finally {
			conn.close();
		}
		return "Free";
		
	}

	public static int getProcessGroup(String feed_name,String country_code) throws SQLException {
		Connection conn= ConnectionUtil.connectToOracle(MetadataDbConstants.ORACLE_IP_PORT_SID, MetadataDbConstants.ORACLE_USER_NAME, MetadataDbConstants.ORACLE_PASSWORD);
		String query= "select distinct nifi_pg from "+MetadataDbConstants.NIFISTATUSTABLE+" where feed_unique_name='"+feed_name+"' and country_code='"+country_code+"'";
		try {
			Statement statement=conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				while(rs.next()) {
					return rs.getInt(1);
				}
		}else {
			System.out.println("system running first time");
			return 0;
		}
		}catch(SQLException e) {
			throw e;
		}finally {
			conn.close();
		}
		return 0;
	}
	
	

}

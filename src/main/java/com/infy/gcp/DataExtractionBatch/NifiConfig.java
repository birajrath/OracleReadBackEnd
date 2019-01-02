package com.infy.gcp.DataExtractionBatch;

import java.io.BufferedWriter;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import com.infy.gcp.DataExtractionBatch.NifiConstants;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.infy.gcp.DataExtractionBatch.GenericConstants;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class NifiConfig {
	
	
	@SuppressWarnings("static-access")
	public static void main( String[] args ) throws Exception{
		
		
		
		String feed_name=args[0];
		ExtractDto extractDto = new ExtractDto();
		
		extractDto.setConnDto(DBUtils.getConnectionObject(feed_name));
		extractDto.setFeedDto(DBUtils.getFeedObject(feed_name));
		String targetList=extractDto.getFeedDto().getTarget();
		extractDto.setTargetArr(DBUtils.getTargetObject(targetList));
		if(!(extractDto.getFeedDto().getTableList()==null||extractDto.getFeedDto().getTableList().isEmpty())) {
			String tableList=extractDto.getFeedDto().getTableList();
			extractDto.setTableInfoDto(DBUtils.getTableInfoObject(tableList));
		}
		if(!(extractDto.getFeedDto().getFileList()==null||extractDto.getFeedDto().getFileList().isEmpty())) {
			if(extractDto.getConnDto().getConn_type().equalsIgnoreCase("UNIX")) {
				String fileList=extractDto.getFeedDto().getFileList();
				extractDto.setFileInfoDto(DBUtils.getFileInfoObject(fileList));
			}
						
		}
		
		String processGroupUrl="";
		String listenHttpUrl="";
		String controllerId="";
		String clientId="";
		String controllerVersion="";
		String controllerStatus="";
		int index=0;
		String processorInfo="";
		HttpEntity respEntity=null;
		String trigger_flag="N";
		int processGroupIndex=0;
		String processGroupStatus="";
		String path="";
		
		Date date = Calendar.getInstance().getTime();
		DateFormat formatter = new SimpleDateFormat("ddMMyyyy");
        String today = formatter.format(date);
        System.out.println("today is "+today);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Long runId=timestamp.getTime();
        String connectionString=DBUtils.getConnectionString(extractDto.getConnDto());
		
        if(extractDto.getConnDto().getConn_type().equalsIgnoreCase("ORACLE")) 
		{
		
			if(extractDto.getTableInfoDto().getIncr_flag().equalsIgnoreCase("Y")) {
				processGroupIndex=DBUtils.getProcessGroup(extractDto.getFeedDto().getFeed_name(),extractDto.getFeedDto().getCountry_code());
				
			}
		
			do {
				
				Thread.currentThread().sleep(10000);
				if(processGroupIndex==0) {
					index=1;
					//index=getRandomNumberInRange(1, NifiConstants.NOOFORACLEPROCESSORS);
				}
				else {
					index=processGroupIndex;
				}
				processGroupStatus=DBUtils.checkProcessGroupStatus(index,extractDto.getConnDto().getConn_type());
				if(processGroupStatus.equalsIgnoreCase("FREE")) {
					
					NifiConstants constants=new NifiConstants();
					String varName="ORACLEPROCESSGROUPURL"+index;
					processGroupUrl = String.valueOf(NifiConstants.class.getDeclaredField(varName).get(constants));
					listenHttpUrl= NifiConstants.ORACLELISTENER1;
					respEntity=getProcessGroupDetails(NifiConstants.NIFIURL, processGroupUrl);
					if (respEntity != null) {
						String content = EntityUtils.toString(respEntity);
						JSONObject controllerObj=getControllerObject(content);
						String controllerInfo=getControllerInfo(controllerObj);
						controllerId=controllerInfo.split(",")[0];
						clientId=controllerInfo.split(",")[1];
						controllerVersion=controllerInfo.split(",")[2];
						controllerStatus=controllerInfo.split(",")[3];
						System.out.println("controller id,clientId, version and status are : "+controllerId+" "+clientId+" "+controllerVersion+" "+controllerStatus);
						if(controllerStatus.equalsIgnoreCase("ENABLED")) {
							processorInfo=processorFree(controllerObj);
							if(!processorInfo.equalsIgnoreCase("NOT FREE")) {
								System.out.println("using processgroup"+index);
								System.out.println("processors being used are: "+processorInfo);
								trigger_flag="Y";
								
							}
						}
					}
				}
			}while(trigger_flag.equalsIgnoreCase("N"));	
			
		}
		
			
			
			
			
        if(extractDto.getConnDto().getConn_type().equalsIgnoreCase("ORACLE")||extractDto.getConnDto().getConn_type().equalsIgnoreCase("TERADARA")) {
				
				stopReferencingComponents(processorInfo, clientId);
				disableController(controllerId);
				updateController(connectionString , extractDto.getConnDto().getUserName(), extractDto.getConnDto().getEncr_key(), extractDto.getConnDto().getEncrypted_password(), controllerId);
				
				enableController(controllerId);
				startReferencingComponents(controllerId,processGroupUrl);
				System.out.println("Referencing componnent started");
				path=extractDto.getFeedDto().getCountry_code()+"/"+ extractDto.getFeedDto().getFeed_name()+"/"+today+"/"+runId+"/";
				JSONArray arr=createJsonObject(index,extractDto,connectionString,path,today, runId);
				invokeNifiFull(arr,listenHttpUrl);
		}
        
        if(extractDto.getConnDto().getConn_type().equalsIgnoreCase("UNIX")){
        	
        	
    		do {
    			Thread.currentThread().sleep(10000);
    			index=1;
    			//index=getRandomNumberInRange(1, NifiConstants.NOOFUNIXPROCESSORS);
    			processGroupStatus=DBUtils.checkProcessGroupStatus(index,extractDto.getConnDto().getConn_type());
    			if(processGroupStatus.equalsIgnoreCase("FREE")) {
    				
    				trigger_flag="Y";
    				
    			}
    			
    		}while(trigger_flag.equalsIgnoreCase("N"));
    		
    		String listener=NifiConstants.UNIXLISTENER1;
    		JSONArray jsonArr=createUnixJsonObject(extractDto,index,today, runId);
    		invokeNifiFull(jsonArr,listener);
        	
        }
			
			String updateStatus=DBUtils.updateNifiProcessgroupDetails(extractDto,path, today, runId.toString(), index);

			if(updateStatus.equalsIgnoreCase("success")) {
				
				System.out.println("Job triggered and details updated");
				createTriggerFile(feed_name,String.valueOf(runId));
				
			}
			else {
				throw new Exception("exeception occured while updating process details in metadata tables");
			}
			
			/*createSystemDetailsFile(src_sys_id, country_code,src_unique_name, buckets.toString(), src_sys_type, runId, today);
			createFileDetailsFile( src_sys_id,country_code, src_unique_name, src_sys_type, tableInfo, runId, today);*/
			
					 
		
	}
	
	

	private static void createTriggerFile( String feed_name,String runId) {
		// TODO Auto-generated method stub
		try {
            File file = new File("/home/juniper/scripts/"+feed_name+".txt");
            BufferedWriter output = new BufferedWriter(new FileWriter(file));
            output.write(runId);
            output.close();
        } catch ( IOException e ) {
            e.printStackTrace();
        }
		
	}


	
	
	private  static int getRandomNumberInRange(int min, int max) {

		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
			}
			Random r = new Random();
			return r.nextInt((max - min) + 1) + min;
	}
	
	private static HttpEntity getProcessGroupDetails(String nifiUrl, String processGroupUrl) throws ClientProtocolException, IOException {
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		HttpGet httpGet = new HttpGet(nifiUrl + processGroupUrl);
		HttpResponse response = httpClient.execute(httpGet);
		HttpEntity respEntity = response.getEntity();
		return respEntity;

	}
	
	
	@SuppressWarnings("rawtypes")
	private static JSONObject getControllerObject(String content) throws ClientProtocolException, IOException, org.json.simple.parser.ParseException {
		
		JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
		JSONArray jsonArray = (JSONArray) jsonObject.get("controllerServices");
		Iterator i = jsonArray.iterator();
		while(i.hasNext()) {
			JSONObject controllerObject=(JSONObject) i.next();
			JSONObject controllerComponent = (JSONObject) controllerObject.get("component");
			String name= (String) controllerComponent.get("name");
			if(!(name.contains("metadata"))) {
				System.out.println(name);
				return controllerObject;
			}
			
		}
		
		
		return null;

	}
	
	private static String getControllerInfo(JSONObject controllerJsonObject) throws Exception{
		
		
		String controllerId="";
		String controllerClientId="";
		String controllerVersion="";
		String controllerStatus="";
	
		controllerId=String.valueOf(controllerJsonObject.get("id"));
		JSONObject controllerRevision = (JSONObject) controllerJsonObject.get("revision");
		JSONObject controllerComponent=(JSONObject) controllerJsonObject.get("component");
		controllerClientId=String.valueOf(controllerRevision.get("clientId"));
		controllerVersion=String.valueOf(controllerRevision.get("version"));
		controllerStatus=String.valueOf(controllerComponent.get("state"));
		return controllerId+","+controllerClientId+","+controllerVersion+","+controllerStatus;
		
	}
	
	private static  String processorFree(JSONObject controllerJsonObject)
			throws ParseException, IOException, org.json.simple.parser.ParseException {
		
		int activeThreadCount=0;
		StringBuffer refComponents=new StringBuffer();
		JSONObject controllerComponent=(JSONObject)controllerJsonObject.get("component");
		JSONArray arrayOfReferencingComponents=(JSONArray)controllerComponent.get("referencingComponents");
		for(int i=0;i<arrayOfReferencingComponents.size();i++) {
			JSONObject refComp=(JSONObject)arrayOfReferencingComponents.get(i);
			String refCompId=String.valueOf(refComp.get("id"));
			JSONObject refCompRevision=(JSONObject)refComp.get("revision");
			String refCompVersion=String.valueOf(refCompRevision.get("version"));
			refComponents.append(refCompId+"~"+refCompVersion+",");
			JSONObject refCompComponent=(JSONObject)refComp.get("component");
			if(!String.valueOf(refCompComponent.get("activeThreadCount")).equals("0")) {
				activeThreadCount++;
			}else {
				System.out.println("No Active thread for processor "+refCompId);
			}
			
		}
		refComponents.setLength(refComponents.length()-1);
		if(activeThreadCount==0) {
			return refComponents.toString();
		}
		else {
			return "Not free";
		}

	}

	


		
		
		private static HttpEntity getControllerServiceDetails(String nifiUrl, String controllerServiceUrl) throws ClientProtocolException, IOException {
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			HttpGet httpGet = new HttpGet(nifiUrl + controllerServiceUrl);
			HttpResponse response = httpClient.execute(httpGet);
			HttpEntity respEntity = response.getEntity();
			return respEntity;

		}
		
		
		private static  String getClientId(String content) throws Exception {

			JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
			System.out.println("----->"+jsonObject.toJSONString());
			JSONObject jsonVersionObject = (JSONObject) jsonObject.get("revision");
			String version = jsonVersionObject.get("version").toString();
			Object clientID = jsonVersionObject.get("clientId");
			
			if(clientID == null) {
				clientID=UUID.randomUUID().toString();
			}
			
			return clientID + "," + version;

		}
		
		
		

		
		private static  void stopReferencingComponents(String processorInfo,String clientId) throws Exception {
			
			for (String processor : processorInfo.split(",")) {
				String processorId = processor.split("~")[0];
				String processorVersion = processor.split("~")[1];
				stopProcessor(processorId, processorVersion, clientId);
			}
		}

		
		private static void stopProcessor(String id,String version, String clientId) throws Exception {
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			

			StringEntity input = new StringEntity(NifiConstants.STOPPROCESSOR.replace("${id}", id)
						.replace("${version}", version).replace("${clientId}", clientId));

				input.setContentType("application/json;charset=UTF-8");
				HttpPut putRequest = new HttpPut(NifiConstants.NIFIURL + NifiConstants.PROCESSORURL.replace("${id}", id));

				putRequest.setEntity(input);
				CloseableHttpResponse httpResponse = httpClient.execute(putRequest);
				if (httpResponse.getStatusLine().getStatusCode() != 200) {
					System.out.println(EntityUtils.toString(httpResponse.getEntity()));
					throw new Exception("exception occured while stoping processor");
				}
				else {
					System.out.println("processor with id "+id+" stopped ");
				}

			
		}
		
		private static void disableController(String controllerId) throws Exception {
			
			HttpEntity respEntity=null;
			String clientId="";
			String controllerVersion="";
			
			
			respEntity=getControllerServiceDetails(NifiConstants.NIFIURL, "nifi-api/controller-services/"+controllerId);
			if (respEntity != null) {
				String content = EntityUtils.toString(respEntity);
				String clientIdVersion=getClientId(content);
				clientId=clientIdVersion.split(",")[0];
				controllerVersion=clientIdVersion.split(",")[1];
				
			}
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			
			String DISABLECOMMAND=NifiConstants.DISABLECONTROLLERCOMMAND.replace("${clientid}", clientId).replace("${version}", controllerVersion).replace("${id}",controllerId);
			System.out.println("command is "+DISABLECOMMAND);
			StringEntity input = new StringEntity(
					DISABLECOMMAND);
			input.setContentType("application/json;charset=UTF-8");
			System.out.println(input.toString());
			HttpPut putRequest = new HttpPut(NifiConstants.NIFIURL + "nifi-api/controller-services/"+controllerId );
			putRequest.setEntity(input);

			CloseableHttpResponse httpResponse = httpClient.execute(putRequest);
			if (httpResponse.getStatusLine().getStatusCode() != 200) {
				System.out.println(httpResponse.getStatusLine().toString());
				System.out.println(EntityUtils.toString(httpResponse.getEntity()));
				throw new Exception("exception occured while disabling controller");
			}
			else {
				System.out.println("controller disabled");
			}

		}
			
		@SuppressWarnings("static-access")
		private static void updateController(String connUrl,String uname,byte[] encrypted_key,byte[] encrypted_password,String controllerId) throws Exception {

			HttpEntity respEntity=null;
			String state="";
			String clientId="";
			String controllerVersion="";
			String password=EncryptionUtil.decyptPassword(encrypted_key, encrypted_password);
			System.out.println("decrypted password is "+password);
			
			do {
				Thread.currentThread().sleep(5000);
				respEntity=getControllerServiceDetails(NifiConstants.NIFIURL, "nifi-api/controller-services/"+controllerId);
				if (respEntity != null) {
					String content = EntityUtils.toString(respEntity);
					state=getControllerState(content);
					if(state.equalsIgnoreCase("DISABLED")) {
						String controllerInfo=getClientId(content);
						clientId=controllerInfo.split(",")[0];
						controllerVersion=controllerInfo.split(",")[1];
						
					}
			}
			
		}while(!state.equalsIgnoreCase("DISABLED"));
			System.out.println("controller is now disabled");
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			StringEntity input = new StringEntity(NifiConstants.UPDATEDBCONNECTIONPOOL.replace("${clientId}", clientId)
					.replace("${ver}", controllerVersion).replace("${contId}", controllerId)
					.replace("${conUrl}", connUrl).replace("${user}", uname)
					.replace("${pasword}", password));

			input.setContentType("application/json;charset=UTF-8");
			HttpPut putRequest = new HttpPut(NifiConstants.NIFIURL + "nifi-api/controller-services/"+controllerId);

			putRequest.setEntity(input);
			CloseableHttpResponse httpResponse = httpClient.execute(putRequest);
			if (httpResponse.getStatusLine().getStatusCode() != 200) {
				System.out.println(EntityUtils.toString(httpResponse.getEntity()));
				throw new Exception("exception occured while updating controller");
			}

		}
		
		
		
		private  static String getControllerState(String content) throws org.json.simple.parser.ParseException {
			JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
			JSONObject controllerComponent = (JSONObject) jsonObject.get("component");
			String state = controllerComponent.get("state").toString();
			return state;
		}
		
		private static void enableController(String controllerId) throws Exception{
			
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			HttpEntity respEntity=null;
			String controllerInfo="";
			String clientId="";
			String controllerVersion="";
			respEntity=getProcessGroupDetails(NifiConstants.NIFIURL, "nifi-api/controller-services/"+controllerId);
			if (respEntity != null) {
				String content = EntityUtils.toString(respEntity);
				controllerInfo=getClientId(content);
				clientId=controllerInfo.split(",")[0];
				controllerVersion=controllerInfo.split(",")[1];
		}
			
				StringEntity input = new StringEntity(NifiConstants.ENABLEDBCONNECTIONPOOL.replace("${clientId}", clientId)
						.replace("${ver}", controllerVersion).replace("${contId}", controllerId));
				input.setContentType("application/json;charset=UTF-8");
				HttpPut putRequest = new HttpPut(NifiConstants.NIFIURL + "nifi-api/controller-services/"+controllerId);

				putRequest.setEntity(input);
				CloseableHttpResponse httpResponse = httpClient.execute(putRequest);
				if (httpResponse.getStatusLine().getStatusCode() != 200) {
					System.out.println(EntityUtils.toString(httpResponse.getEntity()));
					throw new Exception("exception occured while enabling controller");
				}
				else {
					System.out.println("controller Enabling started");
				}
			}
			
		
		
		

		
		@SuppressWarnings("static-access")
		private static  void startReferencingComponents(String controllerId,String processGroupUrl)
				throws Exception {
			HttpEntity respEntity=null;
			String state="";
			String clientId="";
			
			do {
				Thread.currentThread().sleep(5000);
				respEntity=getControllerServiceDetails(NifiConstants.NIFIURL, "nifi-api/controller-services/"+controllerId);
				if (respEntity != null) {
					String content = EntityUtils.toString(respEntity);
					state=getControllerState(content);
				
			}
			
		}while(!state.equalsIgnoreCase("ENABLED"));
			System.out.println("controller is now enabled");
			respEntity=getProcessGroupDetails(NifiConstants.NIFIURL, processGroupUrl);
			if (respEntity != null) {
				String content = EntityUtils.toString(respEntity);
				JSONObject controllerObj=getControllerObject(content);
				StringBuffer refComponents=new StringBuffer();
				JSONObject controllerComponent=(JSONObject)controllerObj.get("component");
				JSONArray arrayOfReferencingComponents=(JSONArray)controllerComponent.get("referencingComponents");
				for(int i=0;i<arrayOfReferencingComponents.size();i++) {
					JSONObject refComp=(JSONObject)arrayOfReferencingComponents.get(i);
					String refCompId=String.valueOf(refComp.get("id"));
					JSONObject refCompRevision=(JSONObject)refComp.get("revision");
					String refCompVersion=String.valueOf(refCompRevision.get("version"));
					refComponents.append(refCompId+"~"+refCompVersion+",");
			}
			refComponents.setLength(refComponents.length()-1);
			for(String processor:refComponents.toString().split(",")) {
				String processorId=processor.split("~")[0];
				String processorVersion=processor.split("~")[1];
				CloseableHttpClient httpClient = HttpClientBuilder.create().build();
				StringEntity input = new StringEntity(NifiConstants.STARTPROCESSOR.replace("${id}", processorId)
						.replace("${version}", processorVersion).replace("${clientId}", clientId));

				input.setContentType("application/json;charset=UTF-8");
				HttpPut putRequest = new HttpPut(NifiConstants.NIFIURL + NifiConstants.PROCESSORURL.replace("${id}", processorId));

				putRequest.setEntity(input);
				CloseableHttpResponse httpResponse = httpClient.execute(putRequest);
				if (httpResponse.getStatusLine().getStatusCode() != 200) {
					System.out.println(EntityUtils.toString(httpResponse.getEntity()));
					throw new Exception("exception occured while starting processor");
				}

			}

		}
		}
		
			
		
		
		
		@SuppressWarnings("unchecked")
		private  static JSONArray createJsonObject(int index,ExtractDto extractDto,String conn_string,String path,String date,Long runId) throws Exception {

			JSONArray arr = new JSONArray();
			HashMap<String, JSONObject> map = new HashMap<String, JSONObject>();
			
			for(TableMetadataDto tableMetadata: extractDto.getTableInfoDto().getTableMetadataArr()) {
				StringBuffer gcsTarget=new StringBuffer();
				StringBuffer hdfsTarget=new StringBuffer();
				JSONObject json=new JSONObject();
				StringBuffer columnsWithQuotes=new StringBuffer();
				String[] columns=tableMetadata.getColumns().split(",");
				for(String column :columns) {
					column =new StringBuilder()
			        .append('\'')
			        .append(column)
			        .append('\'')
			        .append(',')
			        .toString();
					columnsWithQuotes.append(column);
				}
				columnsWithQuotes.setLength((columnsWithQuotes.length()-1));
				
				json.put("table_name", tableMetadata.getTable_name());
				if(!(tableMetadata.getColumns().equalsIgnoreCase("all"))) {
					json.put("columns", tableMetadata.getColumns());
					json.put("columns_where_clause","where upper(table_name)='"+tableMetadata.getTable_name().split("\\.")[1].toUpperCase()
							+"' and upper(owner)='"+tableMetadata.getTable_name().split("\\.")[0].toUpperCase()
							+ "' and upper(column_name) in("+columnsWithQuotes.toString().toUpperCase()+")");
				}else {
					json.put("columns_where_clause","where upper(table_name)='"+tableMetadata.getTable_name().split("\\.")[1].toUpperCase()
							+"' and upper(owner)='"+tableMetadata.getTable_name().split("\\.")[0].toUpperCase()+"'");
				}
				
				json.put("where_clause", tableMetadata.getWhere_clause());
				if(tableMetadata.getFetch_type().equalsIgnoreCase("INCR")) {
					json.put("incremental_column", tableMetadata.getIncr_col());
				}
				json.put("project_sequence", extractDto.getFeedDto().getProject_sequence());
				json.put("process_group", index);
				json.put("country_code", extractDto.getFeedDto().getCountry_code());
				json.put("feed_id",Integer.toString(extractDto.getFeedDto().getFeed_id()));
				json.put("feed_name", extractDto.getFeedDto().getFeed_name());
				json.put("date", date);
				json.put("run_id", runId);
				json.put("path", path);
				for(TargetDto targetDto :extractDto.getTargetArr()) {
					if(targetDto.getTarget_type().equalsIgnoreCase("gcs")) {
						gcsTarget.append(targetDto.getTarget_project()+"~");
						gcsTarget.append(targetDto.getService_account()+"~");
						gcsTarget.append(targetDto.getTarget_bucket()+",");
					}
					if(targetDto.getTarget_type().equalsIgnoreCase("hdfs")) {
						
						String password=EncryptionUtil.decyptPassword(targetDto.getEncrypted_key(), targetDto.getEncrypted_password());
						hdfsTarget.append(targetDto.getTarget_knox_url()+"~");
						hdfsTarget.append(targetDto.getTarget_user()+"~");
						hdfsTarget.append(password+"~");
						hdfsTarget.append(targetDto.getTarget_hdfs_path()+",");
					}
					
					
				}
				if(gcsTarget.length()>1) {
					gcsTarget.setLength(gcsTarget.length()-1);
					json.put("gcsTarget", gcsTarget.toString());
				}
				if(hdfsTarget.length()>1) {
					hdfsTarget.setLength(hdfsTarget.length()-1);
					json.put("hdfsTarget", hdfsTarget.toString());
				}

				map.put(tableMetadata.getTable_name()+"_obj", json);
				arr.add(map.get(tableMetadata.getTable_name()+"_obj"));
				
			}
			
			return arr;

		}
		
		@SuppressWarnings("unchecked")
		private static  JSONArray createUnixJsonObject(ExtractDto rtExtractDto,int index,String date,Long runId) throws Exception {

			JSONArray arr = new JSONArray();
			StringBuffer gcsTarget=new StringBuffer();
			StringBuffer hdfsTarget=new StringBuffer();
			for(FileMetadataDto file: rtExtractDto.getFileInfoDto().getFileMetadataArr()) {
				JSONObject json=new JSONObject();
				json.put("process_group", index);
				json.put("project_sequence", rtExtractDto.getFeedDto().getProject_sequence());
				json.put("feed_id", rtExtractDto.getFeedDto().getFeed_id());
				json.put("file_sequence", file.getFile_sequence());
				json.put("file_name", file.getFile_name());
				json.put("avro_conv_flg", file.getAvro_conv_flag());
				json.put("field_list", file.getField_list());
				json.put("date", date);
				json.put("file_type", file.getFile_type());
				json.put("file_delimiter", file.getFile_delimiter());
				json.put("feed_name", rtExtractDto.getFeedDto().getFeed_name());
				json.put("country_code", rtExtractDto.getFeedDto().getCountry_code());
				json.put("file_path", rtExtractDto.getFeedDto().getFilePath());
				json.put("run_id", runId);
				json.put("date", date);
				for(TargetDto targetDto :rtExtractDto.getTargetArr()) {
					if(targetDto.getTarget_type().equalsIgnoreCase("gcs")) {
						gcsTarget.append(targetDto.getTarget_project()+"~");
						gcsTarget.append(targetDto.getService_account()+"~");
						gcsTarget.append(targetDto.getTarget_bucket()+",");
					}
					if(targetDto.getTarget_type().equalsIgnoreCase("hdfs")) {
						
						String password=EncryptionUtil.decyptPassword(targetDto.getEncrypted_key(), targetDto.getEncrypted_password());
						
						hdfsTarget.append(targetDto.getTarget_knox_url()+"~");
						hdfsTarget.append(targetDto.getTarget_user()+"~");
						hdfsTarget.append(password+"~");
						hdfsTarget.append(targetDto.getTarget_hdfs_path()+",");
					}
					
					
				}
				if(gcsTarget.length()>1) {
					gcsTarget.setLength(gcsTarget.length()-1);
					
					json.put("gcsTarget", gcsTarget.toString());
				}
				if(hdfsTarget.length()>1) {
					hdfsTarget.setLength(hdfsTarget.length()-1);
					json.put("hdfsTarget", hdfsTarget.toString());
					
				}
				
				arr.add(json);
				
			}
		
			return arr;
		}

		
			


		
		
	
		
		@SuppressWarnings("rawtypes")
		private static void invokeNifiFull(JSONArray arr,String listenHttpUrl) throws UnsupportedOperationException, Exception {
			
			
					
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			Iterator it = arr.iterator();
			while (it.hasNext()) {
				JSONObject json=(JSONObject) it.next();
				HttpPost postRequest=new HttpPost(listenHttpUrl);
				StringEntity input = new StringEntity(json.toString());
				postRequest.setEntity(input); 
				HttpResponse response = httpClient.execute(postRequest);
				if(response.getStatusLine().getStatusCode()!=200) {
						System.out.println("Nifi listener problem"+response.getStatusLine().getStatusCode());
						throw new Exception("Nifi Not Running Or Some Problem In Sending HTTP Request"+response.getEntity().getContent().toString());
				}
				else {
					System.out.println("Nifi Triggered");
				}
			}
		}
		

		  public static void createSystemDetailsFile(String src_sys_id,String country_code,String src_unique_name,String bucket,String src_sys_type,String runId,String date) throws IOException, JSchException {

				
				
				
				String bucketName = bucket;
				StringBuffer stringBuffer = new StringBuffer();
				stringBuffer.append("\n");
				stringBuffer.append(src_unique_name+","+","+src_sys_id+src_sys_type+","+src_sys_type + " FEED"+","+bucketName+","+"gs://"+bucketName+"/"+src_sys_id+"-"+src_unique_name+"/"+date+"/"+runId+"/DATA"+","+GenericConstants.PROJECTID+","
						+","+","+"MC");
				JSch obj_JSch = new JSch();
				//obj_JSch.addIdentity("/home/birajrath2008/.ssh/id_rsa");
			    Session obj_Session = null;
			    try {
				obj_Session = obj_JSch.getSession("extraction_user", NifiConstants.NIFINSTANCEIP);
				obj_Session.setPort(22);
				obj_Session.setPassword("Infy@123");
				Properties obj_Properties = new Properties();
				obj_Properties.put("StrictHostKeyChecking", "no");
				obj_Session.setConfig(obj_Properties);
				obj_Session.connect();
				Channel obj_Channel = obj_Session.openChannel("sftp");
				obj_Channel.connect();
				ChannelSftp obj_SFTPChannel = (ChannelSftp) obj_Channel;
				InputStream obj_InputStream = new ByteArrayInputStream(stringBuffer.toString().getBytes());
				String homePath="/home/google/";
				obj_SFTPChannel.cd(homePath);
				String path=country_code+"/"+src_unique_name+"/"+date+"/"+runId+"/metadata";
				System.out.println("path is "+path);
				String[] folders = path.split( "/" );
				for ( String folder : folders ) {
				    if ( folder.length() > 0 ) {
				        try {
				        	obj_SFTPChannel.cd(folder);
				        	
				        }
				        catch ( SftpException e ) {
				        	obj_SFTPChannel.mkdir(folder);
				        	obj_SFTPChannel.cd(folder);
				        	
				        }
				    }
				}

				obj_SFTPChannel.put(obj_InputStream,"/home/google/"+ path+"/" + "mstr_src_sys_dtls.csv" );
				obj_SFTPChannel.exit();
				obj_InputStream.close();
				obj_Channel.disconnect();
				obj_Session.disconnect();
				
		       
				}catch ( Exception e ) {
				e.printStackTrace();
			}
				

				
			}

		public static  void createFileDetailsFile(String src_sys_id,String country_code,String src_unique_name,String conn_type,ArrayList<Map<String,String>> tableInfo,String runId,String date) throws IOException, JSchException, SftpException  {
			
			
			
			
			StringBuffer filebuffer = new StringBuffer();
			filebuffer.append("\n");
			for(Map<String,String> tbl:tableInfo) {
				if(tbl.get("fetch_type").equalsIgnoreCase("full")) {
					filebuffer.append(src_unique_name+","+tbl.get("table_name")+","+tbl.get("table_name")+","+src_sys_id+"_samplefile"+","+"A"+","+""+","+tbl.get("table_name")+","+","+"0"+","+"0"+","+"0"+","+"0"+","+"SQL"+","+"F"+"\n");
				}
				else {
					filebuffer.append(src_unique_name+","+tbl.get("table_name")+","+tbl.get("table_name")+","+src_sys_id+"_samplefile"+","+"A"+","+""+","+tbl.get("table_name")+","+","+"0"+","+"0"+","+"0"+","+"0"+","+"SQL"+","+"I"+"\n");
				}
				
				}
			JSch obj_JSch = new JSch();
			//obj_JSch.addIdentity("/home/birajrath2008/.ssh/id_rsa");
		    Session obj_Session = null;
			try {
					obj_Session = obj_JSch.getSession("extraction_user", NifiConstants.NIFINSTANCEIP);
					obj_Session.setPort(22);
					 obj_Session.setPassword("Infy@123");
					Properties obj_Properties = new Properties();
					obj_Properties.put("StrictHostKeyChecking", "no");
					obj_Session.setConfig(obj_Properties);
					obj_Session.connect();
					Channel obj_Channel = obj_Session.openChannel("sftp");
					obj_Channel.connect();
					ChannelSftp obj_SFTPChannel = (ChannelSftp) obj_Channel;
					InputStream obj_InputStream = new ByteArrayInputStream(filebuffer.toString().getBytes());
					String homePath="/home/google/";
					obj_SFTPChannel.cd(homePath);
					String path=country_code+"/"+src_unique_name+"/"+date+"/"+runId+"/METADATA";
					System.out.println("path is "+path);
					String[] folders = path.split( "/" );
					for ( String folder : folders ) {
					    if ( folder.length() > 0 ) {
					        try {
					        	obj_SFTPChannel.cd(folder);
					        	System.out.println("inside "+folder);
					        }
					        catch ( SftpException e ) {
					        	obj_SFTPChannel.mkdir(folder);
					        	obj_SFTPChannel.cd(folder);
					        	System.out.println(folder+" created");
					        }
					    }
					}

					obj_SFTPChannel.put(obj_InputStream,"/home/google/"+ path+"/" + "mstr_file_dtls.csv" );
					obj_SFTPChannel.exit();
					obj_InputStream.close();
					obj_Channel.disconnect();
					obj_Session.disconnect();
					
			       
					}catch ( Exception e ) {
		    		e.printStackTrace();
		    	}
				



		}
		
		
		
		
		
		
	}
	
	
		



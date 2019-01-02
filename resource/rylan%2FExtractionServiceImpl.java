package com.iig.gcp.extraction.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Service;

import com.iig.gcp.extraction.dto.SourceSystemDetailBean;
import com.iig.gcp.extraction.dto.DataDetailBean;
import com.iig.gcp.extraction.dto.DriveMaster;
import com.iig.gcp.constants.OracleConstants;
import com.iig.gcp.extraction.dto.ConnectionMaster;
import com.iig.gcp.extraction.dto.CountryMaster;
import com.iig.gcp.extraction.dto.ReservoirMaster;
import com.iig.gcp.extraction.dto.SourceSystemMaster;
import com.iig.gcp.utils.ConnectionUtils;

@Service
public class ExtractionServiceImpl implements ExtractionService {
	@Override
	public String invokeRest(String json, String url) throws UnsupportedOperationException, Exception {
		String resp = null;
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost postRequest = new HttpPost(OracleConstants.EXTRACTION_COMPUTE_URL + url);
		postRequest.setHeader("Content-Type", "application/json");
		StringEntity input = new StringEntity(json);
		postRequest.setEntity(input);
		HttpResponse response = httpClient.execute(postRequest);
		String response_string = EntityUtils.toString(response.getEntity(), "UTF-8");
		if (response.getStatusLine().getStatusCode() != 200) {
			resp = "Error" + response_string;
			throw new Exception("Error" + response_string);
		} else {
			resp = response_string;
		}
		return resp;
	}

	@Override
	public ArrayList<ConnectionMaster> getConnections(String src_val) {
		// TODO Auto-generated method stub
		Connection connection;
		ConnectionMaster conn = null;
		ArrayList<ConnectionMaster> arrConnectionMaster = new ArrayList<ConnectionMaster>();
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement(OracleConstants.GET_SOURCE_CONNECTIONS);
			pstm.setString(1, src_val);
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				conn = new ConnectionMaster();
				conn.setConnection_id(rs.getInt(1));
				conn.setConnection_name(rs.getString(2));
				conn.setHost_name(rs.getString(3));
				conn.setPort_no(rs.getString(4));
				conn.setUsername(rs.getString(5));
				conn.setPassword(rs.getString(6));
				conn.setDatabase_name(rs.getString(7));
				conn.setService_name(rs.getString(8));
				arrConnectionMaster.add(conn);
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}

		return arrConnectionMaster;
	}

	@Override
	public ArrayList<String> getTargets() {
		ArrayList<String> arr = new ArrayList<String>();
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select target_unique_name from JUNIPER_EXT_TARGET_MASTER");
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				arr.add(rs.getString(1));
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return arr;
	}
	
	@Override
	public ArrayList<String> getTargets1(String tgt) {
		ArrayList<String> arr = new ArrayList<String>();
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select "
					+ "target_unique_name,target_type,target_project,service_account,target_bucket,target_knox_url,target_hdfs_path,target_user,target_password"
					+ " from JUNIPER_EXT_TARGET_MASTER where target_unique_name='"+tgt+"'");
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				arr.add(rs.getString(1));
				arr.add(rs.getString(2));
				arr.add(rs.getString(3));
				arr.add(rs.getString(4));
				arr.add(rs.getString(5));
				arr.add(rs.getString(6));
				arr.add(rs.getString(7));
				arr.add(rs.getString(8));
				arr.add(rs.getString(9));
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return arr;
	}
	
	@Override
	public ConnectionMaster getConnections1(String src_val,int src_sys_id) {
		// TODO Auto-generated method stub
		Connection connection;
		ConnectionMaster conn = new ConnectionMaster();
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select connection_id from JUNIPER_EXT_EXTRACTION_MASTER where src_sys_id="+src_sys_id);
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				conn.setConnection_id(rs.getInt(1));
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	@Override
	public ConnectionMaster getConnections2(String src_val,int conn_id) {
		// TODO Auto-generated method stub
		Connection connection;
		ConnectionMaster conn = new ConnectionMaster();
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select * from JUNIPER_EXT_CONNECTION_MASTER where connection_id="+conn_id);
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				conn.setConnection_id(rs.getInt(1));
				conn.setConnection_name(rs.getString(2));
				conn.setConnection_type(rs.getString(3));
				conn.setHost_name(rs.getString(4));
				conn.setPort_no(rs.getString(5));
				conn.setUsername(rs.getString(6));
				conn.setPassword(rs.getString(7));
				conn.setDatabase_name(rs.getString(8));
				conn.setService_name(rs.getString(9));
			}
			connection.close();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	@Override
	public String getExtType(int src_sys_id) {
		// TODO Auto-generated method stub
		String val=null;
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select extraction_type from JUNIPER_EXT_EXTRACTION_MASTER where src_sys_id="+src_sys_id);
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				val=rs.getString(1);
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return val;
	}
	
	@Override
	public String getExtType1(String src_unique_name) {
		// TODO Auto-generated method stub
		String val=null;
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select extraction_type from JUNIPER_EXT_EXTRACTION_MASTER "
					+ "where src_sys_id=(select src_sys_id from JUNIPER_EXT_SRC_SYS_MASTER where src_unique_name='"+src_unique_name+"')");
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				val=rs.getString(1);
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return val;
	}
	
	@Override
	public ArrayList<String> getTables(String src_val, int conn_id, String schema_name) {
		ArrayList<ConnectionMaster> arrcm = getConnections(src_val);
		ArrayList<String> arrTbl = new ArrayList<String>();
		String query = null, connectionUrl = null, host = null, port = null, username = null, password = null, db = null, service = null;
		Connection serverConnection = null;
		Statement st = null;
		for (ConnectionMaster cm : arrcm) {
			if (conn_id == cm.getConnection_id()) {
				host = cm.getHost_name();
				port = cm.getPort_no();
				username = cm.getUsername();
				password = cm.getPassword();
				db = cm.getDatabase_name();
				service = cm.getService_name();
			}
		}
		try {
			if (src_val.equalsIgnoreCase("Mssql")) {
				query = "SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='" + db + "'";
				connectionUrl = "jdbc:sqlserver://" + host + ":" + port + ";DatabaseName=" + db;
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			} else if (src_val.equalsIgnoreCase("unix"))
				;
			else {
				query = "select distinct table_name from all_tables where owner='"+schema_name+"' order by table_name";
				Class.forName("oracle.jdbc.driver.OracleDriver");
				connectionUrl = "jdbc:oracle:thin:@" + host + ":" + port + "/" + service + "";
			}
			if (src_val.equalsIgnoreCase("unix"))
				;
			else {
				serverConnection = DriverManager.getConnection(connectionUrl, username, password);
				st = serverConnection.createStatement();

				ResultSet rs2 = st.executeQuery(query);
				while (rs2.next()) {
					if (src_val.equalsIgnoreCase("Mssql")) {
						arrTbl.add(rs2.getString(1) + "." + rs2.getString(2));
					} else {
						arrTbl.add(rs2.getString(1));
					}
				}
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (src_val.equalsIgnoreCase("unix"))
				;
			else {
				try {
					st.close();
					serverConnection.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return arrTbl;
	}

	@Override
	public ArrayList<String> getFields(String id, String src_val, String table_name, int conn_id, String schema_name) {
		ArrayList<ConnectionMaster> arrcm = getConnections(src_val);
		ArrayList<String> arrFld = new ArrayList<String>();
		String query = null, connectionUrl = null, host = null, port = null, username = null, password = null, db = null, service = null;
		Connection serverConnection = null;
		Statement st = null;
		for (ConnectionMaster cm : arrcm) {
			if (conn_id == cm.getConnection_id()) {
				host = cm.getHost_name();
				port = cm.getPort_no();
				username = cm.getUsername();
				password = cm.getPassword();
				db = cm.getDatabase_name();
				service = cm.getService_name();
			}
		}
		String tbls[] = table_name.split(",");
		for (int i = 0; i < tbls.length; i++) {
			try {
				if (src_val.equalsIgnoreCase("Mssql")) {
					String temp = tbls[i].substring(tbls[i].lastIndexOf(".") + 1);
					query = "select ColumnName = col.column_name " + "FROM information_schema.tables tbl INNER JOIN information_schema.columns col ON col.table_name = tbl.table_name "
							+ "LEFT JOIN sys.extended_properties prop ON prop.major_id = object_id(tbl.table_schema + '.' + tbl.table_name) AND prop.minor_id = 0 AND prop.name = '" + temp + "' "
							+ "WHERE tbl.table_type = 'base table' AND tbl.table_name = '" + temp + "'";
					connectionUrl = "jdbc:sqlserver://" + host + ":" + port + ";DatabaseName=" + db;
					Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				} else if (src_val.equalsIgnoreCase("Unix"))
					;
				else {
					String tblsx[] = tbls[i].split("\\.");
					query = "SELECT column_name FROM all_tab_cols where table_name='" + tblsx[1] + "' and owner='"+schema_name+"' order by column_name";
					Class.forName("oracle.jdbc.driver.OracleDriver");
					connectionUrl = "jdbc:oracle:thin:@" + host + ":" + port + "/" + service + "";
				}
				if (src_val.equalsIgnoreCase("Unix"))
					;
				else {
					serverConnection = DriverManager.getConnection(connectionUrl, username, password);
					st = serverConnection.createStatement();
					ResultSet rs2 = st.executeQuery(query);
					while (rs2.next()) {
						arrFld.add(rs2.getString(1));
					}
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if (src_val.equalsIgnoreCase("Unix"))
					;
				else {
					try {
						st.close();
						serverConnection.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		return arrFld;
	}
	
	@Override
	public ArrayList<SourceSystemMaster> getSources(String src_val) {
		SourceSystemMaster ssm = null;
		ArrayList<SourceSystemMaster> arrssm = new ArrayList<SourceSystemMaster>();
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select a.src_sys_id,a.src_unique_name from JUNIPER_EXT_SRC_SYS_MASTER a,JUNIPER_EXT_EXTRACTION_MASTER b where a.src_sys_id=b.src_sys_id");
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				ssm = new SourceSystemMaster();
				ssm.setSrc_sys_id(rs.getInt(1));
				ssm.setSrc_unique_name(rs.getString(2));
				arrssm.add(ssm);
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return arrssm;
	}

	@Override
	public ArrayList<SourceSystemDetailBean> getSources1(String src_val,int src_sys_id) {
		SourceSystemDetailBean ssm = null;
		ArrayList<SourceSystemDetailBean> arrssm = new ArrayList<SourceSystemDetailBean>();
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement(
					"select a.src_sys_id,a.src_unique_name,a.src_sys_desc,a.country_code,b.extraction_type,b.target,b.connection_id "
					+ "from JUNIPER_EXT_SRC_SYS_MASTER a , JUNIPER_EXT_EXTRACTION_MASTER b where a.src_sys_id = b.src_sys_id "
					+ "and a.src_sys_id="+src_sys_id);
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				ssm = new SourceSystemDetailBean();
				ssm.setSrc_sys_id(rs.getInt(1));
				ssm.setSrc_unique_name(rs.getString(2));
				ssm.setSrc_sys_desc(rs.getString(3));
				ssm.setCountry_code(rs.getString(4));
				ssm.setExtraction_type(rs.getString(5));
				ssm.setTarget(rs.getString(6));
				ssm.setConnection_id(rs.getInt(7));
				arrssm.add(ssm);
			}
			connection.close();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			// connection.close();
		}
		// connection.close();
		return arrssm;
	}

	@Override
	public ArrayList<CountryMaster> getCountries() {
		CountryMaster cm = null;
		ArrayList<CountryMaster> arrcm = new ArrayList<CountryMaster>();
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select country_code,country_name from JUNIPER_EXT_COUNTRY_MASTER");
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				cm = new CountryMaster();
				cm.setCountry_code(rs.getString(1));
				cm.setCountry_name(rs.getString(2));
				arrcm.add(cm);
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return arrcm;
	}
	
	@Override
	public ArrayList<ReservoirMaster> getReservoirs() {
		ReservoirMaster rm = null;
		ArrayList<ReservoirMaster> arrrm = new ArrayList<ReservoirMaster>();
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select reservoir_id,reservoir_name,reservoir_desc from reservoir_master where reservoir_status='Y' and lower(reservoir_desc) like '%extraction%'");
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				rm = new ReservoirMaster();
				rm.setReservoir_id(rs.getInt(1));
				rm.setReservoir_name(rs.getString(2));
				rm.setReservoir_desc(rs.getString(3));
				arrrm.add(rm);
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return arrrm;
	}
	
	@Override
	public ArrayList<DataDetailBean> getData(int src_sys_id,String src_val, String schema_name) {
		DataDetailBean ddb = null;
		ArrayList<DataDetailBean> arrddb = new ArrayList<DataDetailBean>();
		ConnectionMaster conn = getConnections1(src_val, src_sys_id);
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement(
					"select table_name, columns, where_clause, fetch_type, incr_col from JUNIPER_EXT_TABLE_MASTER where src_sys_id="+src_sys_id);
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				ddb = new DataDetailBean();
				ddb.setTable_name(rs.getString(1));
				ddb.setTable_name_short(rs.getString(1).split("\\.")[1]);
				ddb.setSchema(rs.getString(1).split("\\.")[0]);
				ddb.setColumn_name(rs.getString(2));
				ddb.setWhere_clause(rs.getString(3));
				ddb.setFetch_type(rs.getString(4));
				ddb.setIncremental_column(rs.getString(5));
				ArrayList<String> agf=getFields("1", src_val, rs.getString(1), conn.getConnection_id(),schema_name);
				String cols = agf.toString();
				cols = cols.substring(1, cols.length() - 1).replace(", ", ",");
				ddb.setCols(cols);
				arrddb.add(ddb);
			}
			connection.close();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			//connection.close();
		}
		
		return arrddb;
	}
	
	@Override
	public int checkNames(String sun)
	{
		Connection connection=null;
		int stat=0;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select src_unique_name from JUNIPER_EXT_SRC_SYS_MASTER where src_unique_name='"+sun+"'");
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				stat=1;break;
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		ConnectionUtils.closeQuietly(connection);
		return stat;
	}
	
	@Override
	public ArrayList<String> getSchema(String src_val,int conn_id)
	{
		ArrayList<ConnectionMaster> arrcm = getConnections(src_val);
		ArrayList<String> arrTbl = new ArrayList<String>();
		String query = null, connectionUrl = null, host = null, port = null, username = null, password = null, db = null, service = null;
		Connection serverConnection = null;
		Statement st = null;
		for (ConnectionMaster cm : arrcm) {
			if (conn_id == cm.getConnection_id()) {
				host = cm.getHost_name();
				port = cm.getPort_no();
				username = cm.getUsername();
				password = cm.getPassword();
				db = cm.getDatabase_name();
				service = cm.getService_name();
			}
		}
		try {
				query = "select distinct owner from all_tables where upper(owner) not like '%SYS%' and upper(owner) not like '%XDB%'";
				Class.forName("oracle.jdbc.driver.OracleDriver");
				connectionUrl = "jdbc:oracle:thin:@" + host + ":" + port + "/" + service + "";
				serverConnection = DriverManager.getConnection(connectionUrl, username, password);
				st = serverConnection.createStatement();
				ResultSet rs2 = st.executeQuery(query);
				while (rs2.next()) {
						arrTbl.add(rs2.getString(1));
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			try {
				st.close();
				serverConnection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return arrTbl;
	}
	public String getSchemaData(String src_val,int src_sys_id)
	{
		ConnectionMaster conn = getConnections1(src_val, src_sys_id);
		String sch="";
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement(
					"select table_name from JUNIPER_EXT_TABLE_MASTER where src_sys_id="+src_sys_id);
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				sch=rs.getString(1).split("\\.")[0];
			}
			connection.close();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			//connection.close();
		}
		return sch;
	}

	public ArrayList<String> getSystem(String project)
	{
		ArrayList<String> sys=new ArrayList<String>();
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement(
					"select a.system_name from JUNIPER_SYSTEM_MASTER a,JUNIPER_PRO_SYS_LINK b "
					+ "where a.SYSTEM_SEQUENCE=b.SYSTEM_SEQUENCE and b.PROJECT_SEQUENCE="
					+ "(select project_sequence from JUNIPER_PROJECT_MASTER where project_id='"+project+"')");
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				sys.add(rs.getString(1));
			}
			connection.close();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			//connection.close();
		}
		return sys;
	}
	
	@Override
	public ArrayList<DriveMaster> getDrives() {
		DriveMaster dm = null;
		ArrayList<DriveMaster> arrdm = new ArrayList<DriveMaster>();
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select * from JUNIPER_EXT_DRIVE_MASTER");
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				dm = new DriveMaster();
				dm.setDrive_id(rs.getInt(1));
				dm.setDrive_name(rs.getString(2));
				dm.setDrive_path(rs.getString(3));
				dm.setProject_id(rs.getString(4));
				arrdm.add(dm);
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return arrdm;
	}
	
	@Override
	public ArrayList<DriveMaster> getDrives1(int src_sys_id) {
		DriveMaster dm = null;
		ArrayList<DriveMaster> arrdm = new ArrayList<DriveMaster>();
		Connection connection;
		try {
			connection = ConnectionUtils.getConnection();
			PreparedStatement pstm = connection.prepareStatement("select c.drive_name,c.mounted_path from JUNIPER_EXT_EXTRACTION_MASTER a, JUNIPER_EXT_CONNECTION_MASTER b, JUNIPER_EXT_DRIVE_MASTER c where a.connection_id=b.connection_id and b.drive_id=c.drive_id and a.src_sys_id="+src_sys_id);
			ResultSet rs = pstm.executeQuery();
			while (rs.next()) {
				dm = new DriveMaster();
				dm.setDrive_name(rs.getString(1));
				dm.setDrive_path(rs.getString(2));
				arrdm.add(dm);
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return arrdm;
	}
}
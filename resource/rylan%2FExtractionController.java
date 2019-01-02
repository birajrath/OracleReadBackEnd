package com.iig.gcp.controllers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.SessionAttributes;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;

import com.iig.gcp.extraction.dto.ConnectionMaster;
import com.iig.gcp.extraction.dto.CountryMaster;
import com.iig.gcp.extraction.dto.DataDetailBean;
import com.iig.gcp.extraction.dto.DriveMaster;
import com.iig.gcp.extraction.dto.SourceSystemDetailBean;
import com.iig.gcp.extraction.dto.SourceSystemMaster;
import com.iig.gcp.extraction.service.ExtractionService;
import com.iig.gcp.login.dto.UserAccount;

@Controller
@SessionAttributes(value= {"user","arrProject","menu_code","project","user_sq","projectFeatureMap"})
public class ExtractionController {

	@Autowired
	private ExtractionService es;

	@RequestMapping(value = "/extraction/ConnectionHome", method = RequestMethod.GET)
	public ModelAndView ConnectionHome() {
		return new ModelAndView("extraction/ConnectionHome");
	}

	@RequestMapping(value = "/extraction/ConnectionDetails", method = RequestMethod.POST)
	public ModelAndView ConnectionDetails(@Valid @ModelAttribute("src_val") String src_val, ModelMap model,HttpServletRequest request) {
		model.addAttribute("src_val", src_val);
		UserAccount u=(UserAccount) request.getSession().getAttribute("user");
		model.addAttribute("usernm", u.getUser_id());
		model.addAttribute("project", (String)request.getSession().getAttribute("project"));
		ArrayList<String> system = es.getSystem((String)request.getSession().getAttribute("project"));
		model.addAttribute("system", system);
		ArrayList<ConnectionMaster> conn_val = es.getConnections(src_val);
		model.addAttribute("conn_val", conn_val);
		ArrayList<DriveMaster> drive = es.getDrives();
		model.addAttribute("drive",drive);
		if(src_val.equalsIgnoreCase("Oracle")) { return new ModelAndView("extraction/ConnectionDetailsOracle"); }
		else if(src_val.equalsIgnoreCase("Teradata")) { return new ModelAndView("extraction/ConnectionDetailsTeradata"); }
		else if(src_val.equalsIgnoreCase("Mssql")) { return new ModelAndView("extraction/ConnectionDetailsMssql"); }
		else if(src_val.equalsIgnoreCase("Unix")) { return new ModelAndView("extraction/ConnectionDetailsUnix"); }
		else if(src_val.equalsIgnoreCase("Hadoop")) { return new ModelAndView("extraction/ConnectionDetailsHadoop"); }
		else if(src_val.equalsIgnoreCase("Gcs")) { return new ModelAndView("extraction/ConnectionDetailsGcs"); }
		else { return new ModelAndView("extraction/ConnectionDetails"); }
	}

	@RequestMapping(value = "/extraction/ConnectionDetails1", method = RequestMethod.POST)
	public ModelAndView ConnectionDetails1(@Valid @ModelAttribute("x") String x, @ModelAttribute("src_val") String src_val, @ModelAttribute("button_type") String button_type, ModelMap model,HttpServletRequest request) throws UnsupportedOperationException, Exception {
		String resp = null;
		if(button_type.equalsIgnoreCase("add"))
			resp = es.invokeRest(x, "addConnection");
		else if (button_type.equalsIgnoreCase("upd"))
			resp = es.invokeRest(x, "updConnection");
		else if (button_type.equalsIgnoreCase("del"))
			resp = es.invokeRest(x, "delConnection");
		ArrayList<ConnectionMaster> conn_val = es.getConnections(src_val);
		model.addAttribute("successString", resp.toString());
		model.addAttribute("src_val", src_val);
		UserAccount u=(UserAccount) request.getSession().getAttribute("user");
		model.addAttribute("usernm", u.getUser_id());
		model.addAttribute("project", (String)request.getSession().getAttribute("project"));
		ArrayList<String> system = es.getSystem((String)request.getSession().getAttribute("project"));
		model.addAttribute("system", system);
		model.addAttribute("conn_val", conn_val);
		return new ModelAndView("extraction/ConnectionDetails");
	}

	@RequestMapping(value = "/extraction/ConnectionDetailsEdit", method = RequestMethod.POST)
	public ModelAndView ConnectionDetailsEdit(@Valid @ModelAttribute("conn") int conn, @ModelAttribute("src_val") String src_val, ModelMap model,HttpServletRequest request) throws UnsupportedOperationException, Exception {
		ConnectionMaster conn_val = es.getConnections2(src_val,conn);
		model.addAttribute("conn_val", conn_val);
		model.addAttribute("src_val", src_val);
		UserAccount u=(UserAccount) request.getSession().getAttribute("user");
		model.addAttribute("usernm", u.getUser_id());
		model.addAttribute("project", (String)request.getSession().getAttribute("project"));
		ArrayList<String> system = es.getSystem((String)request.getSession().getAttribute("project"));
		model.addAttribute("system", system);
		if(src_val.equalsIgnoreCase("Oracle")) { return new ModelAndView("extraction/ConnectionDetailsEditOracle"); }
		else if(src_val.equalsIgnoreCase("Teradata")) { return new ModelAndView("extraction/ConnectionDetailsEditTeradata"); }
		else if(src_val.equalsIgnoreCase("Mssql")) { return new ModelAndView("extraction/ConnectionDetailsEditMssql"); }
		else if(src_val.equalsIgnoreCase("Unix")) { return new ModelAndView("extraction/ConnectionDetailsEditUnix"); }
		else if(src_val.equalsIgnoreCase("Hadoop")) { return new ModelAndView("extraction/ConnectionDetailsEditHadoop"); }
		else if(src_val.equalsIgnoreCase("Gcs")) { return new ModelAndView("extraction/ConnectionDetailsEditGcs"); }
		else { return new ModelAndView("extraction/ConnectionDetailsEdit"); }
	}
	
	@RequestMapping(value = "/extraction/TargetDetails", method = RequestMethod.GET)
	public ModelAndView TargetDetails(@Valid ModelMap model,HttpServletRequest request) {
		ArrayList<String> tgt = es.getTargets();
		UserAccount u=(UserAccount) request.getSession().getAttribute("user");
		model.addAttribute("usernm", u.getUser_id());
		model.addAttribute("project", (String)request.getSession().getAttribute("project"));
		ArrayList<String> system = es.getSystem((String)request.getSession().getAttribute("project"));
		model.addAttribute("system", system);
		model.addAttribute("tgt_val", tgt);
		ArrayList<DriveMaster> drive = es.getDrives();
		model.addAttribute("drive",drive);
		return new ModelAndView("extraction/TargetDetails");
	}
	
	@RequestMapping(value = "/extraction/TargetDetails1", method = RequestMethod.POST)
	public ModelAndView TargetDetails1(@Valid @ModelAttribute("x") String x, @ModelAttribute("button_type") String button_type, ModelMap model,HttpServletRequest request) throws UnsupportedOperationException, Exception {
//		System.out.println(x);
		String resp = null;
		if(button_type.equalsIgnoreCase("add"))
			resp = es.invokeRest(x, "addTarget");
		else if (button_type.equalsIgnoreCase("upd"))
			resp = es.invokeRest(x, "updTarget");
		else if (button_type.equalsIgnoreCase("del"))
			resp = es.invokeRest(x, "delTarget");
		model.addAttribute("successString", resp.toString());
		UserAccount u=(UserAccount) request.getSession().getAttribute("user");
		model.addAttribute("usernm", u.getUser_id());
		model.addAttribute("project", (String)request.getSession().getAttribute("project"));
		ArrayList<String> system = es.getSystem((String)request.getSession().getAttribute("project"));
		model.addAttribute("system", system);
		ArrayList<DriveMaster> drive = es.getDrives();
		model.addAttribute("drive",drive);
		return new ModelAndView("extraction/TargetDetails");
	}
	
	@RequestMapping(value = "/extraction/TargetDetailsEdit", method = RequestMethod.POST)
	public ModelAndView TargetDetailsEdit(@Valid @ModelAttribute("tgt") String tgt,ModelMap model,HttpServletRequest request) {
		ArrayList<String> tgtx = es.getTargets1(tgt);
		model.addAttribute("tgtx", tgtx);
		UserAccount u=(UserAccount) request.getSession().getAttribute("user");
		model.addAttribute("usernm", u.getUser_id());
		model.addAttribute("project", (String)request.getSession().getAttribute("project"));
		ArrayList<String> system = es.getSystem((String)request.getSession().getAttribute("project"));
		model.addAttribute("system", system);
		return new ModelAndView("extraction/TargetDetailsEdit");
	}
	
	@RequestMapping(value = "/extraction/SystemHome", method = RequestMethod.GET)
	public ModelAndView SystemHome() {
		return new ModelAndView("extraction/SystemHome");
	}

	@RequestMapping(value = "/extraction/SystemDetails", method = RequestMethod.POST)
	public ModelAndView SystemDetails(@Valid @ModelAttribute("src_val") String src_val, ModelMap model) {
		model.addAttribute("src_val", src_val);
		ArrayList<SourceSystemMaster> src_sys_val = es.getSources(src_val);
		model.addAttribute("src_sys_val", src_sys_val);
		ArrayList<ConnectionMaster> conn_val = es.getConnections(src_val);
		model.addAttribute("conn_val", conn_val);
		ArrayList<String> tgt = es.getTargets();
		model.addAttribute("tgt", tgt);
		/*ArrayList<String> buckets = DBUtils.getBuckets();
		model.addAttribute("buckets", buckets);*/
		ArrayList<CountryMaster> countries = es.getCountries();
		model.addAttribute("countries", countries);
		/*ArrayList<ReservoirMaster> reservoir = es.getReservoirs();
		model.addAttribute("reservoir", reservoir);*/
		return new ModelAndView("extraction/SystemDetails");
	}

	@RequestMapping(value = "/extraction/SystemDetails1", method = RequestMethod.POST)
	public ModelAndView SystemDetails1(@Valid @RequestParam(value = "sun", required = true) String sun, ModelMap model) throws UnsupportedOperationException, Exception {
		int stat = es.checkNames(sun);
		model.addAttribute("stat", stat);
		return new ModelAndView("extraction/SystemDetails1");
	}

	@RequestMapping(value = "/extraction/SystemDetails2", method = RequestMethod.POST)
	public ModelAndView SystemDetails2(@Valid @ModelAttribute("src_val") String src_val, @ModelAttribute("x") String x, @ModelAttribute("button_type") String button_type, ModelMap model) throws UnsupportedOperationException, Exception {
//		System.out.println(x);
		String resp = null;
		if(button_type.equalsIgnoreCase("add"))
			resp = es.invokeRest(x, "onboardSystem");
		else if (button_type.equalsIgnoreCase("upd"))
			resp = es.invokeRest(x, "updSystem");
		else if (button_type.equalsIgnoreCase("del"))
			resp = es.invokeRest(x, "delSystem");
		model.addAttribute("successString", resp.toString());
		model.addAttribute("src_val", src_val);
		ArrayList<SourceSystemMaster> src_sys_val = es.getSources(src_val);
		model.addAttribute("src_sys_val", src_sys_val);
		ArrayList<ConnectionMaster> conn_val = es.getConnections(src_val);
		model.addAttribute("conn_val", conn_val);
		ArrayList<String> tgt = es.getTargets();
		model.addAttribute("tgt", tgt);
		/*ArrayList<String> buckets = DBUtils.getBuckets();
		model.addAttribute("buckets", buckets);*/
		ArrayList<CountryMaster> countries = es.getCountries();
		model.addAttribute("countries", countries);
		/*ArrayList<ReservoirMaster> reservoir = es.getReservoirs();
		model.addAttribute("reservoir", reservoir);*/
		return new ModelAndView("extraction/SystemDetails");
	}

	@RequestMapping(value = "/extraction/SystemDetailsEdit", method = RequestMethod.POST)
	public ModelAndView SystemDetailsEdit(@Valid @ModelAttribute("src_sys") int src_sys, @ModelAttribute("src_val") String src_val, ModelMap model) throws UnsupportedOperationException, Exception {
		ArrayList<SourceSystemDetailBean> ssm = es.getSources1(src_val, src_sys);
		model.addAttribute("ssm", ssm);
		ArrayList<ConnectionMaster> conn_val = es.getConnections(src_val);
		model.addAttribute("conn_val", conn_val);
		ArrayList<String> tgt = es.getTargets();
		model.addAttribute("tgt", tgt);
//		ArrayList<String> buckets = DBUtils.getBuckets();
//		model.addAttribute("buckets", buckets);
		ArrayList<CountryMaster> countries = es.getCountries();
		model.addAttribute("countries", countries);
//		ArrayList<ReservoirMaster> reservoir = es.getReservoirs();
//		model.addAttribute("reservoir", reservoir);
		model.addAttribute("src_val", src_val);
		return new ModelAndView("extraction/SystemDetailsEdit");
	}

	@RequestMapping(value = "/extraction/DataHome", method = RequestMethod.GET)
	public ModelAndView DataHome() {
		return new ModelAndView("extraction/DataHome");
	}

	@RequestMapping(value = "/extraction/DataDetails", method = RequestMethod.POST)
	public ModelAndView DataDetails(@Valid @ModelAttribute("src_val") String src_val, ModelMap model) throws IOException {
		model.addAttribute("src_val", src_val);
		ArrayList<SourceSystemMaster> src_sys_val = es.getSources(src_val);
		model.addAttribute("src_sys_val", src_sys_val);
		return new ModelAndView("extraction/DataDetails"+src_val);
	}
	
	@RequestMapping(value = "/extraction/DataDetailsOracle0", method = RequestMethod.POST)
	public ModelAndView DataDetails0(@Valid @ModelAttribute("src_sys_id") int src_sys_id, @ModelAttribute("src_val") String src_val, ModelMap model)
			throws UnsupportedOperationException, Exception {
		ConnectionMaster conn_val = es.getConnections1(src_val,src_sys_id);
		model.addAttribute("conn_val", conn_val);
		ArrayList<String> schema_name = es.getSchema(src_val,conn_val.getConnection_id());
		model.addAttribute("schema_name", schema_name);
		return new ModelAndView("extraction/DataDetailsOracle0");
	}
	
	@RequestMapping(value = "/extraction/DataDetailsOracle1", method = RequestMethod.POST)
	public ModelAndView DataDetails1(@Valid @ModelAttribute("src_sys_id") int src_sys_id, @ModelAttribute("src_val") String src_val, @ModelAttribute("schema_name") String schema_name, ModelMap model)
			throws UnsupportedOperationException, Exception {
		ConnectionMaster conn_val = es.getConnections1(src_val,src_sys_id);
		model.addAttribute("conn_val", conn_val);
		String ext_type=es.getExtType(src_sys_id);
		model.addAttribute("ext_type", ext_type);
		ArrayList<String> tables = es.getTables(src_val, conn_val.getConnection_id(), schema_name);
		model.addAttribute("tables", tables);
		model.addAttribute("schema_name", schema_name);
		return new ModelAndView("extraction/DataDetailsOracle1");
	}
	
	@RequestMapping(value = "/extraction/DataDetailsOracle2", method = RequestMethod.POST)
	public ModelAndView DataDetails2(@Valid @ModelAttribute("id") String id, @ModelAttribute("src_val") String src_val, @ModelAttribute("table_name") String table_name,
			@ModelAttribute("connection_id") int connection_id, @ModelAttribute("schema_name") String schema_name, ModelMap model) throws UnsupportedOperationException, Exception {
		ArrayList<String> fields = es.getFields(id, src_val, table_name, connection_id,schema_name);
		model.addAttribute("fields", fields);
		model.addAttribute("id", id);
		return new ModelAndView("extraction/DataDetailsOracle2");
	}
	
	@RequestMapping(value = "/extraction/DataDetailsOracle3", method = RequestMethod.POST)
	public ModelAndView DataDetails3(@Valid @ModelAttribute("src_val") String src_val, @ModelAttribute("x") String x, ModelMap model) throws UnsupportedOperationException, Exception {
//		System.out.println(x);
		String resp = es.invokeRest(x, "addTableInfo");
		model.addAttribute("successString", resp.toString());
		model.addAttribute("src_val", src_val);
		ArrayList<SourceSystemMaster> src_sys_val = es.getSources(src_val);
		model.addAttribute("src_sys_val", src_sys_val);
		return new ModelAndView("extraction/DataDetailsOracle");
	}
	
	@RequestMapping(value = "/extraction/DataDetailsHadoop1", method = RequestMethod.POST)
	public ModelAndView DataDetailsHadoop(@Valid @ModelAttribute("src_val") String src_val, @ModelAttribute("x") String x, ModelMap model) throws UnsupportedOperationException, Exception {
//		System.out.println(x);
		String resp = es.invokeRest(x, "addHDFSInfo");
		model.addAttribute("successString", resp.toString());
		model.addAttribute("src_val", src_val);
		ArrayList<SourceSystemMaster> src_sys_val = es.getSources(src_val);
		model.addAttribute("src_sys_val", src_sys_val);
		return new ModelAndView("extraction/DataDetailsHadoop");
	}
	
	@RequestMapping(value = "/extraction/DataDetailsUnix1", method = RequestMethod.POST)
	public ModelAndView DataDetailsUnix1(@Valid @ModelAttribute("src_val") String src_val, @ModelAttribute("src_sys_id") int src_sys_id, ModelMap model) throws UnsupportedOperationException, Exception {
		ArrayList<DriveMaster> drive = es.getDrives1(src_sys_id);
		model.addAttribute("drive",drive);
		return new ModelAndView("extraction/DataDetailsUnix1");
	}

	@RequestMapping(value = "/extraction/DataDetailsUnix2", method = RequestMethod.POST)
	public ModelAndView DataDetailsUnix(@Valid @ModelAttribute("src_val") String src_val, @ModelAttribute("x") String x, @RequestParam("file") MultipartFile multiPartFile1, @RequestParam("field") MultipartFile multiPartFile2, ModelMap model) throws UnsupportedOperationException, Exception {
		model.addAttribute("src_val", src_val);
		ArrayList<SourceSystemMaster> src_sys_val = es.getSources(src_val);
		model.addAttribute("src_sys_val", src_sys_val);
		File file = convert(multiPartFile1);
		Workbook workbook = WorkbookFactory.create(file);
		Sheet sheet = workbook.getSheetAt(1);
		DataFormatter dataFormatter = new DataFormatter();
		Iterator<Row> rowIterator = sheet.rowIterator();
		ArrayList<String> col_names = new ArrayList<String>();
		String json1="\"file_details\":[";
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			// This is done to eliminate top row in excel with heading
			if (row.getRowNum() == 0 || row.getRowNum() == 1) {
				continue;
			}
			//If there are no data in columns then we will end process and proceed next.
			if(row.getPhysicalNumberOfCells()==0){
				break;
			}
			// Now let's iterate over the columns of the current row
			Iterator<Cell> cellIterator = row.cellIterator();
				String file_name = dataFormatter.formatCellValue(cellIterator.next());
				if(!file_name.equals("")) {json1+="{\"file_name\":\""+file_name+"\","; }else {break;}
				String file_type = dataFormatter.formatCellValue(cellIterator.next());
				if(!file_type.equals("")) {json1+="\"file_type\":\""+file_type+"\","; }else {break;}
				String file_delimiter = dataFormatter.formatCellValue(cellIterator.next());
				if(!file_delimiter.equals("")) {json1+="\"file_delimiter\":\""+file_delimiter+"\","; }else {break;}
				String header_count = dataFormatter.formatCellValue(cellIterator.next());
				if(!header_count.equals("")) {json1+="\"header_count\":\""+header_count+"\","; }else {break;}
				String trailer_count = dataFormatter.formatCellValue(cellIterator.next());
				if(!trailer_count.equals("")) {json1+="\"trailer_count\":\""+trailer_count+"\","; }else {break;}
				String avro_conv_flag = dataFormatter.formatCellValue(cellIterator.next());
				if(!avro_conv_flag.equals("")) {json1+="\"avro_conv_flag\":\""+avro_conv_flag+"\"},"; }else {break;}
		}
		json1=json1.substring(0,json1.length()-1)+"]";

		file = convert(multiPartFile2);
		workbook = WorkbookFactory.create(file);
		sheet = workbook.getSheetAt(1);
		dataFormatter = new DataFormatter();
		rowIterator = sheet.rowIterator();
		col_names = new ArrayList<String>();
		String json2="\"field_details\":[";
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			// This is done to eliminate top row in excel with heading
			if (row.getRowNum() == 0 || row.getRowNum() == 1) {
				continue;
			}
			//If there are no data in columns then we will end process and proceed next.
			if(row.getPhysicalNumberOfCells()==0){
				break;
			}
			// Now let's iterate over the columns of the current row
			Iterator<Cell> cellIterator = row.cellIterator();
				String file_id = dataFormatter.formatCellValue(cellIterator.next());
				if(!file_id.equals("")) {json2+="{\"file_id\":\""+file_id+"\","; }else {break;}
				String field_pos = dataFormatter.formatCellValue(cellIterator.next());
				if(!field_pos.equals("")) {json2+="\"field_pos\":\""+field_pos+"\","; }else {break;}
				String field_name = dataFormatter.formatCellValue(cellIterator.next());
				if(!field_name.equals("")) {json2+="\"field_name\":\""+field_name+"\","; }else {break;}
				String field_datatype = dataFormatter.formatCellValue(cellIterator.next());
				if(!field_datatype.equals("")) {json2+="\"field_datatype\":\""+field_datatype+"\"},"; }else {break;}
		}
		json2=json2.substring(0,json2.length()-1)+"]";
		String json = x + "," + json1 + "," + json2 + "}}}";
		System.out.println(json);
		String resp = es.invokeRest(json, "addFileInfo");
		model.addAttribute("successString", resp.toString());
		return new ModelAndView("extraction/DataDetailsUnix");
	}
	
	@RequestMapping(value = "/extraction/DataDetailsEditOracle", method = RequestMethod.POST)
	public ModelAndView DataDetailsEdit(@Valid @ModelAttribute("src_sys_id") int src_sys_id, @ModelAttribute("src_val") String src_val, ModelMap model)
			throws UnsupportedOperationException, Exception {
		ConnectionMaster conn_val = es.getConnections1(src_val,src_sys_id);
		model.addAttribute("conn_val", conn_val);
		String ext_type=es.getExtType(src_sys_id);
		model.addAttribute("ext_type", ext_type);
		String schema_name = es.getSchemaData(src_val,src_sys_id);
		ArrayList<String> tables = es.getTables(src_val, conn_val.getConnection_id(),schema_name);
		model.addAttribute("tables", tables);
		ArrayList<DataDetailBean> arrddb = es.getData(src_sys_id,src_val,schema_name);
		model.addAttribute("schem", schema_name);
		model.addAttribute("arrddb", arrddb);
		return new ModelAndView("extraction/DataDetailsEditOracle");
	}

	@RequestMapping(value = "/extraction/ExtractHome", method = RequestMethod.GET)
	public ModelAndView ExtractHome() {
		return new ModelAndView("extraction/ExtractHome");
	}

	@RequestMapping(value = "/extraction/ExtractData", method = RequestMethod.POST)
	public ModelAndView ExtractData(@Valid @ModelAttribute("src_val") String src_val, ModelMap model) throws IOException {
		model.addAttribute("src_val", src_val);
		ArrayList<SourceSystemMaster> src_sys_val = es.getSources(src_val);
		model.addAttribute("src_sys_val", src_sys_val);
		return new ModelAndView("extraction/ExtractData");
	}

	@RequestMapping(value = "/extraction/ExtractData1", method = RequestMethod.POST)
	public ModelAndView ExtractData1(@Valid @ModelAttribute("src_unique_name") String src_unique_name, @ModelAttribute("src_val") String src_val, ModelMap model)
			throws UnsupportedOperationException, Exception {
		String ext_type=es.getExtType1(src_unique_name);
		model.addAttribute("ext_type", ext_type);
		return new ModelAndView("extraction/ExtractData1");
	}

	@RequestMapping(value = "/extraction/ExtractData2", method = RequestMethod.POST)
	public ModelAndView ExtractData2(@Valid @ModelAttribute("src_val") String src_val, @ModelAttribute("x") String x, @ModelAttribute("ext_type") String ext_type, ModelMap model) throws UnsupportedOperationException, Exception {
		String resp=null;
//		System.out.println(x);
		if(ext_type.equalsIgnoreCase("Batch"))
		{
			resp = es.invokeRest(x, "createDag");
		}
		else
		{
			resp = es.invokeRest(x, "extractData");
		}
		model.addAttribute("successString", resp.toString());
		model.addAttribute("src_val", src_val);
		ArrayList<SourceSystemMaster> src_sys_val = es.getSources(src_val);
		model.addAttribute("src_sys_val", src_sys_val);
		return new ModelAndView("extraction/ExtractData");
	}
	
	public File convert(MultipartFile multiPartFile) throws Exception {
		File convFile = new File(multiPartFile.getOriginalFilename());
		convFile.createNewFile();
		FileOutputStream fos = new FileOutputStream(convFile);
		fos.write(multiPartFile.getBytes());
		fos.close();
		return convFile;
	}
}

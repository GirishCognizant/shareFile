package com.maven.servlet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

@WebServlet("/servlet/ImportSheet")
@MultipartConfig
public class SheetImportServlet extends HttpServlet {

	static final long serialVersionUID = 1L;
	static Map<String, XSSFRow> autoVinSheetToBeWritten = new HashMap();
	static Map<String, XSSFRow> nonAutoVinSheetToBeWritten = new HashMap();
	static Map<Integer, String> autoColMaps = new HashMap<Integer, String>();
	static final DecimalFormat df = new DecimalFormat("0.0000");
	static List<Integer> toIgnoreCols = new ArrayList<>();
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		PrintWriter pw = resp.getWriter();
		Boolean isException = false;
		try {
			Part fileName = req.getPart("file");
			System.out.println("FileName::"+fileName.getSubmittedFileName());
			autoVinSheetToBeWritten = new HashMap();
			nonAutoVinSheetToBeWritten = new HashMap();
  
			XSSFWorkbook workbook = new XSSFWorkbook(fileName.getInputStream()); 
            
            for(int a=0;a < workbook.getNumberOfSheets(); a++) {
            	
            	autoVinSheetToBeWritten.clear();
            	XSSFSheet autoVinSheet = workbook.getSheetAt(a);
	            System.out.println("autoVinSheet.getLastRowNum:::"+autoVinSheet.getLastRowNum());
	            
	            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	            //TODO: Change this hard coded configuration into configuration file and get the data from that configuration file.
	            Properties prop = new Properties();
	            try {
	            	InputStream fis = this.getClass().getClassLoader().getResourceAsStream("sqlserver.properties");
//	            	FileInputStream fis = new FileInputStream("WEB/sqlserver.properties");
	            	prop.load(fis);
	            } catch(Exception e) {
	            	e.printStackTrace();
	            	pw.write("error in config file");
	            	isException = true;
	            	break;
	            }
	            String serverName = prop.getProperty("ServerName");
	            String port = prop.getProperty("port");
	            String db = prop.getProperty("db");
	            System.out.println("serverName:::::"+serverName);
	            System.out.println("port:::::"+port);
	            System.out.println("db:::::"+db);
	            String dbURL = "jdbc:sqlserver://"+serverName+":"+port+";encrypt=true;trustServerCertificate=true;integratedSecurity=true;databaseName="+db;
	            
	            Connection connection = DriverManager.getConnection(dbURL);
	            toIgnoreCols = new ArrayList<>();
	            if(fileName.getSubmittedFileName().toLowerCase().contains("tol rule") 
	            		|| fileName.getSubmittedFileName().toLowerCase().contains("tol_rule") 
	            		|| fileName.getSubmittedFileName().toLowerCase().contains("decision")) {
	            	String columnsWithComma = "", tableName = "";
	            	if(fileName.getSubmittedFileName().toLowerCase().contains("decision")) {
	            		tableName = "decision_rule";
	            	} else {
	            		tableName = "tol_rule";
            		}
	            	columnsWithComma = insertColumnIfNotExist(connection, autoVinSheet, tableName);
			        
			        String[] splt = columnsWithComma.split("~`~~`~");
			        String insertQuery = "INSERT INTO "+tableName+" (" + splt[0] + ", sheet_name) VALUES (" 
			        		+ splt[1] + ", '"+autoVinSheet.getSheetName() + "')"; 
			        System.out.println(insertQuery);
		            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery); 
		            Map<String, String> colsTyp = colTypeDbMap(connection, "Tol_Rule");
			        
			        List<String> ls = new ArrayList<String>();
			        String selectQuery = "Select 1 from "+tableName+" where 1=1";
//			        System.out.println("last row: "+autoVinSheet.getLastRowNum());
			        if(autoVinSheet.getLastRowNum()>=20000) {
				        for(int i=1;autoVinSheet.getLastRowNum()/20000>=i; i++) {
				        	for(int j=((20000*(i-1))+1);j<20000*i;j++) {
				        		if(j==0) {
				        			continue;
				        		}
				        		XSSFRow row = autoVinSheet.getRow(j);
				        		if(row==null) {
				        			break;
				        		}
				        		
				        		String s = selectQueryParams(row, colsTyp);
				        		
				        		PreparedStatement preparedStatement1 = connection.prepareStatement(selectQuery+s);
					        	
					        	ResultSet rs = preparedStatement1.executeQuery();
					        	if(!rs.next()) {
					        		insertRow(row, preparedStatement, new ArrayList<>(), false, colsTyp, true);
					        	}
				        	}
				        	int[] res =preparedStatement.executeBatch(); 
				            System.out.println("inserted auto result: "+res.length);
				            preparedStatement.clearBatch();
				            System.gc();
				            Runtime.getRuntime().freeMemory();
				        }
			        } else {
			        	for(int j=1;j<20000;j++) {
			        		XSSFRow row = autoVinSheet.getRow(j);
			        		if(row==null) {
			        			break;
			        		}
			        		String s = selectQueryParams(row, colsTyp);
			        		
			        		PreparedStatement preparedStatement1 = connection.prepareStatement(selectQuery+s);
				        	
				        	ResultSet rs = preparedStatement1.executeQuery();
				        	if(!rs.next()) {
				        		insertRow(row, preparedStatement, new ArrayList<>(), false, colsTyp, true);
				        	}
			        	}
			        	int[] res =preparedStatement.executeBatch(); 
			            System.out.println("inserted auto result: "+res.length);
			            preparedStatement.clearBatch();
			            System.gc();
			            Runtime.getRuntime().freeMemory();
			        }
	            } else if(autoVinSheet.getSheetName().equalsIgnoreCase("Auto VIN") 
	            		|| autoVinSheet.getSheetName().equalsIgnoreCase("Non Auto VIN")) {
	            	
			        String columnsWithComma = insertColumnIfNotExist(connection, autoVinSheet, "VINDetails");
			        
			        PreparedStatement prepStmt = connection.prepareStatement("select VIN from VINDetails");
			        ResultSet rs = prepStmt.executeQuery();
			        List<String> dbList = new LinkedList();
			        while (rs.next()) {
			        	dbList.add(rs.getString("VIN"));
			        }
			        
			        String[] splt = columnsWithComma.split("~`~~`~");
			        String vinType = autoVinSheet.getSheetName().equalsIgnoreCase("Auto VIN")? "Auto Vin":"Vin";
			        String insertQuery = "INSERT INTO VINDetails (" + splt[0] + ", VIN_Type) VALUES (" + splt[1] + ", '"+vinType + "')"; 
			        System.out.println(insertQuery);
		            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery); 
		            Map<String, String> colsTyp = colTypeDbMap(connection, "VINDetails");
			        
			        List<String> ls = new ArrayList<String>();
//			        System.out.println("last row: "+autoVinSheet.getLastRowNum());
			        if(autoVinSheet.getLastRowNum()>=20000) {
				        for(int i=1;autoVinSheet.getLastRowNum()/20000>=i; i++) {
				        	for(int j=((20000*(i-1))+1);j<20000*i;j++) {
				        		if(j==0) {
				        			continue;
				        		}
				        		XSSFRow row = autoVinSheet.getRow(j);
				        		if(row==null) {
				        			break;
				        		}
				        		String vin = row.getCell(0)!=null ? row.getCell(0).getStringCellValue().replaceAll("(^\\h*)|(\\h*$)","") :"";
				            	if(vin==null || vin.isEmpty() || ls.contains(vin)) {
				            		continue;
				            	}
					        	insertRow(row, preparedStatement, dbList, true, colsTyp, true);
					        	ls.add(vin);
				        	}
				        	int[] res =preparedStatement.executeBatch(); 
				            System.out.println("inserted auto result: "+res.length);
				            preparedStatement.clearBatch();
				            System.gc();
				            Runtime.getRuntime().freeMemory();
				        }
			        } else {
			        	for(int j=1;j<20000;j++) {
			        		XSSFRow row = autoVinSheet.getRow(j);
			        		if(row==null) {
			        			break;
			        		}
			        		String vin = row.getCell(0)!=null ? row.getCell(0).getStringCellValue().replaceAll("(^\\h*)|(\\h*$)","") :"";
			            	if(vin==null || vin.isEmpty() || ls.contains(vin)) {
			            		continue;
			            	}
				        	insertRow(row, preparedStatement, dbList, true, colsTyp, true);
				        	ls.add(vin);
			        	}
			        	int[] res =preparedStatement.executeBatch(); 
			            System.out.println("inserted auto result: "+res.length);
			            preparedStatement.clearBatch();
			            System.gc();
			            Runtime.getRuntime().freeMemory();
			        }
	            } else {
	            	String table = autoVinSheet.getSheetName();
	            	table = table.replaceAll("-", " ");
	            	table = table.replaceAll("&", " and ");
	            	table = table.trim().replaceAll(" +", "_");
			        List<String> dbList = new LinkedList();
			        
			        String columnsWithComma = insertColumnIfNotExist(connection, autoVinSheet, table);
			        String[] splt = columnsWithComma.split("~`~~`~");
			        
			        String insertQuery = "INSERT INTO "+table+" (" + splt[0] + ") VALUES (" + splt[1] + ")"; 
			        System.out.println(insertQuery);
		            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery); 
		            Map<String, String> colsTyp = colTypeDbMap(connection, table);
		            
		            String selectQuery = "Select 1 from "+table+" where 1=1";
		            
		            if(autoVinSheet.getLastRowNum()>=20000) {
				        for(int i=1;autoVinSheet.getLastRowNum()/20000>=i; i++) {
				        	for(int j=((20000*(i-1)));j<20000*i;j++) {
				        		if(j==0) {
				        			continue;
				        		}
				        		XSSFRow row = autoVinSheet.getRow(j);
				        		if(row==null) {
				        			break;
				        		}
				        		String s = selectQueryParams(row, colsTyp);
				        		
				        		PreparedStatement preparedStatement1 = connection.prepareStatement(selectQuery+s);
					        	
					        	ResultSet rs = preparedStatement1.executeQuery();
					        	if(!rs.next()) {
					        		insertRow(row, preparedStatement, dbList, false, colsTyp, true);
					        	}
				        	}
				        	int[] res =preparedStatement.executeBatch(); 
				            System.out.println("inserted auto result: "+res.length);
				            preparedStatement.clearBatch();
				            System.gc();
				            Runtime.getRuntime().freeMemory();
				        }
			        } else {
			        	for(int j=1;j<20000;j++) {
			        		XSSFRow row = autoVinSheet.getRow(j);
			        		if(row==null) {
			        			break;
			        		}
			        		String s = selectQueryParams(row, colsTyp);
			        		
			        		PreparedStatement preparedStatement1 = connection.prepareStatement(selectQuery+s);
				        	
				        	ResultSet rs = preparedStatement1.executeQuery();
				        	if(!rs.next()) {
				        		insertRow(row, preparedStatement, dbList, false, colsTyp, true);
				        	}
			        	}
			        	int[] res =preparedStatement.executeBatch(); 
			            System.out.println("inserted auto result: "+res.length);
			            preparedStatement.clearBatch();
			            System.gc();
			            Runtime.getRuntime().freeMemory();
			        }
			                    	
	            }
	            
	            System.out.println("it is done\n");
	            connection.close();
	            
            }
            workbook.close();
        } 
        catch (Exception e) {
    		e.printStackTrace(pw);
            e.printStackTrace(); 
            isException = true;
        }
		if(!isException) {
			pw.write("SUCCESS");
		}
		
	}
	

	
	private static Map<String, String> colTypeDbMap(Connection connection, String table){
		Map<String, String> retMap = new HashMap<>();
		try {
			PreparedStatement pre = connection.prepareStatement("Select * from "+table);
			pre.setMaxRows(1);
			ResultSet rs = pre.executeQuery();
			ResultSetMetaData rsm = rs.getMetaData();
			for(int i=1; i<=rsm.getColumnCount(); i++) {
				String type = rsm.getColumnTypeName(i);
				String col = rsm.getColumnName(i);
				retMap.put(col, type);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retMap;
	}
	
	private static String insertColumnIfNotExist(Connection connection, XSSFSheet autoVinSheet1, String table) {
		String str = "",retStr="";
		try {
			XSSFRow autoVinHeaderRow = autoVinSheet1.getRow(0);
			
			PreparedStatement prepStmt = connection.prepareStatement
        			("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"+table+"'");
	        ResultSet rs2 = prepStmt.executeQuery();
	        if(!rs2.next()) {
	        	if(table.equalsIgnoreCase("decision_rule") || table.equalsIgnoreCase("tol_rule")) {
		        	PreparedStatement prepStmt3 = connection
		        			.prepareStatement("CREATE TABLE "+table+" (id int NOT NULL IDENTITY(1,1), sheet_name varchar(255) NOT NULL)");
			        prepStmt3.execute();
	        	} else if(table.equalsIgnoreCase("VINDetails")) {
		        	PreparedStatement prepStmt3 = connection
		        			.prepareStatement("CREATE TABLE "+table+" (id int NOT NULL IDENTITY(1,1), VIN varchar(20) NOT NULL UNIQUE, VIN_Type varchar(20) NOT NULL)");
			        prepStmt3.execute();
	        	} else {
	        		PreparedStatement prepStmt3 = connection
		        			.prepareStatement("CREATE TABLE "+table+" (id int NOT NULL IDENTITY(1,1))");
			        prepStmt3.execute();
	        	}
	        }
			
			autoColMaps.clear();
			int i=0;
	        for(int j=0;j<autoVinHeaderRow.getLastCellNum();j++) {
	        	String col = autoVinHeaderRow.getCell(i).getStringCellValue().trim();
	        	if(col.contains("&")) {
	        		col = col.replaceAll("&", " and ");
	        	}
	        	if(col.contains("%")) {
	        		col = col.replaceAll("%", "Percentage");
	        	}
	        	if(col.contains("/")) {
	        		col = col.replaceAll("/", " or ");
	        	}
	        	col = col.replaceAll("-", " ");
	        	col = col.trim().replaceAll(" +", "_");
	        	if(autoColMaps.containsValue(col)) {
	        		toIgnoreCols.add(j+1);
	        		continue;
	        	}
	        	if(table.equalsIgnoreCase("tol_rule") && col.equalsIgnoreCase("rule")) {
	        		col = "Description";
	        	}
	        	autoColMaps.put(i+1, col);
	        	str = str + col + ", ";
	        	retStr = retStr + "?, ";
	        	PreparedStatement prepStmt1 = connection.prepareStatement
	        			("SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME = '"+table+"'"
    	        			+ " AND COLUMN_NAME = '"+col+"'");
		        ResultSet rs1 = prepStmt1.executeQuery();
		        if(!rs1.next()) {
		        	if(autoVinSheet1.getRow(1).getCell(i)==null) {
		        		PreparedStatement prepStmt3 = connection
			        			.prepareStatement("ALTER TABLE "+table+" ADD "+col+" varchar(1023)");
				        prepStmt3.execute();
				        continue;
					}
		        	
		        	switch (autoVinSheet1.getRow(1).getCell(i).getCellType()) {
			            case Cell.CELL_TYPE_NUMERIC: 
			            	if(col.equalsIgnoreCase("override_level")) {
			            		PreparedStatement prepStmt2 = connection
					        			.prepareStatement("ALTER TABLE "+table+" ADD "+col+" varchar(1023)");
							        prepStmt2.execute();
			            	} else {
			            		PreparedStatement prepStmt2 = connection
					        			.prepareStatement("ALTER TABLE "+table+" ADD "+col+" decimal(10,4)");
							        prepStmt2.execute();
			            	}
			            	
					        
			                break; 
			
			            default: 
		            		PreparedStatement prepStmt3 = connection
				        			.prepareStatement("ALTER TABLE "+table+" ADD "+col+" varchar(1023)");
					        prepStmt3.execute();
		            }
		        }
		        i++;
	        }
	        if(table.equalsIgnoreCase("VINDetails")) {
		        PreparedStatement prepStmt1 = connection.prepareStatement
	        			("SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME = '"+table+"'"
		        			+ " AND COLUMN_NAME = 'VIN_Type'");
		        ResultSet rs1 = prepStmt1.executeQuery();
		        if(!rs1.next()) {
	        		PreparedStatement prepStmt3 = connection
		        			.prepareStatement("ALTER TABLE "+table+" ADD VIN_Type varchar(20)");
			        prepStmt3.execute();
		        }
	        }
		} catch (Exception e) {
			System.out.println("Exception in insertColumnIfNotExist");
			e.printStackTrace();
		}
		return str.substring(0, str.length()-2) + "~`~~`~" + retStr.substring(0,retStr.length()-2);
	}
	
	@SuppressWarnings({ "null", "deprecation" })
	private static String selectQueryParams(XSSFRow oldRow, Map<String, String> colsTyp) {
		String s1 = "";
		int i = 1;
		for(int j=0;j<autoColMaps.keySet().size();j++) {
	        try {
	        	if(toIgnoreCols !=null && !toIgnoreCols.isEmpty() && toIgnoreCols.contains(j+1)) {
	        		continue;
	        	}
				Cell oldCell = oldRow.getCell(i-1);
				try {
					if(oldCell==null || oldCell.getStringCellValue()==null || oldCell.getStringCellValue().isEmpty()) {
						s1= s1 +" and "+autoColMaps.get(i)+" is Null";
						continue;
					} else {
		            	String s = oldCell.getStringCellValue().replaceAll("(^\\h*)|(\\h*$)","").trim();
		            	boolean doTry = false;
		            	try {
		            		int l = Integer.parseInt(s);
		            		float f = Float.parseFloat(df.format(s));
		            		if("decimal".equalsIgnoreCase(colsTyp.get(autoColMaps.get(i)))) {
		            			s1= s1 +" and "+autoColMaps.get(i)+" = "+f;
		            			continue;
		            		} else {
		            			if(s.contains("\'")) {
		            				s = s.replaceAll("'", "\"");
		            			}
		            			s1= s1 +" and "+autoColMaps.get(i)+" = '"+s+"'";
		            			continue;
		            		}
		            	} catch (Exception e) {
		            		doTry = true;
		            	}
		            	if(doTry) {
			            	if(oldCell.getStringCellValue() == null 
				            	|| oldCell.getStringCellValue().isEmpty()) {
			            		s1= s1 +" and "+autoColMaps.get(i)+" is Null";
			            	}  else if(oldCell.getStringCellValue().equalsIgnoreCase("NA") 
			            			|| oldCell.getStringCellValue().equalsIgnoreCase("N/A")) {
			            		if("decimal".equalsIgnoreCase(colsTyp.get(autoColMaps.get(i)))) {
				            		s1= s1 +" and "+autoColMaps.get(i)+" is Null";
			            		} else {
				            		s1= s1 +" and "+autoColMaps.get(i)+" = '"+s+"'";
			            		}
			            	} else {
			            		if(s.contains("\'")) {
		            				s = s.replaceAll("'", "\"");
		            			}
			            		s1= s1 +" and "+autoColMaps.get(i)+" = '"+s+"'";
			            	}
		            	}
					}
				} catch (Exception e) {
					if(oldCell == null) {
	            		s1= s1 +" and "+autoColMaps.get(i)+" is Null";
	            	} else {
	            		float f = Float.parseFloat(df.format(oldCell.getNumericCellValue()));
	            		s1= s1 +" and "+autoColMaps.get(i)+" = "+f;
	            	}
				}
				i++;
	        } catch (Exception e) {
	        	System.out.println("Exception while copying data from the sheet");
	        	e.printStackTrace();
	        	break;
	        }
		}
		
		return s1;
	}
	int h=0; 
	
	@SuppressWarnings({ "null", "deprecation" })
	private static void insertRow(XSSFRow oldRow, PreparedStatement preparedStatement
			, List<String> dbList, boolean isVin, Map<String, String> colsTyp, boolean isBatch) {
		boolean isBreak = false, isAnyValue=false;
		String s1 = "";
		int i=0;
        for(int j=0;j<autoColMaps.keySet().size();j++) {
			if(i==0 && isVin && oldRow.getCell(i)!=null && dbList.contains(oldRow.getCell(i).getStringCellValue().length() > 17 
					? oldRow.getCell(i).getStringCellValue().replaceAll("(^\\h*)|(\\h*$)","").trim() : oldRow.getCell(i).getStringCellValue())) {
				isBreak = true;
				break;
			}
	        try {
	        	
	        	if(toIgnoreCols !=null && !toIgnoreCols.isEmpty() && toIgnoreCols.contains(j+1)) {
	        		continue;
	        	}
				Cell oldCell = oldRow.getCell(i);
				try {
					if(oldCell==null || oldCell.getStringCellValue()==null || oldCell.getStringCellValue().isEmpty()) {
						if(isVin && i==0) {
							break;
						}
						s1= s1 +(i+1)+"+Null_";
						preparedStatement.setNull(i+1, Cell.CELL_TYPE_STRING);
						i++;
						continue;
					} else {
		            	String s = oldCell.getStringCellValue().replaceAll("(^\\h*)|(\\h*$)","").trim();
		            	boolean doTry = false;
		            	try {
		            		Float l = Float.parseFloat(s);
		            		
		            		if("decimal".equalsIgnoreCase(colsTyp.get(autoColMaps.get(i)))) {
		            			preparedStatement.setFloat(i+1, Float.parseFloat(df.format(l)));
		            			isAnyValue=true;
		            			s1= s1 +(i+1)+"+"+Float.parseFloat(df.format(l));
		            			i++;
		            			continue;
		            		} else {
		            			preparedStatement.setString(i+1, s);
		            			isAnyValue=true;
		            			s1= s1 +(i+1)+"+"+s;
		            			i++;
		            			continue;
		            		}
		            	} catch (Exception e) {
		            		doTry = true;
		            	}
		            	if(doTry) {
			            	if(oldCell.getStringCellValue() == null 
				            	|| oldCell.getStringCellValue().isEmpty()) {
			            		preparedStatement.setNull(i+1, Cell.CELL_TYPE_STRING);
			            		s1= s1 +(i+1)+"+Null_";
			            	} else if(oldCell.getStringCellValue().equalsIgnoreCase("NA") 
			            			|| oldCell.getStringCellValue().equalsIgnoreCase("N/A")) {
			            		if("decimal".equalsIgnoreCase(colsTyp.get(autoColMaps.get(i)))) {
			            			preparedStatement.setNull(i+1, Cell.CELL_TYPE_NUMERIC);
				            		s1= s1 +(i+1)+"+Null_";
			            		} else {
			            			preparedStatement.setString(i+1, s);
				            		isAnyValue=true;
				            		s1= s1 +(i+1)+"+"+s+"_";
			            		}
			            	} else {	
			            		preparedStatement.setString(i+1, s);
			            		isAnyValue=true;
			            		s1= s1 +(i+1)+"+"+s+"_";
			            	}
		            	}
					}
				} catch (Exception e) {
					if(oldCell == null) {
	            		preparedStatement.setNull(i+1, Cell.CELL_TYPE_NUMERIC);
	            		s1= s1 +(i+1)+"+Null_";
	            	} else {
	            		float f = Float.parseFloat(df.format(oldCell.getNumericCellValue()));
	            		preparedStatement.setFloat(i+1, f);
	            		isAnyValue=true;
	            		s1= s1 +(i+1)+"+"+ f+"_";
	            	}
				}
				i++;
	        } catch (Exception e) {
	        	System.out.println("Exception while copying data from the sheet");
	        	e.printStackTrace();
	        	isBreak = true;
	        	break;
	        }
		}
		
		if(!isBreak && isBatch && isAnyValue) {
			try {
				preparedStatement.addBatch();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println(s1);
	}
	
}

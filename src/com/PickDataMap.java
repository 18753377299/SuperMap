package com;

import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.EngineType;
import com.supermap.data.QueryParameter;
import com.supermap.data.Recordset;
import com.supermap.data.SpatialQueryMode;
import com.supermap.data.Workspace;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年3月13日 上午9:24:31
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class PickDataMap {
	// 导出地图excel数据(正式数据)
	public static void main(String[]args){
//		AjaxResult ajaxResult = new AjaxResult();
		Workspace workspace = new Workspace();
	     // 定义数据源连接信息，假设以下所有数据源设置都存在
        DatasourceConnectionInfo datasourceconnection = new  DatasourceConnectionInfo();
		 datasourceconnection.setEngineType(EngineType.ORACLEPLUS);
        datasourceconnection.setServer("10.10.68.248:1521/orcl");
//        datasourceconnection.setDatabase("riskcontrol");
//        datasourceconnection.setUser("riskcontrol"); // riskcontrol_freeze
//        datasourceconnection.setPassword("riskcontrol");
        
        datasourceconnection.setDatabase("riskcontrol_freeze");
        datasourceconnection.setUser("riskcontrol_freeze"); // riskcontrol_freeze
        datasourceconnection.setPassword("riskcontrol_freeze");
        datasourceconnection.setAlias("ORACLE");
        
        // 打开数据源
        Datasource datasource = workspace.getDatasources().open(datasourceconnection);
        // 获取的点数据集
        DatasetVector datasetVector = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS");
        System.out.println("点数据集数量"+datasetVector.getRecordCount() );
       
//      DatasetVector dataset2 = (DatasetVector)datasource.getDatasets().get("RISKMAP_DISASTER2");
        
        if (datasource == null) {
            System.out.println("打开数据源失败");
	    } else {
	        System.out.println("数据源打开成功！");
	    }
        
//        double average = dataset.statistic("POINTX_2000",StatisticMode.AVERAGE);
//		System.out.println("POINTX_2000的平均值为：" + average);
//        average = 131;
//        
//		QueryParameter queryParameter = new QueryParameter();
//		queryParameter.setAttributeFilter("POINTX_2000 >"+average);
//		queryParameter.setHasGeometry(true);
		
//		Toolkit toolkit =new Toolkit();
        // 获取的面数据集
        DatasetVector datasetVectorArea = (DatasetVector)datasource.getDatasets().get("guangdong_country");
        System.out.println("面数据集数量"+datasetVectorArea.getRecordCount());
//		Geometry geome= Toolkit.GeoJsonToGemetry(geometry);
//		GeoRegion geoRegion  =(GeoRegion)geome;
//		
		//设置查询参数
		QueryParameter parameter = new QueryParameter();
		parameter.setSpatialQueryObject(datasetVectorArea);
		parameter.setSpatialQueryMode(SpatialQueryMode.CONTAIN);
//		parameter.setAttributeFilter("POINTX_2000 >0");
		parameter.setAttributeFilter("VALIDSTATUS=1");
		
		// ,"POINTY_2000 desc"
//		parameter.setOrderBy(new String[] {"SmID asc"});
//		parameter.setCursorType(CursorType.DYNAMIC);
				
		Recordset queryRecordset = datasetVector.query(parameter);
		
		System.out.println("记录数:"+queryRecordset.getRecordCount());
		DatasetVector datasetVector_1 = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS_1");
		//将Recordset追加到RISKMAP_ADDRESS_1数据集中
		boolean flag = datasetVector_1.append(queryRecordset);
		
		if(flag){
			System.out.println("追加到矢量数据集success!");
		}
		
//		FieldInfos  fieldInfos = queryRecordset.getFieldInfos();
//		FieldInfo  fieldInfo = fieldInfos.get(0);
//		String  name = fieldInfo.getName();
//		String  value = fieldInfo.getDefaultValue();
//		
//		Map<Integer,Feature>  features= queryRecordset.getAllFeatures();
//		for(Feature feature:features.values()){
//			String valueString = feature.getString("POINTX_2000");
//			System.out.println("POINTX_2000:"+valueString);
//		}
		
//		HSSFWorkbook wkb = new HSSFWorkbook();
//		HSSFSheet sheet = this.createExcel(wkb);
//		
//		HSSFWorkbook wkb2 = new HSSFWorkbook();
//		HSSFSheet sheet2 = this.createExcel(wkb2);
//		
//		Map<Integer,Feature>  features= queryRecordset.getAllFeatures();
		System.out.println(queryRecordset.getRecordCount());
		
//		int i=0;
//		boolean flagI = true;
//		for(Integer key:features.keySet()){	
//			if(i<65530&&flagI){
//				HSSFRow row=sheet.createRow(i + 3);
//				System.out.println(features.get(key).getString("SMID")+":"+
//						features.get(key).getString("POINTX_2000"));
//				row.createCell(0).setCellValue(features.get(key).getString("SMID"));
//				row.createCell(1).setCellValue(features.get(key).getString("POINTX_2000"));
//				row.createCell(2).setCellValue(features.get(key).getString("POINTY_2000"));   
//				row.createCell(3).setCellValue(features.get(key).getString("ADDRESSNAME"));
//				row.createCell(4).setCellValue(features.get(key).getString("SCORE"));
//				row.createCell(5).setCellValue(features.get(key).getString("RAINSCALE"));
//				i++;
//			}else {
//				i=0;
//				flagI = false;
//				HSSFRow row=sheet2.createRow(i + 3);
//				System.out.println(features.get(key).getString("SMID")+":"+
//						features.get(key).getString("POINTX_2000"));
//				row.createCell(0).setCellValue(features.get(key).getString("SMID"));
//				row.createCell(1).setCellValue(features.get(key).getString("POINTX_2000"));
//				row.createCell(2).setCellValue(features.get(key).getString("POINTY_2000"));   
//				row.createCell(3).setCellValue(features.get(key).getString("ADDRESSNAME"));
//				row.createCell(4).setCellValue(features.get(key).getString("SCORE"));
//				row.createCell(5).setCellValue(features.get(key).getString("RAINSCALE"));
//				i++;
//			}
//		}		
		// 释放工作空间资源
        datasourceconnection.dispose();
        workspace.dispose();

//		ResourceBundle bundle = ResourceBundle.getBundle("config.savePath",
//				Locale.getDefault());
//		String dir = bundle.getString("saveRootPath")+bundle.getString("saveTypePath");
//		StringBuffer stringBuf = new StringBuffer();
//		stringBuf.append(dir).append("/").append("downloadExcel");
//		File dirPath = new File(stringBuf.toString());
//		if(!dirPath.exists()) {
//			dirPath.mkdirs();
//		}
//		File outFile = new File(stringBuf.append("/").append("mapData00").append(".xls").toString());
//		File outFile1 = new File("F:/riskcontrol/riskcontrol_file/downloadExcel/mapData01.xls");
//		//输出Excel文件
//		try {
//			FileOutputStream output=new FileOutputStream(outFile);
//			FileOutputStream output1=new FileOutputStream(outFile1);
//			ajaxResult.setData(stringBuf.toString());
//			wkb.write(output);
//			wkb2.write(output1);
//			output.flush();
//			output1.flush();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
//		OutputStream output =null;
//		FTPUtil ftp = new FTPUtil();
//		// 存储到公共上传目录下
//		//输出Excel文件
//		try {
//			ResourceBundle bundle = ResourceBundle.getBundle("config.savePath",
//					Locale.getDefault());
//
//    		output =ftp.uploadFile("downloadExcel/mapData.xls");
//			ajaxResult.setData("/downloadExcel/mapData.xls");
//			wkb.write(output);
//			output.flush();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}finally {
//            if (output != null) {
//                try {
//                	output.close();
//                } catch (Exception e2) {
//                	LOGGER.error(e2.getMessage(), e2);
//                }
//                
//            }
//            if(ftp!=null) {
//	            try {
//					ftp.close();
//				} catch (IOException e) {
//					LOGGER.info("关闭ftp异常：" + e.getMessage() ,e);
//				}
//            }
//		}
	}
	
				
				
				
			
//			public  HSSFSheet  createExcel(HSSFWorkbook wkb){
//				//建立新的sheet对象（excel的表单）
//				HSSFSheet sheet=wkb.createSheet("统计表");
//				//在sheet里创建第一行，参数为行索引(excel的行)，可以是0～65535之间的任何一个
//				HSSFRow row1=sheet.createRow(0);
//				//创建单元格（excel的单元格，参数为列索引，可以是0～255之间的任何一个
//				HSSFCell cell=row1.createCell(0);
//				//设置居中
//				HSSFCellStyle style = wkb.createCellStyle();
//			    style.setAlignment(HSSFCellStyle.ALIGN_CENTER);
//			    // 设置字体
//		        HSSFFont font = wkb.createFont();
//		        //设置字体大小
//		        font.setFontHeightInPoints((short)11);
//		        //字体加粗
//		        font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
//		        //在样式用应用设置的字体;  
//		         style.setFont(font);
//				//设置单元格内容
//				cell.setCellValue("地图统计表");
//				cell.setCellStyle(style);
//				//创建单元格并设置单元格内容
//				//合并单元格CellRangeAddress构造参数依次表示起始行，截至行，起始列， 截至列
//				sheet.addMergedRegion(new CellRangeAddress(0,1,0,5));
//				//在sheet里创建第二行
//				HSSFRow row2=sheet.createRow(2);
//				
//				HSSFCell cell0=row2.createCell(0);
//				cell0.setCellValue("地图ID");
//				sheet.setColumnWidth(cell0.getColumnIndex(), 256 * 20);
//				
//				HSSFCell cell1=row2.createCell(1);
//				cell1.setCellValue("POINTX_2000");
//				sheet.setColumnWidth(cell1.getColumnIndex(), 256 * 20);
//				
//				HSSFCell cell2=row2.createCell(2);
//				cell2.setCellValue("POINTX_2000");
//				sheet.setColumnWidth(cell2.getColumnIndex(), 256 * 20);
//				
//				HSSFCell cell3=row2.createCell(3);
//				cell3.setCellValue("ADDRESSNAME");
//				sheet.setColumnWidth(cell3.getColumnIndex(), 256 * 60);
//				
//				HSSFCell cell4=row2.createCell(4);
//				row2.createCell(4).setCellValue("SCORE");
//				sheet.setColumnWidth(cell4.getColumnIndex(), 256 * 20);
//				
//				HSSFCell cell5=row2.createCell(5);
//				row2.createCell(5).setCellValue("RAINSCALE");
//				sheet.setColumnWidth(cell5.getColumnIndex(), 256 * 20);
//				
//				return sheet;
//				
//			}
}

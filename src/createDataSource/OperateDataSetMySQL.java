package createDataSource;

import com.supermap.data.CursorType;
import com.supermap.data.DatasetType;
import com.supermap.data.DatasetVector;
import com.supermap.data.DatasetVectorInfo;
import com.supermap.data.Datasets;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.EngineType;
import com.supermap.data.FieldInfo;
import com.supermap.data.FieldInfos;
import com.supermap.data.FieldType;
import com.supermap.data.GeoCircle;
import com.supermap.data.GeoRegion;
import com.supermap.data.Point2D;
import com.supermap.data.PrjCoordSys;
import com.supermap.data.PrjCoordSysType;
import com.supermap.data.Recordset;
import com.supermap.data.Workspace;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月7日 上午9:42:20
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class OperateDataSetMySQL {
	/*静态变量*/
	
//	private static final  String  iobjectJavaServer = "10.10.68.248:1521/orcl";
//	private static final  String  iobjectJavaDatabase = "riskcontrol";
//	private static final  String  iobjectJavaUser = "riskcontrol";
//	private static final  String  iobjectJavaPassword = "riskcontrol";
//	private static final  String  riskMap_address  = "SMDTV_60";
	
	private static final  String  iobjectJavaServer = "localhost:3306/shop";
	private static final  String  iobjectJavaDatabase = "shop";
	private static final  String  iobjectJavaUser = "root";
	private static final  String  iobjectJavaPassword = "qk941009";
//	private static final  String  riskMap_address  = "SMDTV_60";
	
	public static void main(String[]args){
		/*创建圆形 的面数据集*/
		createRegionDataSet();
		/*读取圆形 的面数据集*/
//		getRegionDataSet();
	}
	
	/*读取圆形 的面数据集*/
	public static void getRegionDataSet(){
		Workspace workspace = new Workspace();
		// 定义数据源连接信息，假设以下所有数据源设置都存在
      DatasourceConnectionInfo datasourceconnection = new  DatasourceConnectionInfo();
//	   datasourceconnection.setEngineType(EngineType.ORACLEPLUS);
	   datasourceconnection.setEngineType(EngineType.MYSQL);
      datasourceconnection.setServer(iobjectJavaServer);
      datasourceconnection.setDatabase(iobjectJavaDatabase);
      datasourceconnection.setUser(iobjectJavaUser); // riskcontrol_freeze
      datasourceconnection.setPassword(iobjectJavaPassword);
//      datasourceconnection.setAlias("ORACLE");
      datasourceconnection.setAlias("MYSQL");
      // 打开数据源
      Datasource datasource = workspace.getDatasources().open(datasourceconnection);
      
      // 获取的点数据集
//      DatasetVector datasetVector = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS");
      
      if (datasource == null) {
          System.out.println("打开数据源失败");
      } else {
         System.out.println("数据源打开成功！");
      }
      
   // 获取的点数据集(给数据集添加字段时，使用Recordset不能成功，使用DatasetVector可以成功)
      DatasetVector datasetVector = (DatasetVector)datasource.getDatasets().get("circleName1");
//      Recordset recordset = datasetVector.getRecordset(false, CursorType.DYNAMIC);
	  
	  // 实例化一个字段信息对象，对其进行设置
      FieldInfo fieldInfoNew = new FieldInfo();
      fieldInfoNew.setName("Pop_2009");
      fieldInfoNew.setCaption("Pop_2009");
      fieldInfoNew.setDefaultValue("0");
      fieldInfoNew.setType(FieldType.DOUBLE);
      fieldInfoNew.setRequired(true);
      
      FieldInfos  fieldInfosOld = datasetVector.getFieldInfos();
      fieldInfosOld.add(fieldInfoNew);
      
      FieldInfos  fieldInfos = datasetVector.getFieldInfos();
      for(int i =0;i<fieldInfos.getCount();i++){
		  FieldInfo  fieldInfo = fieldInfos.get(i);
		  String  name = fieldInfo.getName();
		  String  value = fieldInfo.getDefaultValue();
		  System.out.println("字段值："+i+":"+name+":"+value);
	  }
	    
//	    if(fieldInfoNew!=null){
//	    	fieldInfoNew.dispose();
//		}
	    
		if(datasetVector!=null){
			datasetVector.close();
		}
	    if(datasource!=null){
			datasource.close();
		}
	    if(datasourceconnection!=null){
			datasourceconnection.dispose();
		}
		if(workspace!=null){
			// 关闭工作空间
			workspace.close();
			// 释放该对象所占用的资源
			workspace.dispose();
		}
		
	}
	/*创建圆形 的面数据集*/
	public static void createRegionDataSet(){
		Workspace workspace = new Workspace();
		// 定义数据源连接信息，假设以下所有数据源设置都存在
		/*String  iobjectJavaServer = "10.10.68.248:1521/orcl";
		String  iobjectJavaDatabase = "riskcontrol";
		String  iobjectJavaUser = "riskcontrol";
		String  iobjectJavaPassword = "riskcontrol";
		String  riskMap_address  = "SMDTV_60";*/
		
	     // 定义数据源连接信息，假设以下所有数据源设置都存在
      DatasourceConnectionInfo datasourceconnection = new  DatasourceConnectionInfo();
//	   datasourceconnection.setEngineType(EngineType.ORACLEPLUS);
	   datasourceconnection.setEngineType(EngineType.MYSQL);
      datasourceconnection.setServer(iobjectJavaServer);
      datasourceconnection.setDatabase(iobjectJavaDatabase);
      datasourceconnection.setUser(iobjectJavaUser); // riskcontrol_freeze
      datasourceconnection.setPassword(iobjectJavaPassword);
//      datasourceconnection.setAlias("ORACLE");
      datasourceconnection.setAlias("MYSQL");
      // 打开数据源
      Datasource datasource = workspace.getDatasources().open(datasourceconnection);
      
      // 获取的点数据集
//      DatasetVector datasetVector = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS");
      
      if (datasource == null) {
          System.out.println("打开数据源失败");
      } else {
         System.out.println("数据源打开成功！");
      }
      
      // 创建数据集
      Datasets datasets = datasource.getDatasets();
      DatasetVectorInfo datasetVectorInfo = new DatasetVectorInfo();
       //面数据集类型
      datasetVectorInfo.setType(DatasetType.REGION);
	  datasetVectorInfo.setName("circleName5");
	  
	  DatasetVector datasetVector = datasets.create(datasetVectorInfo);
	  
	  PrjCoordSys prjCoordSys = new PrjCoordSys();
	  //地理经纬坐标
	  prjCoordSys.setType(PrjCoordSysType.PCS_EARTH_LONGITUDE_LATITUDE );
//	  GeoCoordSys geoCoordSys = new GeoCoordSys(GeoCoordSysType.GCS_CHINA_2000,GeoSpatialRefType.SPATIALREF_EARTH_LONGITUDE_LATITUDE );
//	  prjCoordSys.setCoordUnit(Unit.KILOMETER); // 千米
	  
	  datasetVector.setPrjCoordSys(prjCoordSys);
	  
	  Recordset recordset = datasetVector.getRecordset(false, CursorType.DYNAMIC);
	  
	  recordset.edit();
	  
	  FieldInfos  fieldInfosold = datasetVector.getFieldInfos();
	 
	  // 实例化一个字段信息对象，对其进行设置
      FieldInfo fieldInfoNew = new FieldInfo();
      fieldInfoNew.setName("Pop_2009");
      fieldInfoNew.setCaption("Pop_2009");
      fieldInfoNew.setDefaultValue("0");
      fieldInfoNew.setType(FieldType.DOUBLE);
      fieldInfoNew.setRequired(true);
      fieldInfosold.add(fieldInfoNew);
      
      
      FieldInfos  fieldInfos = datasetVector.getFieldInfos();
      for(int i =0;i<fieldInfos.getCount();i++){
		  FieldInfo  fieldInfo = fieldInfos.get(i);
		  String  name = fieldInfo.getName();
		  String  value = fieldInfo.getDefaultValue();
		  System.out.println("字段值："+i+":"+name+":"+value);
	  }
      
      
//	  	/*初始化半径*/
//		double radius = 22.22;
//		/*设置中心点坐标*/
//		Point2D  point2D =new Point2D(116, 39);
//		
//		GeoCircle geoCircle =new GeoCircle();
//		geoCircle.setRadius(radius);
//		geoCircle.setCenter(point2D);
//		
//		/*将圆几何对象转换为面几何对象。*/
//		GeoRegion geoRegion = geoCircle.convertToRegion(50);
//		
//		/*初始化半径*/
//		double radius1 = 11;
//		/*设置中心点坐标*/
//		Point2D  point2D1 =new Point2D(109, 36);
//		
//		GeoCircle geoCircle1 =new GeoCircle();
//		geoCircle1.setRadius(radius1);
//		geoCircle1.setCenter(point2D1);
//		
//		/*将圆几何对象转换为面几何对象。*/
//		GeoRegion geoRegion1 = geoCircle1.convertToRegion(50);
//	  
//		recordset.addNew(geoRegion);
//		// 没有这个数据集更新不能够成功
//		recordset.update();
//		recordset.addNew(geoRegion1);
//		recordset.update();
      
      /**添加多个面数据集的方式*/
       double  [][]dataList = {{22,116,36},{10,110,22}};
       Recordset recordsetNew = addMoreRecordset(recordset ,dataList);
       
       if(recordsetNew!=null){
    	   recordsetNew.close();
    	   recordsetNew.dispose();
		}
	    if(fieldInfoNew!=null){
	    	fieldInfoNew.dispose();
		}
		if(datasetVector!=null){
			datasetVector.close();
		}
	    if(datasource!=null){
			datasource.close();
		}
	    if(datasourceconnection!=null){
			datasourceconnection.dispose();
		}
		if(workspace!=null){
			// 关闭工作空间
			workspace.close();
			// 释放该对象所占用的资源
			workspace.dispose();
		}
		
	}
	
	/**添加多个面数据集的方式*/
	public static Recordset addMoreRecordset(Recordset recordset ,double  [][]dataList){
		if(dataList.length>0){
			for(int i=0;i<dataList.length;i++){
				recordset.edit();
				/*初始化半径*/
				double radius = dataList[i][0];
				/*设置中心点坐标*/
				Point2D  point2D =new Point2D(dataList[i][1], dataList[i][2]);
				
				GeoCircle geoCircle =new GeoCircle();
				geoCircle.setRadius(radius);
				geoCircle.setCenter(point2D);
				
				/*将圆几何对象转换为面几何对象。*/
				GeoRegion geoRegion = geoCircle.convertToRegion(50);
				recordset.update();
				recordset.addNew(geoRegion);
				// 没有这个数据集更新不能够成功
				recordset.update();
				
				if(geoRegion!=null){
					geoRegion.clone();
					geoRegion.dispose();
				}
				if(point2D!=null){
					point2D.clone();
				}
			}
		}
		return  recordset;
	}

}

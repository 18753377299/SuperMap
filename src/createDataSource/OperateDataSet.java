package createDataSource;

import com.supermap.data.CursorType;
import com.supermap.data.DatasetType;
import com.supermap.data.DatasetVector;
import com.supermap.data.DatasetVectorInfo;
import com.supermap.data.Datasets;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.EngineType;
import com.supermap.data.GeoCircle;
import com.supermap.data.GeoCoordSys;
import com.supermap.data.GeoCoordSysType;
import com.supermap.data.GeoRegion;
import com.supermap.data.GeoSpatialRefType;
import com.supermap.data.Point2D;
import com.supermap.data.PrjCoordSys;
import com.supermap.data.PrjCoordSysType;
import com.supermap.data.Recordset;
import com.supermap.data.Unit;
import com.supermap.data.Workspace;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月7日 上午9:42:20
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class OperateDataSet {
/*静态变量*/
	
	private static final  String  iobjectJavaServer = "10.10.68.248:1521/orcl";
	private static final  String  iobjectJavaDatabase = "riskcontrol";
	private static final  String  iobjectJavaUser = "riskcontrol";
	private static final  String  iobjectJavaPassword = "riskcontrol";
	private static final  String  riskMap_address  = "SMDTV_60";
	
	public static void main(String[]args){
		createRegionDataSet();
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
	   datasourceconnection.setEngineType(EngineType.ORACLEPLUS);
      datasourceconnection.setServer(iobjectJavaServer);
      datasourceconnection.setDatabase(iobjectJavaDatabase);
      datasourceconnection.setUser(iobjectJavaUser); // riskcontrol_freeze
      datasourceconnection.setPassword(iobjectJavaPassword);
      datasourceconnection.setAlias("ORACLE");
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
	  datasetVectorInfo.setName("circleName1");
	  
	  DatasetVector datasetVector = datasets.create(datasetVectorInfo);
	  
	  PrjCoordSys prjCoordSys = new PrjCoordSys();
	  //地理经纬坐标
	  prjCoordSys.setType(PrjCoordSysType.PCS_EARTH_LONGITUDE_LATITUDE );
//	  GeoCoordSys geoCoordSys = new GeoCoordSys(GeoCoordSysType.GCS_CHINA_2000,GeoSpatialRefType.SPATIALREF_EARTH_LONGITUDE_LATITUDE );
//	  prjCoordSys.setCoordUnit(Unit.KILOMETER); // 千米
	  
	  datasetVector.setPrjCoordSys(prjCoordSys);
	  
	  Recordset recordset = datasetVector.getRecordset(false, CursorType.DYNAMIC);
	  
	  	/*初始化半径*/
		double radius = 22.22;
		/*设置中心点坐标*/
		Point2D  point2D =new Point2D(116, 39);
		
		GeoCircle geoCircle =new GeoCircle();
		geoCircle.setRadius(radius);
		geoCircle.setCenter(point2D);
		
		/*将圆几何对象转换为面几何对象。*/
		GeoRegion geoRegion = geoCircle.convertToRegion(50);
	  
	    recordset.addNew(geoRegion);
		
	    
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
	

}

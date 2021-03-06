package createDataSource;

import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.EngineType;
import com.supermap.data.GeoCircle;
import com.supermap.data.Point2D;
import com.supermap.data.Workspace;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月6日 下午6:54:31
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class OperateDataSource {
	/*静态变量*/
	
	private static final  String  iobjectJavaServer = "10.10.68.248:1521/orcl";
	private static final  String  iobjectJavaDatabase = "riskcontrol";
	private static final  String  iobjectJavaUser = "riskcontrol";
//	private static final  String  iobjectJavaPassword = "riskcontrol";
	private static final  String  iobjectJavaPassword = "7269736b636f6e74726f6c";;
	private static final  String  riskMap_address  = "SMDTV_60";
	
	public static void main(String []args){
		 /*打开数据源*/
		openDataSource();
		
		/*创建数据源*/
//		createDataSource();
		
		/*创建面数据集*/
//		createDataSet();
		
	}
	/*创建面数据集*/
	public static void createDataSet(){
		/*初始化半径*/
		double radius = 22.22;
		/*设置中心点坐标*/
		Point2D  point2D =new Point2D(116, 39);
		
		GeoCircle geoCircle =new GeoCircle();
		geoCircle.setRadius(radius);
		geoCircle.setCenter(point2D);
		
		/*将圆几何对象转换为面几何对象。*/
		geoCircle.convertToRegion(50);
		
	}
	
	/*创建数据源*/
	public static void createDataSource(){
		Workspace workspace = new Workspace();
		
		DatasourceConnectionInfo info = new DatasourceConnectionInfo();
		info.setEngineType(EngineType.ORACLEPLUS);
		info.setServer(iobjectJavaServer);
		info.setDatabase(iobjectJavaDatabase);
		info.setUser(iobjectJavaUser);
		info.setPassword(iobjectJavaPassword);
		
		Datasource ds = workspace.getDatasources().create(info);
		//创建成功,输出数据源相关信息
		if (ds != null) {
			System.out.println("数据源创建成功!");
		} else {
			System.out.println("数据源创建失败!");
		}
		//关闭所有打开的数据源
		workspace.getDatasources().closeAll();
	}
	
	/*打开数据源*/
	public static void openDataSource(){
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
//      datasourceconnection.setPassword(iobjectJavaPassword);
      datasourceconnection.setPassword(iobjectJavaPassword);
      datasourceconnection.setAlias("ORACLE");
      // 打开数据源
      Datasource datasource = workspace.getDatasources().open(datasourceconnection);
      // 获取的点数据集
      DatasetVector datasetVector = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS");
      
      if (datasource == null) {
          System.out.println("打开数据源失败");
      } else {
         System.out.println("数据源打开成功！");
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
}

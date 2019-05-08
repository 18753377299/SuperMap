package common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import vo.WzTFLslj;

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
import com.supermap.data.Geometry;
import com.supermap.data.Point2D;
import com.supermap.data.PrjCoordSys;
import com.supermap.data.PrjCoordSysType;
import com.supermap.data.Recordset;
import com.supermap.data.Workspace;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月8日 下午5:31:18
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */

public class MapUtils {
	
	/**连接数据源*/
	public static Datasource connectDataSource(Workspace workspace,DatasourceConnectionInfo datasourceconnection,
			String iobjectJavaServer,String iobjectJavaDatabase,String iobjectJavaUser,String iobjectJavaPassword){
		
		   datasourceconnection.setEngineType(EngineType.ORACLEPLUS);
	       datasourceconnection.setServer(iobjectJavaServer);
	       datasourceconnection.setDatabase(iobjectJavaDatabase);
	       datasourceconnection.setUser(iobjectJavaUser); // riskcontrol_freeze
	       datasourceconnection.setPassword(iobjectJavaPassword);
		   datasourceconnection.setAlias("ORACLE");
	      // 打开数据源
	      Datasource datasource = workspace.getDatasources().open(datasourceconnection);
	      
	      // 获取的点数据集
//		  DatasetVector datasetVector = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS");
	      
	      if (datasource == null) {
	          System.out.println("打开数据源失败");
	          return null;
	      } else {
	         System.out.println("数据源打开成功！");
	         return datasource;
	      }
	}
	// 创建面数据集
	public static DatasetVector createDataSet(String dataSetName,Datasource datasource){
		  
		  Datasets datasets = datasource.getDatasets();
	      DatasetVectorInfo datasetVectorInfo = new DatasetVectorInfo();
	      //面数据集类型
	      datasetVectorInfo.setType(DatasetType.REGION);
	      datasetVectorInfo.setName(dataSetName);
		  
		  DatasetVector datasetVector = datasets.create(datasetVectorInfo);
		  
		  PrjCoordSys prjCoordSys = new PrjCoordSys();
		  //地理经纬坐标
		  prjCoordSys.setType(PrjCoordSysType.PCS_EARTH_LONGITUDE_LATITUDE );
//			  GeoCoordSys geoCoordSys = new GeoCoordSys(GeoCoordSysType.GCS_CHINA_2000,GeoSpatialRefType.SPATIALREF_EARTH_LONGITUDE_LATITUDE );
//			  prjCoordSys.setCoordUnit(Unit.KILOMETER); // 千米
		  datasetVector.setPrjCoordSys(prjCoordSys);
		  
//		  Recordset recordset = datasetVector.getRecordset(false, CursorType.DYNAMIC);
//		  recordset.edit();
		  
		  FieldInfos  fieldInfosold = datasetVector.getFieldInfos();
		  
		  // 实例化一个字段信息对象，对其进行设置
	      FieldInfo fieldInfoNew = new FieldInfo();
	      fieldInfoNew.setName("Pop_2009");
	      fieldInfoNew.setCaption("Pop_2009");
//	      fieldInfoNew.setDefaultValue("0");
	      fieldInfoNew.setType(FieldType.DOUBLE);
//	      fieldInfoNew.setRequired(true);
	      fieldInfosold.add(fieldInfoNew);
		
	      return datasetVector;
	}
	// 给面数据添加字段信息
	public static void addFieldInfo(){
		
//		return null;
	}
	
	/*给面数据集增加数据*/
	public static Recordset addDataToDatasetVector (DatasetVector datasetVector,List<WzTFLslj>  wzTFLsljsList){
//		BatchEditor editor = recordset.getBatch();
//		// 设置批量更新每次提交的记录数目
//        editor.setMaxRecordCount(50);
//        // 从 World 数据集中读取几何对象和字段值，批量更新到 example 数据集中
//        editor.begin();
		Recordset recordset = datasetVector.getRecordset(false, CursorType.DYNAMIC);
		
		if(dataList.length>0){
			for(int i=0;i<dataList.length;i++){
				Map<String , Object> map =new HashMap<String, Object>();
				
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
				Geometry geometry = (Geometry)geoRegion;
				
				recordset.update();
				recordset.addNew(geometry,map);
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
		 // 批量操作统一提交
//        editor.update();

		return  recordset;
	}
	
	
}

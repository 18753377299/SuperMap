package area;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.EngineType;
import com.supermap.data.Feature;
import com.supermap.data.GeoRegion;
import com.supermap.data.Geometry;
import com.supermap.data.QueryParameter;
import com.supermap.data.Recordset;
import com.supermap.data.SpatialQueryMode;
import com.supermap.data.Toolkit;
import com.supermap.data.Workspace;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年4月2日 下午6:29:19
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class AreaCrossArea {
	public static void main(String []args){
		Workspace workspace = new Workspace();
		
		 // 定义数据源连接信息，假设以下所有数据源设置都存在
		String  iobjectJavaServer = "10.10.68.248:1521/orcl";
		String  iobjectJavaDatabase = "riskcontrol_freeze";
		String  iobjectJavaUser = "riskcontrol_freeze";
		String  iobjectJavaPassword = "riskcontrol_freeze";
		String  riskMap_address  = "SMDTV_2";
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
       DatasetVector datasetVector = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS");
       if (datasource == null) {
           System.out.println("打开数据源失败");
	    } else {
	        System.out.println("数据源打开成功！");
	    }
       //前台往后台传递的面数据
//		Geometry geome= Toolkit.GeoJsonToGemetry(riskMapMainVo.getGeometry());
		String geometryString  = ""; // 这是一个字符串类型的面数据
		Geometry geome= Toolkit.GeoJsonToGemetry(geometryString);
		GeoRegion geoRegion  =(GeoRegion)geome;
		//设置查询参数
		QueryParameter parameter = new QueryParameter();
		parameter.setSpatialQueryObject(geoRegion);
		parameter.setSpatialQueryMode(SpatialQueryMode.CONTAIN);
//		parameter.setAttributeFilter("POINTX_2000 >110");
		// ,"POINTY_2000 desc"
//		parameter.setOrderBy(new String[] {"SmID asc"});
//		parameter.setCursorType(CursorType.DYNAMIC);
				
		Recordset queryRecordset = datasetVector.query(parameter);
		Map<Integer,Feature>  features= queryRecordset.getAllFeatures();
		System.out.println(queryRecordset.getRecordCount());
		
		List<Integer> smidList =new ArrayList<Integer>(1000);
		for(Integer key:features.keySet()){	
			smidList.add(Integer.parseInt(features.get(key).getString("SMID")));
		}
		
       if(parameter!=null){
       	parameter.dispose();
		}
       if(geoRegion!=null){
       	geoRegion.dispose();
		}
       if(geome!=null){
       	geome.dispose();
		}
		if(queryRecordset!=null){
			queryRecordset.close();
			queryRecordset.dispose();
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

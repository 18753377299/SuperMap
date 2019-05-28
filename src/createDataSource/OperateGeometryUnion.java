package createDataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.supermap.data.CursorType;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.Feature;
import com.supermap.data.Geometrist;
import com.supermap.data.Geometry;
import com.supermap.data.Recordset;
import com.supermap.data.Workspace;
import common.MapUtils;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月13日 下午2:29:03
 * @version 1.0 
 * @parameter 
 * @since   这是对多个风圈数据进行合并成一个面数据
 * @return  */
public class OperateGeometryUnion {
	
	/*静态变量*/
	private static final  String  iobjectJavaServer = "10.10.68.248:1521/orcl";
	private static final  String  iobjectJavaDatabase = "riskcontrol";
	private static final  String  iobjectJavaUser = "riskcontrol";
	private static final  String  iobjectJavaPassword = "riskcontrol";
	private static final  String  riskMap_address  = "SMDTV_60";
	
	public static void main(String []args){
		startGeometryUnion();
	}
	
	public static String startGeometryUnion(){
		Workspace workspace = new Workspace();
		// 定义数据源连接信息，假设以下所有数据源设置都存在
	    DatasourceConnectionInfo datasourceconnection = new  DatasourceConnectionInfo();
		//进行数据源的连接
		Datasource datasource =MapUtils.connectDataSource(workspace,datasourceconnection);
		// 获取的面数据集
//		TF_7M
	    DatasetVector datasetVector_7 = (DatasetVector)datasource.getDatasets().get("TF_7M");
	    
	    DatasetVector datasetVector_10 = (DatasetVector)datasource.getDatasets().get("TF_10M");
	    // 新建面数据集
	    DatasetVector datasetVectorUnion_7 =  MapUtils.createDataSet("UnionTF_7_10",datasource);
	    
	    Recordset recordsetUnion = datasetVectorUnion_7.getRecordset(false, CursorType.DYNAMIC);
	    
	    String filter = "TFBH = 201822";
	    Recordset recordset_7 = datasetVector_7.query(filter,CursorType.DYNAMIC );
	    Recordset recordset_10 = datasetVector_10.query(filter,CursorType.DYNAMIC ); 
	    
	    Map<Integer,Feature>  features= recordset_7.getAllFeatures();
	    Map<Integer,Feature>  features_10= recordset_10.getAllFeatures();
	    
	    features.putAll(features_10);
	 // 锁定当前记录位置
	    recordsetUnion.edit();
	    recordsetUnion.update();
        
	    List<Geometry> geoList=new ArrayList<Geometry>();
	    if(recordset_7.getRecordCount()>0){
	    	for(Feature feature:features.values()){
	    		Geometry geometry= feature.getGeometry(); 
	    		geoList.add(geometry);
			}
	    	
	    	Geometry geometry = geoList.get(0);
	    	for (int j = 1;j<geoList.size();j++) {
    			Geometry geome= geoList.get(j);
    			geometry = Geometrist.union(geometry, geome);
    		}
	    	recordsetUnion.addNew(geometry);
	    	recordsetUnion.update();
	    	
	    	if(geometry!=null){
	    		geometry.dispose();
	    	}
	    	
	    }else {
	    	System.out.println("==============没有数据");
	    }
	    
	    if(recordsetUnion!=null){
	    	recordsetUnion.close();
	    	recordsetUnion.dispose();
		}
	    
        if(recordset_7!=null){
        	recordset_7.close();
        	recordset_7.dispose();
		}
//	    if(fieldInfoNew!=null){
//	    	fieldInfoNew.dispose();
//		}
		if(datasetVector_7!=null){
			datasetVector_7.close();
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
		
		return null;
	}
}

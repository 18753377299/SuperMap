package dissolveDatasetVector;

import com.supermap.analyst.spatialanalyst.OverlayAnalyst;
import com.supermap.analyst.spatialanalyst.OverlayAnalystParameter;
import com.supermap.data.DatasetType;
import com.supermap.data.DatasetVector;
import com.supermap.data.DatasetVectorInfo;
import com.supermap.data.Datasets;
import com.supermap.data.Datasource;
import com.supermap.data.EncodeType;
import com.supermap.data.EngineType;
import com.supermap.data.Workspace;
import com.supermap.data.WorkspaceConnectionInfo;
import com.supermap.data.WorkspaceType;
/**
 * 将两个面数据集合并成第三个面数据集
 * **/
public class DatasetVectorUnion {
	
	public static void main(String [] args) {
		Workspace workspace = new Workspace();
		WorkspaceConnectionInfo workspaceConnectionInfo = new WorkspaceConnectionInfo();
		Datasource datasource = new Datasource(EngineType.UDB);
		workspaceConnectionInfo.setType(WorkspaceType.SMWU);

//		String  file = "F:/A_supermap/superMap_file/Dissovle/dissolveDatasetVector/data/dissovle.smwu";
//		String  file = "F:/A_supermap/superMap_file/Dissovle/data/dissovle.smwu";
		String  file = "/home/supermapData/test/data/dissovle.smwu";
//		String file =filePath.getString("filePath");
		workspaceConnectionInfo.setServer(file);
		workspace.open(workspaceConnectionInfo); 
		datasource = workspace.getDatasources().get("dissovle"); 
		
		if (datasource == null) {
            System.out.println("打开数据源失败");
	    } else {
	        System.out.println("数据源打开成功！");
	    }
		
		Datasets datasets = datasource.getDatasets();
		DatasetVector dtv=(DatasetVector) datasets.get("DatasetVector");
		DatasetVector dtv1=(DatasetVector) datasets.get("DatasetVector1");
		System.out.println(dtv.getName());
		System.out.println(dtv1.getName());
		String name = datasets.getAvailableDatasetName("河流");
//		// 设置矢量数据集的信息
        DatasetVectorInfo datasetVectorInfo = new DatasetVectorInfo();
        datasetVectorInfo.setName(name);
        datasetVectorInfo.setType(DatasetType.REGION );
        datasetVectorInfo.setEncodeType(EncodeType.NONE);
        datasetVectorInfo.setFileCache(true);
        
//        System.out.println("矢量数据集的信息为：" + datasetVectorInfo.toString());
//
//        // 创建矢量数据集
        DatasetVector datasetVector = datasets.create(datasetVectorInfo);
        System.out.println(datasetVector.getName());
//        Recordset recordset = datasetVector.getRecordset(false, CursorType.DYNAMIC);
        // 设置叠加分析参数
        OverlayAnalystParameter overlayAnalystParamUnion = new OverlayAnalystParameter();
//        overlayAnalystParamUnion.setOperationRetainedFields(new String[] {
//                "Name"});
//        overlayAnalystParamUnion.setSourceRetainedFields(new String[] {
//                "ClassID"});
        overlayAnalystParamUnion.setTolerance(0.0000011074);
        // 调用合并叠加分析方法实合并分析
        OverlayAnalyst.union(dtv,dtv1,datasetVector,overlayAnalystParamUnion);
		
        datasetVector.close();
        
        if(datasource!=null){
			datasource.close();
		}
	    if(workspaceConnectionInfo!=null){
	    	workspaceConnectionInfo.dispose();
		}
		if(workspace!=null){
			// 关闭工作空间
			workspace.close();
			// 释放该对象所占用的资源
			workspace.dispose();
		}
	}
}

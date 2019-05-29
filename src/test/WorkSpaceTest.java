package test;

import java.util.ArrayList;
import java.util.List;

import vo.GridValue;

import com.supermap.data.Workspace;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月23日 上午11:23:18
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class WorkSpaceTest {
	public  static void main(String  []args){
		Workspace workspace = new Workspace();
		System.out.println("====success===");
		List<GridValue> list =new ArrayList<GridValue>();
		List<GridValue> list2 =new ArrayList<GridValue>();
		for(int i=0;i<2;i++){
			GridValue gridValue =new GridValue();
			gridValue.setRow(1);
			list.add(gridValue);
			if(i>0){
				gridValue.setRow(33);
				list2.add(gridValue);
			}
		}
		System.out.println("======end========");
	}
}

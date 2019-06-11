package com;

import com.supermap.data.Workspace;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年1月16日 下午5:49:30
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */

public class JavaTest {
	public static void main(String[] args) {
		System.out.println("before-success11!");
		
		long start = System.currentTimeMillis();
		new Workspace();
		long end = System.currentTimeMillis();
		System.out.println("success-workspace!=" + (end - start));
	}
}

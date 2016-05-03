package cn.xsx.hdfs.rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import cn.xsx.hdfs.rpc.LoginServiceInterface;

public class LoginController {
	public static void main(String[] args) throws Exception {
		LoginServiceInterface proxy = RPC.getProxy(LoginServiceInterface.class, 1L, new InetSocketAddress("hadoop",10000),new Configuration());
		String result = proxy.login("xsx", "1234");
		System.out.println(result);
	}
}

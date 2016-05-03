package cn.xsx.hdfs.rpc;

public interface LoginServiceInterface {
	public static final long versionID=1L;
	public String login(String username,String password);
}

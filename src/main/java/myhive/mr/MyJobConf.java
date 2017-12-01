package myhive.mr;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 公共配置
 */
public class MyJobConf {

    private String jobname;
    private Class<?> jarByClass;
    private Class<? extends Mapper> mapper;
    private Class<? extends Reducer> reducer;

	public MyJobConf(String jobname, Class<?> jarByClass) {
		this.jobname = jobname;
		this.jarByClass = jarByClass;
	}

	public MyJobConf(String jobname, Class<?> jarByClass, Class<? extends Reducer> reducer, Class<? extends Mapper> mapper) {
		this.jobname = jobname;
		this.reducer = reducer;
		this.mapper = mapper;
		this.jarByClass = jarByClass;
	}

	public String getJobname() {
		return jobname;
	}

	public void setJobname(String jobname) {
		this.jobname = jobname;
	}

	public Class<?> getJarByClass() {
		return jarByClass;
	}

	public void setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
	}

	public Class<? extends Mapper> getMapper() {
		return mapper;
	}

	public void setMapper(Class<? extends Mapper> mapper) {
		this.mapper = mapper;
	}

	public Class<? extends Reducer> getReducer() {
		return reducer;
	}

	public void setReducer(Class<? extends Reducer> reducer) {
		this.reducer = reducer;
	}
}

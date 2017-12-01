package myhive.mr;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class AbstractMRJob extends Configured implements Tool {

	protected static Logger logger = LoggerFactory.getLogger(AbstractMRJob.class);

	/**
	 * 将零散的统计总的run
	 * @param args
	 * @param
	 * @return
	 * @throws Exception
	 */
	@Override
	public int run(String[] args) throws Exception{
		Job job = this.getJob(args);
		logger.info("\n>>>>>>>>>>>>>>>>>>>>执行任务{}，开始。。。。。。。",job.getJobName());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		int status = job.waitForCompletion(true) ? 0 : 1;
		stopWatch.stop();
		logger.info("\n>>>>>>>>>>>>>执行任务{}执行所花时间(毫秒):{}",job.getJobName(),stopWatch.getTime());
		return status;
	}

	/**
	 * 获取job
	 * @param args
	 * @param myJobConf
	 * @return
	 * @throws IOException
	 */
	protected Job getJob(String[] args, MyJobConf myJobConf) throws IOException {

		Configuration configuration = getConf();
		if(null == configuration){
			configuration = new Configuration();
		}
		GenericOptionsParser parser = new GenericOptionsParser(configuration, args);
		args = parser.getRemainingArgs();
		String jobName = this.getJobTaskName(myJobConf.getJobname(),configuration);
		Job job = Job.getInstance(configuration,jobName);
		job.setJarByClass(myJobConf.getJarByClass());
		job.setMapperClass(myJobConf.getMapper());
		job.setReducerClass(myJobConf.getReducer());

		this.handlePath(args,job);
		Path outputpath = FileOutputFormat.getOutputPath(job);
		FileSystem fileSystem = FileSystem.get(configuration);
		if(fileSystem.exists(outputpath)){
			fileSystem.delete(outputpath,true);
		}
		return job;
	}

	protected abstract Job getJob(String args[]) throws Exception;
	/**
	 * 处理参数路径
	 * @param args
	 * @param job
	 */
	protected abstract void handlePath(String[] args,Job job) throws IOException;

	/**
	 * 获取任务name
	 * @return
	 */
	protected  String getJobTaskName(String jobName,Configuration configuration){
		String frameWorkName = configuration.get(MRConfig.FRAMEWORK_NAME,MRConfig.LOCAL_FRAMEWORK_NAME);
		StringBuilder builder = new StringBuilder(jobName);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		String dateStr = simpleDateFormat.format(new Date(System.currentTimeMillis()));
		builder.append("_").append(frameWorkName).append("_").append(dateStr);
		return builder.toString();
	}
}

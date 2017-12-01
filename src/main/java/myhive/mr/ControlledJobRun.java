package myhive.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ControlledJobRun {

	public static void main(String[] args) throws Exception {
		OrcToAvroJob avroJob = new OrcToAvroJob();
		Job avro = avroJob.getJob(args);

		Path path = FileOutputFormat.getOutputPath(avro);
		System.out.println(path.getName());
		JobControl jobControl = new JobControl(avro.getJobName());
		ControlledJob controlledJob = new ControlledJob(avro.getConfiguration());
		jobControl.addJob(controlledJob);
		//任务执行结果监控
		JobControlMonitor.listenJobControl(jobControl);
		jobControl.run();
	}
}

package myhive.mr;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;
import java.io.InputStream;

public class OrcToAvroJob extends AbstractMRJob {

	@Override
	public Job getJob(String[] args) throws Exception {

		MyJobConf myJobConf = new MyJobConf("myOrcToAvroJob",OrcToAvroJob.class,Reducer.class,OrcToAvroMapper.class);

		Job job = super.getJob(args, myJobConf);

		job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);

		job.setInputFormatClass(OrcInputFormat.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setNumReduceTasks(0);
		AvroJob.setOutputKeySchema(job,new Schema.Parser().parse(avro_schema));
		return job;
	}

	/**
	 * 处理参数路径
	 *
	 * @param args
	 * @param job
	 */
	@Override
	protected void handlePath(String[] args, Job job) throws IOException {

		Configuration configuration = job.getConfiguration();
		String defaultInpuPath = "localbase.db/user_install_status_orc";
        String defaultOutPath = "avro";
		String inputPath = configuration.get("file.input.path",defaultInpuPath);
		String outPath = configuration.get("file.out.path",defaultOutPath);

		FileInputFormat.setInputPaths(job,inputPath);
		Path out = new Path(outPath);
		FileOutputFormat.setOutputPath(job,out);
	}

	private static class OrcToAvroMapper extends Mapper<NullWritable, OrcStruct,AvroKey<GenericData.Record>,NullWritable> {

		Schema reduceSchema = new Schema.Parser().parse(avro_schema);

		@Override
		protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {

			GenericData.Record record = new GenericData.Record(reduceSchema);
			record.put("id",(Text)value.getFieldValue(0));
			record.put("pkname",(Text)value.getFieldValue(1));
			//TODO 这里的输出在多字段的时候需要修改
			context.write(new AvroKey(record), NullWritable.get());
			//context.write(aid,pkname);
		}
	}

	private static String  avro_schema = null;
	static {
		InputStream resourceAsStream = OrcToAvroJob.class.getResourceAsStream("/avro.avsc");
		try {
			avro_schema = IOUtils.toString(resourceAsStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

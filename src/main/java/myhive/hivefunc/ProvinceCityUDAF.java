package myhive.hivefunc;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

/**
 * 聚合函数
 */
public class ProvinceCityUDAF extends AbstractGenericUDAFResolver {

	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
		System.out.println("getEvaluator.......GenericUDAFParameterInfo="+info.getParameterObjectInspectors().toString());
		return super.getEvaluator(info);
	}

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
		System.out.println("getEvaluator.............typeinfo="+info[0].getTypeName().toString());
		return new ProvinceCityEvaluator();
	}

	private static class ProvinceCityAggBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {

		private StringBuilder value;

		public ProvinceCityAggBuffer(){
			value = new StringBuilder();
		}

		@Override
		public int estimate() {
			return 2* JavaDataModel.PRIMITIVES2;
		}
		public StringBuilder getValue() {
			return value;
		}

		public void setValue(StringBuilder value) {
			this.value = value;
		}

		public void append(String str){
			if(StringUtils.isNotEmpty(str)){
				value.append(",").append(str);
			}
		}
	}

	private static class ProvinceCityEvaluator extends GenericUDAFEvaluator{

		/**
		 * Initialize the evaluator.
		 *
		 * @param m          The mode of aggregation.
		 * @param parameters The ObjectInspector for the parameters: In PARTIAL1 and COMPLETE
		 *                   mode, the parameters are original data; In PARTIAL2 and FINAL
		 *                   mode, the parameters are just partial aggregations (in that case,
		 *                   the array will always have a single element).
		 * @return The ObjectInspector for the return value. In PARTIAL1 and PARTIAL2
		 * mode, the ObjectInspector for the return value of
		 * terminatePartial() call; In FINAL and COMPLETE mode, the
		 * ObjectInspector for the return value of terminate() call.
		 * <p>
		 * NOTE: We need ObjectInspector[] (in addition to the TypeInfo[] in
		 * GenericUDAFResolver) for 2 reasons: 1. ObjectInspector contains
		 * more information than TypeInfo; and GenericUDAFEvaluator.init at
		 * execution time. 2. We call GenericUDAFResolver.getEvaluator at
		 * compilation time,
		 */
		/**
		 * 初始化evaluator   在
		 * @param m
		 * @param parameters
		 * @return
		 * @throws HiveException
		 */
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			System.out.println("init.......mode=="+m+parameters[0].getTypeName());
			//直接返回hive基本类型中的String类型
			super.init(Mode.COMPLETE,parameters);
			return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		}

		/**
		 * Get a new aggregation object.
		 */
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			System.out.println("getNewAggregationBuffer");
			return new ProvinceCityAggBuffer();
		}

		/**
		 * Reset the aggregation. This is useful if we want to reuse the same
		 * aggregation.
		 *
		 * @param agg
		 */
		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			ProvinceCityAggBuffer provinceCityAggBuffer = (ProvinceCityAggBuffer)agg;
			provinceCityAggBuffer.setValue(new StringBuilder());
			System.out.println("reset.......");
		}

		/**
		 * Iterate through original data.
		 *
		 * @param agg
		 * @param parameters
		 */
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			ProvinceCityAggBuffer provinceCityAggBuffer = (ProvinceCityAggBuffer)agg;
			Object parameter = parameters[0];
			provinceCityAggBuffer.append(parameter.toString());
			System.out.println("iterate...."+parameter.toString());
		}

		/**
		 * Get partial aggregation result.
		 *
		 * @param agg
		 * @return partial aggregation result.
		 */
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			ProvinceCityAggBuffer provinceCityAggBuffer = (ProvinceCityAggBuffer)agg;
			System.out.println("terminatePartial....");
			return new Text(provinceCityAggBuffer.getValue().toString());
		}

		/**
		 * Merge with partial aggregation result. NOTE: null might be passed in case
		 * there is no input data.
		 *
		 * @param agg
		 * @param partial
		 */
		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			ProvinceCityAggBuffer provinceCityAggBuffer = (ProvinceCityAggBuffer)agg;
			provinceCityAggBuffer.append(partial.toString());
			System.out.println("merge....,partial="+partial.toString());
		}

		/**
		 * Get final aggregation result.
		 *
		 * @param agg
		 * @return final aggregation result.
		 */
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			ProvinceCityAggBuffer provinceCityAggBuffer = (ProvinceCityAggBuffer)agg;
			Text text = new Text();
			text.set(provinceCityAggBuffer.getValue().toString());
			System.out.println("terminate..........");
			return text;
		}
	}
}

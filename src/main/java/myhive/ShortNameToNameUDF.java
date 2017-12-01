package myhive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * 将省份拼音简称转化为省份汉子名称
 */
public class ShortNameToNameUDF extends GenericUDF {

	private PrimitiveObjectInspector inspector;

	private  static  Map<String,String> provinceMap = new HashMap<>();

	/**
	 * Initialize this GenericUDF. This will be called once and only once per
	 * GenericUDF instance.
	 *
	 * @param arguments The ObjectInspector for the arguments
	 * @return The ObjectInspector for the return value
	 * @throws UDFArgumentException Thrown when arguments have wrong types, wrong length, etc.
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		ObjectInspector objectInspector = arguments[0];
		ObjectInspector.Category category = objectInspector.getCategory();
		if(category.equals(ObjectInspector.Category.PRIMITIVE)){
		}
		return inspector = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}


	Text text = new Text();

	/**
	 * Evaluate the GenericUDF with the arguments.
	 *
	 * @param arguments The arguments as DeferedObject, use DeferedObject.get() to get the
	 *                  actual argument Object. The Objects can be inspected by the
	 *                  ObjectInspectors passed in the initialize call.
	 * @return The
	 */
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		DeferredObject deferredObject = arguments[0];
		Object o = deferredObject.get();
		//PrimitiveObjectInspectorConverter.StringConverter stringConverter = new PrimitiveObjectInspectorConverter.StringConverter(inspector);
		String hanzi = provinceMap.get(o.toString());
		text.set(hanzi);
		return text;
	}

	/**
	 * Get the String to be displayed in explain.
	 *
	 * @param children
	 */
	@Override
	public String getDisplayString(String[] children) {
		return "please use this udf like 'short_to_name(column)'";
	}

	static {
		try {
			InputStream inputStream = ShortNameToNameUDF.class.getResourceAsStream("/province.txt");
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
			String str = "";
			while(StringUtils.isNotEmpty(str = bufferedReader.readLine())){
				String[] arr = str.split(" ");
				provinceMap.put(arr[1],arr[0]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {

		}

	}
}

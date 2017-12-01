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
	 * 输入数据类型检查
	 *
	 * @param arguments The ObjectInspector for the arguments
	 * @return The ObjectInspector for the return value
	 * @throws UDFArgumentException Thrown when arguments have wrong types, wrong length, etc.
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		System.out.println("initialize");
		ObjectInspector objectInspector = arguments[0];
		ObjectInspector.Category category = objectInspector.getCategory();
		if(category.equals(ObjectInspector.Category.PRIMITIVE)){
		}
		return inspector = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}


	Text text = new Text();

	/**
	 *  执行具体的计算逻辑
	 */
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		System.out.println("evaluate");
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

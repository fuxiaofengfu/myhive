package avro;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AvroUtil {

	public byte[] jsonToAvro(Schema schema, String jsonFile) throws IOException {

		GenericData genericRecord = GenericData.get();
		DatumWriter datumWriter = genericRecord.createDatumWriter(schema);
		DatumReader datumReader = genericRecord.createDatumReader(schema);
		DataFileWriter dataFileWriter = new DataFileWriter(datumWriter);

		DataInputStream dataInputStream = new DataInputStream(new FileInputStream(jsonFile));
		JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, dataInputStream);
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		dataFileWriter.setCodec(CodecFactory.fromString(DataFileConstants.NULL_CODEC));

		dataFileWriter.create(schema, byteArrayOutputStream);
		System.out.println(byteArrayOutputStream.toString());
		Object datum = null;
		while (true) {
			try {
				datum = datumReader.read(datum, jsonDecoder);
				dataFileWriter.append(datum);
				byteArrayOutputStream.flush();
			} catch (IOException e) {
				//e.printStackTrace();
				break;
			}
		}
		dataFileWriter.close();
		byteArrayOutputStream.close();
		dataInputStream.close();
		return byteArrayOutputStream.toByteArray();
	}

	public String avroToJson(Schema schema, String avroFile) throws IOException {
		GenericData genericRecord = GenericData.get();
		DatumWriter datumWriter = genericRecord.createDatumWriter(schema);

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, byteArrayOutputStream, false);

		DatumReader datumReader = genericRecord.createDatumReader(schema);
		//BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(new FileInputStream(avroFile), null);
		DataFileStream dataFileStream = new DataFileStream(new FileInputStream(avroFile), datumReader);
		Object read = null;
		while (dataFileStream.hasNext()) {
			read = dataFileStream.next(read);
			datumWriter.write(read, jsonEncoder);
			jsonEncoder.flush();
		}
		byteArrayOutputStream.flush();
		return new String(byteArrayOutputStream.toByteArray());
	}

	public Schema addField(Schema schema, Map<Schema.Field, Schema.Type> fieldsMap) {
		Schema record = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
		List<Schema.Field> fields = new ArrayList<>();
		schema.getFields().forEach(field -> {
			fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()));
		});
		fieldsMap.forEach((k, v) -> {
			fields.add(new Schema.Field(k.name(), Schema.createUnion(Schema.create(v), Schema.create(Schema.Type.NULL)), k.doc(), k.defaultVal()));
		});
		record.setFields(fields);
		//return Schema.createRecord(fields);
		return record;
	}

	public Schema deleteField(Schema schema, String... delFields) {
		Schema record = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
		List<Schema.Field> fields = new ArrayList<>();
		schema.getFields().forEach(field -> {
			for (String delField : delFields) {
				if (!field.name().equals(delField))
					fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()));
			}
		});
		record.setFields(fields);
		return record;
	}

	public Schema loadSchema(String filePath) {

		File file = new File(filePath);
		try {
			if (!file.exists()) {
				throw new IOException(filePath + "----->file not found");
			}
			return new Schema.Parser().parse(file);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		AvroUtil avroUtil = new AvroUtil();
		Schema schema = avroUtil.loadSchema("src/main/resources/avro.avsc");
		//byte[] bytes = avroUtil.jsonToAvro(schema, "avro1/aa");
		//System.out.println(bytes.length);
		byte[] jsonNode = avroUtil.jsonToAvro(schema, "avro1/aa");
		System.out.println(jsonNode.length);
		System.out.println(new String(jsonNode));
		//System.out.println(jsonNode);
		//System.out.println(jsonNode);
		org.apache.avro.tool.Main.main(new String[]{"fromjson", "avro1/aa", "--schema-file", "src/main/resources/avro.avsc"});
		//org.apache.avro.tool.Main.main(new String[]{"1"});
	}
}

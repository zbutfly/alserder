package net.butfly.alserdes.avro;

import static net.butfly.albacore.utils.collection.Colls.empty;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public class AvroSerDes extends AvroSchemalessSerDes {
	private static final long serialVersionUID = -2576795139831747265L;
	private static final Logger logger = Logger.getLogger(AvroSerDes.class);

	@Override
	public byte[] ser(Map<String, Object> m) {
		if (empty(m)) return null;
		return Builder.ser(m, Builder.schema(m));
	}

	@Override
	public Map<String, Object> deser(byte[] b) {
		if (null == b || 0 == b.length) return null;
		throw new UnsupportedOperationException("Deserialization as avro without schema (FieldDesc) is not supported.");
	}

	public static interface Builder {
		static Schema schema(Map<String, Object> src) {
			List<Schema.Field> fieldList = new ArrayList<>();
			src.forEach((k, v) -> {
				Schema.Field schemaField = field(k, v);
				fieldList.add(schemaField);
			});
			Schema s = Schema.createRecord(fieldList);
			logger.trace("Schema build from field list: \n\t" + s.toString());
			return s;
		}

		static Schema.Field field(String name, Object type) {
			if (null == name && null == type) return null;
			if (type instanceof byte[]) return new Schema.Field(name, Schema.create(Schema.Type.BYTES), null, new byte[0]);
			else if (type instanceof Integer) return new Schema.Field(name, Schema.create(Schema.Type.INT), null, 0);
			else if (type instanceof Long || type instanceof Date) return new Schema.Field(name, Schema.create(Schema.Type.LONG), null, 0);
			else if (type instanceof Float) return new Schema.Field(name, Schema.create(Schema.Type.FLOAT), null, 0.0);
			else if (type instanceof Double) return new Schema.Field(name, Schema.create(Schema.Type.DOUBLE), null, 0.0);
			else if (type instanceof Boolean) return new Schema.Field(name, Schema.create(Schema.Type.BOOLEAN), null, false);
			else if (type instanceof String) return new Schema.Field(name, Schema.create(Schema.Type.STRING), null, "");
			else throw new UnsupportedOperationException("This schema type  is unsupported");
		}

		/**
		 * it WRONG!
		 */
		@Deprecated
		static Schema schemaWrong(Map<String, Object> dst) {
			List<Schema.Field> fieldList = new ArrayList<>();
			dst.forEach((k, v) -> {
				if (v instanceof byte[]) {
					ByteArrayInputStream bi = new ByteArrayInputStream((byte[]) v);
					ObjectInputStream oi = null;
					try {
						oi = new ObjectInputStream(bi);
						Object obj = oi.readObject();
						Schema.Field schemaField = field(k, obj);
						fieldList.add(schemaField);
					} catch (Exception e) {
						logger.error("byteArray transform object error", e);
					}
				}
			});
			return Schema.createRecord(fieldList);
		}

		static final Map<String, DataFileWriter<GenericRecord>> WRITERS = Maps.of();
		static final Map<String, DatumReader<GenericRecord>> READERS = Maps.of();

		static byte[] ser(Map<String, Object> m, Schema schema) {
			GenericRecord rec = rec(m, schema);
			DataFileWriter<GenericRecord> writer = WRITERS.computeIfAbsent(schema.getFullName(), //
					q -> new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema)));
			byte[] avro;
			try (ByteArrayOutputStream out = new ByteArrayOutputStream();) {
				writer.create(schema, out);
				writer.append(rec);
				avro = out.toByteArray();
				return avro.length == 0 ? null : avro;
			} catch (Exception e) {
				return null;
			}
		}

		static Map<String, Object> deser(byte[] v, Schema schema) {
			if (null == v || v.length == 0) return null;
			DatumReader<GenericRecord> reader = READERS.computeIfAbsent(schema.getFullName(), q -> new GenericDatumReader<GenericRecord>(
					schema));
			try (SeekableInput in = new SeekableByteArrayInput((byte[]) v);
					FileReader<GenericRecord> fr = DataFileReader.openReader(in, reader);) {
				if (!fr.hasNext()) return null;
				GenericRecord rec = fr.next();
				if (null == rec || null == rec.getSchema() || empty(rec.getSchema().getFields())) return null;
				Map<String, Object> mm = Maps.of();
				rec.getSchema().getFields().forEach(field -> mm.put(field.name(), rec.get(field.name())));
				return mm;
			} catch (IOException e) {
				return null;
			}
		}

		static GenericRecord rec(Map<String, Object> m, Schema schema) {
			GenericRecord rec = new GenericData.Record(schema);
			schema.getFields().forEach(f -> rec.put(f.name(), m.get(f.name())));
			return rec;
		}
	}
}

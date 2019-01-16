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
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.alserdes.SerDes.MapSerDes;

public class AvroSerDes implements MapSerDes<byte[]> {
	private static final long serialVersionUID = -4572168739782292658L;
	private static final Logger logger = Logger.getLogger(AvroSerDes.class);
	private static final Schema MAP_SCHEMA = Schema.createMap(Schema.createUnion(Colls.list(t -> Schema.create(t), //
			Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.BOOLEAN, //
			Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE)));
	private static final Schema LIST_SCHEMA = Schema.createArray(MAP_SCHEMA);
	private static final EncoderFactory encFactory = EncoderFactory.get();
	private static final DecoderFactory decFactory = DecoderFactory.get();

	// RECORD, ENUM, ARRAY, MAP, UNION, FIXED, NULL,
	@Override
	public byte[] ser(Map<String, Object> m) {
		if (empty(m)) return null;
		return avro(m, schema(m));
	}

	@Override
	public Map<String, Object> deser(byte[] b) {
		if (null == b || 0 == b.length) return null;
		throw new UnsupportedOperationException("Deserialization as avro without schema (FieldDesc) is not supported.");
	}

	@Override
	public byte[] sers(List<Map<String, Object>> l) {
		if (empty(l)) return null;
		try (final ByteArrayOutputStream o = new ByteArrayOutputStream();) {
			JsonEncoder enc = encFactory.jsonEncoder(LIST_SCHEMA, o);
			final DatumWriter<Object> writer = new ReflectDatumWriter<>();
			writer.write(l, enc);
			return o.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Map<String, Object>> desers(byte[] b) {
		if (null == b || b.length == 0) return null;
		try (final ByteArrayInputStream i = new ByteArrayInputStream(b);) {
			JsonDecoder dec = decFactory.jsonDecoder(LIST_SCHEMA, i);
			final DatumReader<Object> reader = new ReflectDatumReader<>();
			return (List<Map<String, Object>>) reader.read(null, dec);
		} catch (IOException e) {
			return null;
		}
	}

	public static Schema schema(Map<String, Object> src) {
		List<Schema.Field> fieldList = new ArrayList<>();
		src.forEach((k, v) -> {
			Schema.Field schemaField = field(k, v);
			fieldList.add(schemaField);
		});
		Schema s = Schema.createRecord(fieldList);
		logger.trace("Schema build from field list: \n\t" + s.toString());
		return s;
	}

	private static Schema.Field field(String name, Object type) {
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
	protected static Schema wrongSchema(Map<String, Object> dst) {
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

	private static final Map<String, DataFileWriter<GenericRecord>> WRITERS = Maps.of();
	private static final Map<String, DatumReader<GenericRecord>> READERS = Maps.of();

	public byte[] avro(Map<String, Object> m, Schema schema) {
		GenericData.Record rec = new GenericData.Record(schema);
		m.forEach(rec::put);
		byte[] avro;
		DataFileWriter<GenericRecord> writer = WRITERS.computeIfAbsent(schema.getFullName(), q -> new DataFileWriter<>(
				new GenericDatumWriter<GenericRecord>(schema)));
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();) {
			writer.create(schema, out);
			writer.append(rec);
			avro = out.toByteArray();
			return avro.length == 0 ? null : avro;
		} catch (Exception e) {
			return null;
		}
	}

	public Map<String, Object> avro(byte[] v, Schema schema) {
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
}

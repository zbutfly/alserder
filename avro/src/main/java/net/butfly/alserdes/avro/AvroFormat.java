package net.butfly.alserdes.avro;

import static net.butfly.albacore.utils.collection.Colls.empty;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BINARY;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BOOL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.CHAR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DOUBLE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.FLOAT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.GEO;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.JSON_STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STRL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import org.apache.commons.io.output.ByteArrayOutputStream;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.RmapFormat;
import net.butfly.alserdes.SerDes;

@SerDes.As("avro")
public class AvroFormat extends RmapFormat {
	private static final long serialVersionUID = 3687957634350865452L;
	private static final Logger logger = Logger.getLogger(AvroFormat.class);
	private static final Map<String, Schema> SCHEMAS = Maps.of();
	private static final Map<String, DataFileWriter<GenericRecord>> WRITERS = Maps.of();
	private static final Map<String, DatumReader<GenericRecord>> READERS = Maps.of();

	@Override
	public Rmap ser(Rmap m, TableDesc dst) {
		if (empty(dst.fields())) return ser(m);
		Schema schema = SCHEMAS.computeIfAbsent(dst.name, t -> avroSchema(dst.fields()));
		DataFileWriter<GenericRecord> w = WRITERS.computeIfAbsent(dst.name, t -> new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(
				schema)));
		return avroSer(m, schema, w);
	}

	@Override
	public Rmap ser(Rmap src) {
		if (empty(src)) return null;
		Schema schema = avroSchema(src);
		return avroSer(src, schema, new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema)));
	}

	@Override
	public Rmap deser(Rmap m, TableDesc dst) {
		if (empty(m)) return null;
		if (empty(dst.fields())) return deser(m);
		Schema schema = SCHEMAS.computeIfAbsent(dst.name, k -> avroSchema(dst.fields()));
		DatumReader<GenericRecord> r = READERS.computeIfAbsent(dst.name, t -> new GenericDatumReader<GenericRecord>(schema));
		return avroDeser(m, r);

	}

	@Override
	public Rmap deser(Rmap m) {
		if (empty(m)) return null;
		throw new UnsupportedOperationException("Deserialization as avro without schema (FieldDesc) is not supported.");
		// Schema schema = schema(m);
		// DatumReader<GenericRecord> r = new GenericDatumReader<GenericRecord>(schema);
		// return deserBySchema(m, r);
	}

	@Override
	public Rmap sers(List<Rmap> l, TableDesc dst) {
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");
	}

	@Override
	public List<Rmap> desers(Rmap rmap, TableDesc dst) {
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");
	}

	@Override
	public Class<?> formatClass() {
		return byte[].class;
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
					Schema.Field schemaField = avroField(k, obj);
					fieldList.add(schemaField);
				} catch (Exception e) {
					logger.error("byteArray transform object error", e);
				}
			}
		});
		return Schema.createRecord(fieldList);
	}

	private static Schema avroSchema(FieldDesc... fields) {
		List<Schema.Field> fieldList = new ArrayList<>();
		for (FieldDesc f : fields)
			fieldList.add(avroField(f));
		Schema s = Schema.createRecord(fieldList);
		logger.info("Schema build from field list: \n\t" + s.toString());
		return s;
	}

	private static Schema avroSchema(Map<String, Object> src) {
		List<Schema.Field> fieldList = new ArrayList<>();
		src.forEach((k, v) -> {
			Schema.Field schemaField = avroField(k, v);
			fieldList.add(schemaField);
		});
		Schema s = Schema.createRecord(fieldList);
		logger.trace("Schema build from field list: \n\t" + s.toString());
		return s;
	}

	private static Schema.Field avroField(FieldDesc f) {
		if (null == f) return null;
		switch (f.type.flag) {
		case BINARY:
			return new Schema.Field(f.name, Schema.create(Schema.Type.BYTES), null, new byte[0]);
		case INT:
			return new Schema.Field(f.name, Schema.create(Schema.Type.INT), null, 0);
		case DATE:
		case LONG:
			return new Schema.Field(f.name, Schema.create(Schema.Type.LONG), null, 0);
		case FLOAT:
			return new Schema.Field(f.name, Schema.create(Schema.Type.FLOAT), null, 0.0);
		case DOUBLE:
			return new Schema.Field(f.name, Schema.create(Schema.Type.DOUBLE), null, 0.0);
		case BOOL:
			return new Schema.Field(f.name, Schema.create(Schema.Type.BOOLEAN), null, false);
		case CHAR:
		case STR:
		case STRL:
		case GEO:
		case JSON_STR:
		case UNKNOWN:
			return new Schema.Field(f.name, Schema.create(Schema.Type.STRING), null, "");
		}
		return null;
	}

	private static Schema.Field avroField(String name, Object type) {
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

	private Rmap avroSer(Rmap m, Schema schema, DataFileWriter<GenericRecord> writer) {
		GenericData.Record rec = new GenericData.Record(schema);
		m.map().forEach(rec::put);
		byte[] avro;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();) {
			writer.create(schema, out);
			writer.append(rec);
			avro = out.toByteArray();
		} catch (Exception e) {
			return null;
		}
		if (avro.length == 0) return null;
		Rmap r = m.skeleton();
		r.put(UUID.randomUUID().toString(), avro);
		return r;
	}

	private Rmap avroDeser(Rmap m, DatumReader<GenericRecord> reader) {
		if (m.size() > 1) logger().warn("Multiple binary values of one record is not supported.");
		byte[] v = (byte[]) m.entrySet().iterator().next().getValue();
		try (SeekableInput in = new SeekableByteArrayInput((byte[]) v);
				FileReader<GenericRecord> fr = DataFileReader.openReader(in, reader);) {
			if (!fr.hasNext()) return null;
			GenericRecord rec = fr.next();
			if (null == rec || null == rec.getSchema() || empty(rec.getSchema().getFields())) return null;
			Rmap mm = m.skeleton();
			rec.getSchema().getFields().forEach(field -> mm.put(field.name(), rec.get(field.name())));
			return mm;
		} catch (IOException e) {
			return null;
		}
	}
}

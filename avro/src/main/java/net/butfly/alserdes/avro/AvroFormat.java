package net.butfly.alserdes.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.RmapFormat;
import net.butfly.alserdes.SerDes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static net.butfly.albacore.utils.collection.Colls.empty;
import static net.butfly.albatis.ddl.vals.ValType.Flags.*;

@SerDes.As("avro")
public class AvroFormat extends RmapFormat implements Loggable {
	private static final long serialVersionUID = 3687957634350865452L;

	@Override
	public Rmap ser(Rmap src, TableDesc dst) {
		Rmap rmap = new Rmap();
		if (null == dst.fields() || dst.fields().length == 0) return ser(src);
		Schema schema = getSchema(dst.fields());
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		GenericData.Record avroRecord = new GenericData.Record(schema);
		src.map().forEach(avroRecord::put);
		byte[] avroRecordBytes = recordInjection.apply(avroRecord);
		if (null != avroRecordBytes) rmap.put(src.table(), avroRecordBytes);
		logger().info("Avro format ser " + rmap);
		rmap.table(src.table());
		return rmap;
	}

	@Override
	public Rmap deser(Rmap map, TableDesc dst) {
		Rmap rmap = new Rmap();
		Schema schema;
		if (null == dst.fields() || dst.fields().length == 0) return deser(map);
		schema = getSchema(dst.fields());
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		map.map().forEach((k, v) -> {
			GenericRecord genericRecord = recordInjection.invert((byte[]) v).get();
			if (null != genericRecord) rmap.put(k, genericRecord);
		});
		logger().info("Avro format deser " + rmap);
		rmap.table(map.table());
		return rmap;
	}

	private Schema getSchema(FieldDesc... fields) {
		List<Schema.Field> fieldList = new ArrayList<>();
		for (FieldDesc f : fields)
			fieldList.add(getSchemaField(f));
		return Schema.createRecord(fieldList);
	}

	private Schema.Field getSchemaField(FieldDesc f) {
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

	@Override
	public Rmap ser(Rmap src) {
		if (empty(src)) return null;
		Rmap rmap = new Rmap();
		List<Schema.Field> fieldList = new ArrayList<>();
		src.forEach((k, v) -> {
			Schema.Field schemaField = getSchemaField(k, v);
			fieldList.add(schemaField);
		});
		Schema schema = Schema.createRecord(fieldList);
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		GenericData.Record avroRecord = new GenericData.Record(schema);
		src.forEach(avroRecord::put);
		byte[] avroRecordBytes = recordInjection.apply(avroRecord);
		if (null != avroRecordBytes) rmap.put(src.table(), avroRecordBytes);
		logger().info("Avro format ser " + rmap);
		rmap.table(src.table());
		return rmap;
	}

	private Schema.Field getSchemaField(String name, Object type) {
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

	@Override
	public Rmap deser(Rmap dst) {
		if (empty(dst)) return null;
		Rmap rmap = new Rmap();
		List<Schema.Field> fieldList = new ArrayList<>();
		dst.forEach((k, v) -> {
			if (v instanceof byte[]) {
				ByteArrayInputStream bi = new ByteArrayInputStream((byte[]) v);
				ObjectInputStream oi = null;
				try {
					oi = new ObjectInputStream(bi);
					Object obj = oi.readObject();
					Schema.Field schemaField = getSchemaField(k, obj);
					fieldList.add(schemaField);
				} catch (Exception e) {
					logger().error("byteArray transform object error", e);
				}
			}
		});
		Schema schema = Schema.createRecord(fieldList);
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		dst.map().forEach((k, v) -> {
			GenericRecord genericRecord = recordInjection.invert((byte[]) v).get();
			if (null != genericRecord) rmap.put(k, genericRecord);
		});
		logger().info("Avro format deser " + rmap);
		rmap.table(dst.table());
		return rmap;
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
}

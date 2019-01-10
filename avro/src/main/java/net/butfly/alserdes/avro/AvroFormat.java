package net.butfly.alserdes.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.RmapFormat;
import net.butfly.alserdes.SerDes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;

import static net.butfly.albatis.ddl.vals.ValType.Flags.*;

@SerDes.As("avro")
public class AvroFormat extends RmapFormat {
	private static final long serialVersionUID = 3687957634350865452L;

	@Override
	public Rmap ser(Rmap src, TableDesc dst) {
		Rmap rmap = new Rmap();
		Schema schema = getSchema(dst.fields());
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		GenericData.Record avroRecord = new GenericData.Record(schema);
		src.map().forEach(avroRecord::put);
		byte[] avroRecordBytes = recordInjection.apply(avroRecord);
		rmap.put(src.table(), avroRecordBytes);
		return rmap;
	}

	@Override
	public Rmap deser(Rmap map, TableDesc dst) {
		Rmap rmap = new Rmap();
		Schema schema = getSchema(dst.fields());
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		map.map().forEach((k, v) -> {
			GenericRecord genericRecord = recordInjection.invert((byte[]) v).get();
			rmap.put(k, genericRecord);
		});
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
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");
	}

	@Override
	public Rmap deser(Rmap dst) {
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");
	}

	@Override
	public Rmap sers(List<Rmap> l, TableDesc dst) {
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");
	}

	@Override
	public List<Rmap> desers(Rmap rmap, TableDesc dst) {
		throw new UnsupportedOperationException("Schemaness record list format not implemented now.");
	}
}

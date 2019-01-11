package net.butfly.alserdes.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.RmapFormat;
import net.butfly.alserdes.SerDes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static net.butfly.albacore.utils.collection.Colls.empty;
import static net.butfly.albatis.ddl.vals.ValType.Flags.*;

@SerDes.As("avro")
public class AvroFormat extends RmapFormat {
	private static final long serialVersionUID = 3687957634350865452L;
	private static final Schema MAP_SCHEMA = Schema.createMap(Schema.createUnion(Colls.list(Schema::create, //
			Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.BOOLEAN, //
			Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE)));
	private static final Schema LIST_SCHEMA = Schema.createArray(MAP_SCHEMA);
	private static final EncoderFactory encFactory = EncoderFactory.get();
	private static final DecoderFactory decFactory = DecoderFactory.get();

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
		if (empty(src)) return null;
		Rmap rmap = new Rmap();
		try (final ByteArrayOutputStream o = new ByteArrayOutputStream()) {
			JsonEncoder enc = encFactory.jsonEncoder(LIST_SCHEMA, o);
			final DatumWriter<Object> writer = new ReflectDatumWriter<>();
			writer.write(src, enc);
			rmap.put(src.table(),o.toByteArray());
			return rmap;
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public Rmap deser(Rmap dst) {
		if (empty(dst)) return null;
		for(Object v :dst.map().values()){
			try (final ByteArrayInputStream i = new ByteArrayInputStream((byte[])v)) {
				JsonDecoder dec = decFactory.jsonDecoder(MAP_SCHEMA, i);
				final DatumReader<Object> reader = new ReflectDatumReader<>();
				return (Rmap) reader.read(null, dec);
			} catch (IOException e) {
				return null;
			}
		}
		return null;
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

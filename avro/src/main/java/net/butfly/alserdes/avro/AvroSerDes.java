package net.butfly.alserdes.avro;

import static net.butfly.albacore.utils.collection.Colls.empty;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.alserdes.SerDes.MapSerDes;

public class AvroSerDes implements MapSerDes<byte[]> {
	private static final long serialVersionUID = -4572168739782292658L;
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
		try (final ByteArrayOutputStream o = new ByteArrayOutputStream();) {
			JsonEncoder enc = encFactory.jsonEncoder(MAP_SCHEMA, o);
			final DatumWriter<Object> writer = new ReflectDatumWriter<>();
			writer.write(m, enc);
			return o.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> deser(byte[] b) {
		if (null == b || b.length == 0) return null;
		try (final ByteArrayInputStream i = new ByteArrayInputStream(b);) {
			JsonDecoder dec = decFactory.jsonDecoder(MAP_SCHEMA, i);
			final DatumReader<Object> reader = new ReflectDatumReader<>();
			return (Map<String, Object>) reader.read(null, dec);
		} catch (IOException e) {
			return null;
		}
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
}

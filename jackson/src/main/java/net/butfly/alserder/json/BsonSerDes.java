package net.butfly.alserder.json;

import java.util.Map;

import net.butfly.albacore.serder.BsonSerder;
import net.butfly.alserder.SerAs;
import net.butfly.alserder.SerDes.MapSerDes;

@SerAs(format = "bson", from = Map.class, to = byte[].class)
public class BsonSerDes implements MapSerDes<byte[]> {
	private static final long serialVersionUID = -4183221055041421951L;

	@Override
	public byte[] ser(Map<String, Object> m) {
		return null == m ? null : BsonSerder.map(m);
	}

	@Override
	public Map<String, Object> deser(byte[] bytes) {
		return null == bytes ? null : BsonSerder.map(bytes);
	}
}

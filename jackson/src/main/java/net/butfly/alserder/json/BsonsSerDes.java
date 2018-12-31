package net.butfly.alserder.json;

import java.util.List;
import java.util.Map;

import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.alserder.SerAs;
import net.butfly.alserder.SerDes.MapListSerDes;

@SerAs(format = "bsons", from = List.class, to = byte[].class)
public class BsonsSerDes implements MapListSerDes<byte[]> {
	private static final long serialVersionUID = -4183221055041421951L;

	@Override
	public byte[] ser(List<Map<String, Object>> l) {
		return BsonSerder.DEFAULT_OBJ.ser(l);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<Map<String, Object>> deser(byte[] v) {
		Object vvv = BsonSerder.DEFAULT_OBJ.der(v);
		List<Map<String, Object>> l = Colls.list();
		if (null == vvv) return null;
		if (vvv instanceof List) {
			for (Object vv : (List) vvv)
				l.add((Map<String, Object>) vv);
			return l;
		} else if (vvv.getClass().isArray()) {
			for (Object vv : (Object[]) vvv)
				l.add((Map<String, Object>) vv);
			return l;
		}
		throw new IllegalArgumentException("Bson not a list, but a " + vvv.getClass());
	}
}

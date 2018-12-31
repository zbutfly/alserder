package net.butfly.alserder.json;

import java.util.List;
import java.util.Map;

import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.alserder.SerDes;
import net.butfly.alserder.SerDes.MapSerDes;

@SerDes.As("bson")
@SerDes.As(value = "bsons", list = true)
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

	@Override
	public byte[] sers(List<Map<String, Object>> l) {
		return BsonSerder.DEFAULT_OBJ.ser(l);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<Map<String, Object>> desers(byte[] v) {
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

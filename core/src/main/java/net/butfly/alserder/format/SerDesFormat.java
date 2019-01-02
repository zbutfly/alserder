package net.butfly.alserder.format;

import static net.butfly.albacore.utils.collection.Colls.empty;

import java.util.Map;

import net.butfly.alserder.SerDes;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SerDesFormat<M extends Map<String, Object>, D> extends Format<M, D> {
	private static final long serialVersionUID = -5531474612265588196L;
	private final SerDes sd;

	public SerDesFormat(SerDes sd) {
		super();
		this.sd = sd;
	}

	@Override
	public M ser(M m) {
		if (empty(m)) return null;
		Object v;
		for (String k : m.keySet())
			if (null != (v = sd.ser(m.get(k)))) m.put(k, v);
		return m;
	}

	@Override
	public M deser(M m) {
		if (empty(m)) return null;
		Object v;
		for (String k : m.keySet())
			if (null != (v = sd.deser(m.get(k)))) m.put(k, v);
		return m;
	}
}

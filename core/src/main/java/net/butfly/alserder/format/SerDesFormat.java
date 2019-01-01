package net.butfly.alserder.format;

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
	public M assemble(M m, D... extra) {
		if (null == m || m.isEmpty()) return null;
		for (String k : m.keySet())
			m.put(k, sd.ser(m.get(k)));
		return m;
	}

	@Override
	public M disassemble(M m, D... extra) {
		if (null == m || m.isEmpty()) return null;
		for (String k : m.keySet())
			m.put(k, sd.deser(m.get(k)));
		return m;
	}
}

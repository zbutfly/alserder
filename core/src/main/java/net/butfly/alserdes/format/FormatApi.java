package net.butfly.alserdes.format;

import static net.butfly.albacore.utils.collection.Colls.empty;

import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.alserdes.SerDes;

public interface FormatApi<M extends Map<String, Object>> extends SerDes<M, M> {
	/**
	 * <b>Schemaless</b> record assemble.
	 */
	@Override
	M ser(M src);

	/**
	 * <b>Schemaless</b> record disassemble.
	 */
	@Override
	M deser(M dst);

	/**
	 * <b>Schemaless</b> record list assemble.
	 */
	@Override
	default M sers(List<M> l) {
		if (l.size() > 1) //
			logger().warn(getClass() + " does not support multiple serializing, only first will be writen: \n\t" + l.toString());
		M first = l.get(0);
		return empty(first) ? null : ser(first);
	}

	/**
	 * <b>Schemaless</b> record list disassemble.
	 */
	@Override
	default List<M> desers(M m) {
		M r = deser(m);
		return empty(r) ? Colls.list() : Colls.list(r);
	}
}

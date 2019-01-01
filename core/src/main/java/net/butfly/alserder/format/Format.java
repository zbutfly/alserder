package net.butfly.alserder.format;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.utils.Generics;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.alserder.SerDes;

// it's Map SerDes
public abstract class Format<M extends Map<String, Object>, D> implements Loggable, Serializable {
	private static final long serialVersionUID = -8223020677916696133L;
	private SerDes.As as;

	// loop invoking, need override at least one pair of methods.
	public M assemble(M m, @SuppressWarnings("unchecked") D... extra) {
		if (null == m || m.isEmpty()) return null;
		M r = assembles(Colls.list(m), extra);
		return null == r || r.isEmpty() ? null : r;
	}

	public M disassemble(M m, @SuppressWarnings("unchecked") D... extra) {
		if (null == m || m.isEmpty()) return null;
		List<M> l = disassembles(m, extra);
		return null == l || l.isEmpty() ? null : l.get(0);
	}

	public M assembles(List<M> l, @SuppressWarnings("unchecked") D... extra) {
		if (null == l || l.isEmpty()) return null;
		if (l.size() > 1) //
			logger().warn(getClass() + " does not support multiple serializing, only first will be writen: \n\t" + l.toString());
		M first = l.get(0);
		return null == first ? null : assemble(first, extra);
	}

	public List<M> disassembles(M m, @SuppressWarnings("unchecked") D... extra) {
		if (null == m || m.isEmpty()) return Colls.list();
		M r = disassemble(m, extra);
		return null == r || r.isEmpty() ? Colls.list() : Colls.list(r);
	}

	@SuppressWarnings("rawtypes")
	private static final Map<String, Format> FORMATS = Maps.of();
	private static Logger logger = Logger.getLogger(Format.class);

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <F extends Format> F of(String format) {
		return (F) FORMATS.computeIfAbsent(format, f -> {
			Set<Class<? extends Format>> fcls = Reflections.getSubClasses(Format.class);
			Format ff;
			for (Class<? extends Format> c : fcls)
				for (SerDes.As as : c.getAnnotationsByType(SerDes.As.class))
					if (format.equals(as.value()) && null != (ff = Reflections.construct(c))) return ff.as(as);
			logger.debug("Format [" + format + "] not found, scanning for SerDes.");
			Set<Class<? extends SerDes>> sdcls = Reflections.getSubClasses(SerDes.class);
			for (Class<? extends SerDes> c : sdcls)
				for (SerDes.As as : c.getAnnotationsByType(SerDes.As.class))
					if (format.equals(as.value())) return new SerDesFormat<>(Reflections.construct(c)).as(as);
			logger.warn("SerDes [" + format + "] not found, values will not be changed.");
			return constFormat();
		});
	}

	public static SerDes.As[] as(Class<?> cls) {
		Class<?> c = cls;
		SerDes.As[] ann;
		while ((ann = c.getAnnotationsByType(SerDes.As.class)).length == 0 && (null != (c = c.getSuperclass())));
		return ann;
	}

	public SerDes.As as() {
		return as;
	}

	public Format<M, D> as(SerDes.As as) {
		this.as = as;
		return this;
	}

	@SuppressWarnings("deprecation")
	public Class<?> rawClass() {
		return Generics.getGenericParamClass(this.getClass(), Format.class, "M");
	}

	public static <M extends Map<String, Object>, D> Format<M, D> constFormat() {
		return new ConstFormat<>();
	}
}

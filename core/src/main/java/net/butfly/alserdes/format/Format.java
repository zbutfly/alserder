package net.butfly.alserdes.format;

import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.utils.Generics;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.alserdes.SerDes;

import static net.butfly.albacore.utils.collection.Colls.empty;

// it's Map SerDes
public abstract class Format<M extends Map<String, Object>, D> implements FormatApi<M> {
	private static final long serialVersionUID = -8223020677916696133L;
	private SerDes.As as;

	// loop invoking, need override at least one pair of methods.
	public M ser(M m, D extra) {
		if (empty(m)) return null;
		M r = sers(Colls.list(m), extra);
		return empty(r) ? null : r;
	}

	public M deser(M m, D extra) {
		if (empty(m)) return null;
		List<M> l = desers(m, extra);
		return empty(l) ? null : l.get(0);
	}

	public M sers(List<M> l, D extra) {
		if (empty(l)) return null;
		if (l.size() > 1) //
			logger().warn(getClass() + " does not support multiple serializing, only first will be writen: \n\t" + l.toString());
		M first = l.get(0);
		return null == first ? null : ser(first, extra);
	}

	public List<M> desers(M m, D extra) {
		if (empty(m)) return Colls.list();
		M r = deser(m, extra);
		return empty(r) ? Colls.list() : Colls.list(r);
	}

	@SuppressWarnings("rawtypes")
	private static final Map<String, Format> FORMATS = Maps.of();
	private static Logger logger = Logger.getLogger(Format.class);

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <F extends Format> F of(String format) {
		return (F) FORMATS.computeIfAbsent(format, f -> {
			Set<Class<? extends Format>> fcls = Reflections.getSubClasses(Format.class);
			Format ff;
			for (Class<? extends Format> c : fcls) for (SerDes.As as : c.getAnnotationsByType(SerDes.As.class))
				if (format.equals(as.value()) && null != (ff = Reflections.construct(c))) return ff.as(as);
			logger.debug("Format [" + format + "] not found, scanning for SerDes.");
			Set<Class<? extends SerDes>> sdcls = Reflections.getSubClasses(SerDes.class);
			for (Class<? extends SerDes> c : sdcls)
				for (SerDes.As as : c.getAnnotationsByType(SerDes.As.class)) if (format.equals(as.value())) {
					SerDes sd = Reflections.construct(c);
					Format fff = (Format) Reflections.construct("net.butfly.albatis.io.format.SerDesFormat", sd);
					return fff.as(as);
				}
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

	@Override
	@SuppressWarnings("deprecation")
	public Class<?> rawClass() {
		return Generics.getGenericParamClass(this.getClass(), Format.class, "M");
	}

	public static <M extends Map<String, Object>, D> Format<M, D> constFormat() {
		return new ConstFormat<>();
	}
}

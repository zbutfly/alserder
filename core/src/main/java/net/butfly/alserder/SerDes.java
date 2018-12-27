
package net.butfly.alserder;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Loggable;

public interface SerDes<FROM, TO> extends Serializable, Loggable {
	TO ser(FROM v);

	FROM deser(TO r);

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <F, T, D extends SerDes<F, T>> D sd(String format) {
		boolean revert = format.startsWith("rev:");
		SerDes r = ALL_SDERS.computeIfAbsent(revert ? format.substring(4) : format, f -> {
			Set<Class<? extends SerDes>> impls = Reflections.getSubClasses(SerDes.class);
			for (Class<? extends SerDes> c : impls) {
				SerAs sd = c.getAnnotation(SerAs.class);
				if (null != sd && f.equals(sd.format())) try {
					return c.getConstructor().newInstance();
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException e) {
							continue;
						}
			}
			return null;
		});
		return (D) (null == r || !revert ? r : r.revert());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <F, T, D extends SerDes<F, T>> D sd(String... format) {
		if (null == format || format.length == 0) return null;
		List<String> formats = Colls.list(format);
		SerDes sd = sd(formats.remove(0));
		for (String f : formats)
			sd = sd.then(f);
		return (D) sd;
	}

	// info
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	@interface SerAs { // default value for bson
		String format();

		Class<?> from()

		default Map.class;

		Class<?> to() default byte[].class;

		// boolean byField() default true;
	}

	@SuppressWarnings("unchecked")
	default Class<FROM> fromClass() {
		SerAs a = _Private.sdAnn(this.getClass());
		return null == a ? null : (Class<FROM>) a.from();
	}

	@SuppressWarnings("unchecked")
	default Class<TO> toClass() {
		SerAs a = _Private.sdAnn(this.getClass());
		return null == a ? null : (Class<TO>) a.to();
	}

	default String format() {
		SerAs a = _Private.sdAnn(this.getClass());
		return null == a ? null : a.format();
	}

	/**
	 * on input, deser(), toClass is source class.<br>
	 * on output, ser(), fromClass is source class.<br>
	 * so the func return the <code>source class of serder is rmap</code>, means process whole record.<br>
	 * if not, processing fields each by each.<br>
	 */
	default boolean isMapOp(boolean input) {
		Class<?> c = input ? toClass() : fromClass();
		return null == c ? false : Map.class.isAssignableFrom(c);
	}

	// trans
	default <THEN> SerDes<FROM, THEN> then(String format) {
		return new SerDes<FROM, THEN>() {
			private static final long serialVersionUID = -1388017778891200080L;
			private SerDes<TO, THEN> then = SerDes.sd(format);

			@Override
			public THEN ser(FROM o) {
				return then.ser(SerDes.this.ser(o));
			}

			@Override
			public FROM deser(THEN r) {
				return SerDes.this.deser(then.deser(r));
			}
		};
	}

	default SerDes<TO, FROM> revert() {
		return new SerDes<TO, FROM>() {
			private static final long serialVersionUID = 1247776407568101791L;

			@Override
			public FROM ser(TO v) {
				return SerDes.this.deser(v);
			}

			@Override
			public TO deser(FROM r) {
				return SerDes.this.ser(r);
			}

			@Override
			public Class<TO> fromClass() {
				return SerDes.this.toClass();
			}

			@Override
			public Class<FROM> toClass() {
				return SerDes.this.fromClass();
			}

			@Override
			public String format() {
				return "rev:" + SerDes.this.format();
			}
		};
	}

	// lookup
	static final String DEFAULT_SD_FORMAT = Configs.gets("alserder.default.format", "str");
	@SuppressWarnings("rawtypes")
	static final Map<String, SerDes> ALL_SDERS = Maps.of();

	// other utils
	class _Private {
		@SuppressWarnings("rawtypes")
		private static SerAs sdAnn(Class<? extends SerDes> cls) {
			Class<?> c = cls;
			SerAs ann = null;
			while (null == (ann = c.getAnnotation(SerAs.class)) && (null != (c = c.getSuperclass())));
			return ann;
		}
	}

	// other
	interface MapSerDes<TO> extends SerDes<Map<String, Object>, TO> {}

	interface ListSerDes<FROM, TO> extends SerDes<List<FROM>, TO> {}

	interface MapListSerDes<TO> extends ListSerDes<Map<String, Object>, TO> {}
}

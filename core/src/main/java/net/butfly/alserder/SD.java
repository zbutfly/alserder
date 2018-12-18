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

/**
 * Serializer and Deserializer
 * 
 * @param <FROM>
 * @param <TO>
 */
public interface SD<FROM, TO> extends Serializable {
	TO ser(FROM v);

	FROM der(TO r);

	@SuppressWarnings("unchecked")
	default TO sers(FROM... v) {
		return sers(Colls.list(v));
	}

	default TO sers(List<FROM> vs) {
		throw new UnsupportedOperationException();
	}

	default List<FROM> ders(TO r) {
		return Colls.list(der(r));
	}

	// lookup
	static final String DEFAULT_SD_FORMAT = Configs.gets("alserder.default.format", "str");
	@SuppressWarnings("rawtypes")
	static final Map<String, SD> ALL_SDERS = Maps.of();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <F, T> SD<F, T> lookup(String format) {
		return ALL_SDERS.computeIfAbsent(format, f -> {
			Set<Class<? extends SD>> impls = Reflections.getSubClasses(SD.class);
			for (Class<? extends SD> c : impls) {
				SDon sd = c.getAnnotation(SDon.class);
				if (format.equals(sd.format())) try {
					return c.getConstructor().newInstance();
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException e) {
							continue;
						}
			}
			return new StrSD();
		});
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	@interface SDon { // default value for bson
		String format();

		Class<?> from() default Map.class;

		Class<?> to() default byte[].class;
	}

	// then
	default <THEN> SD<FROM, THEN> then(SD<TO, THEN> then) {
		return new SD<FROM, THEN>() {
			private static final long serialVersionUID = -1388017778891200080L;

			@Override
			public THEN ser(FROM o) {
				return then.ser(SD.this.ser(o));
			}

			@Override
			public FROM der(THEN r) {
				return SD.this.der(then.der(r));
			}

			@SuppressWarnings("unchecked")
			@Override
			public THEN sers(FROM... vs) {
				return then.ser(SD.this.sers(vs));
			}

			@Override
			public THEN sers(List<FROM> vs) {
				return then.ser(SD.this.sers(vs));
			}

			@Override
			public List<FROM> ders(THEN r) {
				List<FROM> f = Colls.list();
				then.ders(r).forEach(t -> f.add(SD.this.der(t)));
				return f;
			}
		};
	}

	// other utils
	default SDon sdAnn() {
		Class<?> c = this.getClass();
		SDon ann = null;
		while (null == (ann = c.getAnnotation(SDon.class)) && (null != (c = c.getSuperclass())));
		return ann;
	}

	@SuppressWarnings("unchecked")
	default Class<FROM> fromClass() {
		return (Class<FROM>) sdAnn().from();
	}

	@SuppressWarnings("unchecked")
	default Class<TO> toClass() {
		return (Class<TO>) sdAnn().to();
	}
}

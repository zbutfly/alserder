package net.butfly.alserdes.format;

import static net.butfly.albacore.utils.collection.Colls.empty;
import static net.butfly.alserdes.format.Format.as;
import static net.butfly.alserdes.format.Format.of;

import java.util.List;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Annotations;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.alserdes.SerDes;

public interface Formatable extends Loggable {
	URISpec uri();

	@SuppressWarnings("rawtypes")
	default List<Format> formats() {
		String format = uri().getParameter("df");
		List<Format> fmts = null == format ? Colls.list() : Colls.list(f -> of(f), format.split(","));

		SerDes.As[] defs = as(getClass());
		if (defs.length > 1) logger().warn("Multiple default serdes as annotations marked on " + getClass().getName() + ", first ["
				+ Annotations.toString(defs[0]) + "] will be used. All serdes as: \n\t" + String.join("\n\t", Colls.list(Annotations::toString,
						defs)));
		Format def = defs.length == 1 ? of(defs[0].value()) : null;
		if (null != def && ConstFormat.class.isAssignableFrom(def.getClass())) def = null;
		Format fmt1 = empty(fmts) ? null : fmts.get(0);
		if (null == fmt1) {
			if (null == def) return Colls.list();
			logger().info("Non-format defined, default format [" + def.as().value() + "] used.");
			return Colls.list(def);
		}
		if (defs.length > 0) logger().info("Default format [" + def.as().value() + "] is ignored by [" //
				+ String.join(",", Colls.list(fmts, f -> null == f || null == f.as() ? null : f.as().value())) + "]");
		return fmts;
	}
}

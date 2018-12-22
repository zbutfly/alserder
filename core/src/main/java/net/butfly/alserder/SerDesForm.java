package net.butfly.alserder;

import java.io.Serializable;
import java.util.List;

import net.butfly.albacore.utils.collection.Colls;

interface SerDesForm<FROM, TO> extends Serializable {
	@SuppressWarnings("unchecked")
	default TO sers(FROM... v) {
		return sers(Colls.list(v));
	}

	default TO sers(List<FROM> vs) {
		throw new UnsupportedOperationException();
	}

	List<FROM> desers(TO r);
}

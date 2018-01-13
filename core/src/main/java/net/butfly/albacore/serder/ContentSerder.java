package net.butfly.albacore.serder;

import java.io.Serializable;
import java.nio.charset.Charset;

import org.apache.http.entity.ContentType;

import com.google.common.base.Charsets;

public interface ContentSerder extends Serializable {
	default ContentType contentType() {
		return ContentTypes.TEXT_PLAIN;
	}

	default void charset(Charset charset) {}

	interface ContentTypes {
		ContentType APPLICATION_BSON = ContentType.create("application/bson", Charsets.UTF_8);
		ContentType APPLICATION_BURLAP = ContentType.create("x-application/burlap", Charsets.UTF_8);
		ContentType APPLICATION_HESSIAN = ContentType.create("x-application/hessian", Charsets.UTF_8);

		ContentType APPLICATION_XML = ContentType.create("application/xml", Charsets.UTF_8);
		ContentType TEXT_HTML = ContentType.create("text/html", Charsets.UTF_8);
		ContentType TEXT_PLAIN = ContentType.create("text/plain", Charsets.UTF_8);
		ContentType TEXT_XML = ContentType.create("text/xml", Charsets.UTF_8);

		static boolean mimeMatch(String mimeImpl, String mimeDeclared) {
			return mimeImpl.equals(mimeDeclared);
		}
	}
}

package net.butfly.alserder.json;

import java.util.Map;

import net.butfly.albacore.serder.JsonSerder;
import net.butfly.alserder.SerDes.MapSerDes;
import net.butfly.alserder.SerDes.SerAs;

@SerAs(format = "json", from = Map.class, to = String.class)
public class JsonSerDes implements MapSerDes<String> {
	private static final long serialVersionUID = 6767740047890492594L;

	@Override
	public String ser(Map<String, Object> m) {
		return null == m ? null : JsonSerder.JSON_MAPPER.ser(m);
	}

	@Override
	public Map<String, Object> deser(String s) {
		return null == s ? null : JsonSerder.JSON_MAPPER.der(s);
	}
}

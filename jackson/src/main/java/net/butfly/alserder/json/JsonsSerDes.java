package net.butfly.alserder.json;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;

import net.butfly.albacore.serder.json.Jsons;
import net.butfly.alserder.SerDes;
import net.butfly.alserder.SerDes.MapListSerDes;
import net.butfly.alserder.SerDes.SerAs;

@SerAs(format = "jsons", from = List.class, to = String.class)
public class JsonsSerDes implements MapListSerDes<String> {
	private static final long serialVersionUID = 6767740047890492594L;

	@Override
	public String ser(List<Map<String, Object>> vs) {
		try {
			return Jsons.mapper.writeValueAsString(vs);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public List<Map<String, Object>> deser(String s) {
		try {
			return Jsons.mapper.readValue(s, Jsons.mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
}

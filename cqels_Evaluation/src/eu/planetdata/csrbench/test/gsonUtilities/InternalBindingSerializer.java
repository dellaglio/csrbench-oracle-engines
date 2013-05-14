package eu.planetdata.csrbench.test.gsonUtilities;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class InternalBindingSerializer implements JsonSerializer<InternalBinding>{

	@Override
	public JsonElement serialize(InternalBinding arg0, Type arg1,JsonSerializationContext arg2) {
		JsonObject retValue = new JsonObject();
		if(arg0.getDatatype() != "")
			retValue.addProperty("datatype", arg0.getDatatype());
		retValue.addProperty("type", arg0.getType());
		retValue.addProperty("value", arg0.getValue());
		return retValue;
	}

}

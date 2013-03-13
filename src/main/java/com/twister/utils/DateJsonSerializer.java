package com.twister.utils;

import java.io.IOException;  
import java.text.SimpleDateFormat;  
import java.util.Date;  
  
import org.codehaus.jackson.JsonGenerator;  
import org.codehaus.jackson.JsonProcessingException;  
import org.codehaus.jackson.map.JsonSerializer;  
import org.codehaus.jackson.map.SerializerProvider;  
  
  
public class DateJsonSerializer extends JsonSerializer<Date>{  
  
    @Override  
    public void serialize(Date date, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)  
            throws IOException, JsonProcessingException {  
        SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");  
        String formatedDate= sdf.format(date);  
        jsonGenerator.writeString(formatedDate);  
    }  
  
}  
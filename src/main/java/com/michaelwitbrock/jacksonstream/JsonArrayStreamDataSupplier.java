/**
 * @license APACHE LICENSE, VERSION 2.0 http://www.apache.org/licenses/LICENSE-2.0
 * @author Michael Witbrock
 */
package com.michaelwitbrock.jacksonstream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JsonArrayStreamDataSupplier<T> implements Iterator<T> {
    /*
    * This class wraps the Jackson streaming API for arrays (a common kind of 
    * large JSON file) in a Java 8 Stream. The initial motivation was that 
    * use of a default objectmapper to a Java array was crashing for me on
    * a very large JSON file (> 1GB).  And there didn't seem to be good example 
    * code for handling Jackson streams as Java 8 streams, which seems natural.
    */
    
    static ObjectMapper mapper = new ObjectMapper();
    JsonParser parser;
    boolean maybeHasNext = false;
    int count = 0;
    JsonFactory factory = new JsonFactory();
    private Class<T> type;

    public JsonArrayStreamDataSupplier(InputStream inputStream, Class<T> type, String arrayToken){
        this(null, inputStream, type, arrayToken);

    }
    public JsonArrayStreamDataSupplier(File dataFile, Class<T> type, String arrayToken) {
        this(dataFile, null, type,arrayToken);

    }
    public JsonArrayStreamDataSupplier(File dataFile, InputStream inputStream, Class<T> type, String arrayToken) {
        this.type = type;
        try {
            if(dataFile == null && inputStream == null){
                throw new RuntimeException("File or InputStream must be set");
            }
            if(dataFile != null) {
                parser = factory.createParser(dataFile);
            }
            if(inputStream != null && parser == null) {
                parser = factory.createParser(inputStream);
            }
            parser.setCodec(mapper);
            JsonToken token = parser.nextToken();
            if (token == null) {
                throw new RuntimeException("Can't get any JSON Token from input");
            }
            // the first token is supposed to be the start of array '[' or start of Json Object'{'
            if (!JsonToken.START_ARRAY.equals(token) && !JsonToken.START_OBJECT.equals(token)) {
                // return or throw exception
                maybeHasNext = false;
                throw new RuntimeException("Can't get any JSON Token fro array start from input");
            }
            // If ArrayToken String has been given, fast forward until the given ArrayToken is found.
            if(arrayToken != null) {
                boolean arrayFound = false;
                while (arrayFound != true) {
                    token = parser.nextToken();
                    if (JsonToken.FIELD_NAME.equals(token)) {
                        if (parser.getValueAsString().equalsIgnoreCase(arrayToken)) {
                            parser.nextToken();
                            arrayFound = true;
                        }
                    }
                }
            }

        } catch (Exception e) {
            maybeHasNext = false;
        }
        maybeHasNext = true;
    }

    /*
    This method returns the stream, and is the only method other 
    than the constructor that should be used.
    */
    public Stream<T> getStream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), false);
    }

    /* The remaining methods are what enables this to be passed to the spliterator generator, 
       since they make it Iterable.
    */
    @Override
    public boolean hasNext() {
        if (!maybeHasNext) {
            return false; // didn't get started
        }
        try {
            return (parser.nextToken() == JsonToken.START_OBJECT);
        } catch (Exception e) {
            System.out.println("Ex" + e);
            return false;
        }
    }

    @Override
    public T next() {
        try {
            JsonNode n = parser.readValueAsTree();
            //Because we can't send T as a parameter to the mapper
            T node = mapper.convertValue(n, type);
            return node;
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("Ex" + e);
            return null;
        }

    }

}

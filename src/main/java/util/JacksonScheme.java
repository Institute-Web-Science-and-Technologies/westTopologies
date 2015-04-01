package util;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;




/**
 * Deserialisation scheme for JSON values using the jackson library.
 */
public class JacksonScheme implements Scheme {
    private static final long serialVersionUID = -7734176307841199017L;
    private final ObjectMapper mapper;
    
    public JacksonScheme() {
        this.mapper = new ObjectMapper();
    }

    /**
     * Deserialise a JSON value from <tt>bytes</tt> using the requested
     * character encoding.
     *
     * @param bytes
     * @return a one-element tuple containing the parsed JSON value.
     *
     * @throws IllegalArgumentException  if <tt>bytes</tt> does not contain
     *           valid JSON encoded using the requested encoding.
     * @throws IllegalStateException  if the requested character encoding is
     *           not supported.
     */
    @Override
    public List<Object> deserialize(byte[] bytes) {
        Object json = null;
        try {
            json = mapper.readValue(bytes, Object.class);
        } catch (IOException ex) {
            Logger.getLogger(JacksonScheme.class.getName()).log(Level.SEVERE, null, ex);
        }
        return Collections.singletonList(json);
    }


    /**
     * Emits tuples containing only one field, named "object".
     * @return 
     */
    @Override
    public Fields getOutputFields() {
        return new Fields("object");
    }
}

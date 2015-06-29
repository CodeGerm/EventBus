/**
 * 
 */
package org.cg.eventbus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;

/**
 * Event bus Avro serializer/deserializer. 
 * 
 * Usage : extend the AvroSerializer with default constructor 
 * 
 * @author yanlinwang
 *
 */
public class AvroSerializer<T> implements IEventBusSerializer<T>{
	
	private static Logger log = Logger.getLogger(AvroSerializer.class);
	private DatumWriter<T> writer;
	SpecificDatumReader<T> reader; 
	
	public AvroSerializer(Class<T> typeParameterClass) {
		writer = new SpecificDatumWriter<T>(typeParameterClass);
		reader = new SpecificDatumReader<T>(typeParameterClass);
	}

	@Override
	public T deserialize(String arg0, byte[] data) {
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
		T event = null;
		try {
			event = reader.read(null, decoder);
		} catch (IOException e) {
			log.error("Fail to read object from bytes" , e);
		}
		return event;
	}

	/* (non-Javadoc)
	 * @see kafka.serializer.Decoder#fromBytes(byte[])
	 */
	@Override
	public T fromBytes(byte[] arg0) {
		return deserialize(null,arg0);
	}

	@Override
	public void close() {
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
	}

	@Override
	public byte[] serialize(String arg0, T event) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		try {
			writer.write(event, encoder);
			encoder.flush();
			
		} catch (IOException e) {
			log.error("Fail to write object to bytes" , e);
		} finally{
			try {
				out.close();
			} catch (IOException e) {
				log.error("Fail to close the stream" , e);
			}
		}
		byte[] serializedBytes = out.toByteArray();
		return serializedBytes;
	}

}

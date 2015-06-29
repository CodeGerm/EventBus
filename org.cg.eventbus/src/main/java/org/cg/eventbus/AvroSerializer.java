/**
 * 
 */
package org.cg.eventbus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * @author yanlinwang
 *
 */
public class AvroSerializer<T> implements Encoder<T>, Decoder<T>{
	
	private static Logger log = Logger.getLogger(AvroSerializer.class);
	private DatumWriter<T> writer;
	SpecificDatumReader<T> reader; 
	
	public AvroSerializer(VerifiableProperties verifiableProperties,
			Class<T> typeParameterClass) {
		writer = new SpecificDatumWriter<T>(typeParameterClass);
		reader = new SpecificDatumReader<T>(typeParameterClass);
	}

	@Override
	public T fromBytes(byte[] data) {		
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
		T event = null;
		try {
			event = reader.read(null, decoder);
		} catch (IOException e) {
			log.error("Fail to read object from bytes" , e);
		}
		return event;

	}

	@Override
	public byte[] toBytes(T event) {
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

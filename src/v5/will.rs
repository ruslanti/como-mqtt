use std::convert::{TryFrom, TryInto};
use std::mem::size_of_val;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::encoder::encode_utf8_string;
use crate::v5::error::MqttError;
use crate::v5::error::MqttError::EndOfStream;
use crate::v5::error::MqttError::UnacceptableProperty;
use crate::v5::property::{PropertiesBuilder, PropertiesSize, Property, WillProperties};
use crate::v5::types::{MqttCodec, Will};

impl TryFrom<Bytes> for WillProperties {
    type Error = MqttError;

    fn try_from(mut reader: Bytes) -> Result<Self, Self::Error> {
        let mut builder = PropertiesBuilder::default();
        while reader.has_remaining() {
            let id = decode_variable_integer(&mut reader)?;
            let property = id.try_into()?;
            match property {
                Property::WillDelayInterval => {
                    end_of_stream!(reader.remaining() < 4, "will delay interval");
                    builder = builder.will_delay_interval(reader.get_u32())?;
                }
                Property::PayloadFormatIndicator => {
                    end_of_stream!(reader.remaining() < 1, "will payload format indicator");
                    builder = builder.payload_format_indicator(reader.get_u8())?;
                }
                Property::MessageExpireInterval => {
                    end_of_stream!(reader.remaining() < 4, "will message expire interval");
                    builder = builder.message_expire_interval(reader.get_u32())?;
                }
                Property::ContentType => {
                    builder = builder.content_type(decode_utf8_string(&mut reader)?)?;
                }
                Property::ResponseTopic => {
                    builder = builder.response_topic(decode_utf8_string(&mut reader)?)?;
                }
                Property::CorrelationData => unimplemented!(),
                Property::UserProperty => {
                    let user_property = (
                        decode_utf8_string(&mut reader)?,
                        decode_utf8_string(&mut reader)?,
                    );
                    if let (Some(key), Some(value)) = user_property {
                        builder = builder.user_property((key, value));
                    }
                }
                _ => return Err(UnacceptableProperty(property)),
            }
        }
        Ok(builder.will())
    }
}

impl PropertiesSize for Will {
    fn size(&self) -> usize {
        let properties_length = self.properties.size();
        properties_length.size() + properties_length + self.topic.len() + 4 + self.payload.len()
    }
}

impl Encoder<Will> for MqttCodec {
    type Error = MqttError;

    fn encode(&mut self, msg: Will, writer: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode(msg.properties, writer)?;
        self.encode(msg.topic, writer)?;
        writer.put_u16(msg.payload.len() as u16);
        self.encode(msg.payload, writer)
    }
}

impl PropertiesSize for WillProperties {
    fn size(&self) -> usize {
        let mut len = 4 + 1; //will_delay_interval
        len += check_size_of!(self, payload_format_indicator);
        len += check_size_of!(self, message_expire_interval);
        len += check_size_of_string!(self, content_type);
        len += check_size_of_string!(self, response_topic);
        len += check_size_of_bytes!(self, correlation_data);
        len += self
            .user_properties
            .iter()
            .map(|(x, y)| 5 + x.len() + y.len())
            .sum::<usize>();
        len
    }
}

impl Encoder<WillProperties> for MqttCodec {
    type Error = MqttError;

    fn encode(
        &mut self,
        properties: WillProperties,
        writer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.encode(properties.size(), writer)?;
        encode_property_u32!(
            writer,
            WillDelayInterval,
            Some(properties.will_delay_interval)
        );
        encode_property_u8!(
            writer,
            PayloadFormatIndicator,
            properties.payload_format_indicator.map(|b| b as u8)
        );
        encode_property_u32!(
            writer,
            MessageExpireInterval,
            properties.message_expire_interval
        );
        encode_property_string!(writer, ContentType, properties.content_type);
        encode_property_string!(writer, ResponseTopic, properties.response_topic);
        encode_property_bytes!(writer, CorrelationData, properties.correlation_data);
        encode_property_user_properties!(writer, UserProperty, properties.user_properties);
        Ok(())
    }
}

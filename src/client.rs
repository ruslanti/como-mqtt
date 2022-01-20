use std::fmt;
use std::net::SocketAddr;

use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::timeout;
use tokio::time::Duration;
use tokio_util::codec::Framed;
use tracing::trace;

use crate::identifier::PacketIdentifier;
use crate::v5::property::{PropertiesBuilder, SubscribeProperties};
use crate::v5::string::MqttString;
use crate::v5::types::{
    ConnAck, Connect, ControlPacket, Disconnect, MqttCodec, Publish, PublishResponse, QoS,
    ReasonCode, Retain, SubAck, Subscribe, SubscriptionOptions, Will,
};

pub struct Client {
    stream: Framed<TcpStream, MqttCodec>,
    client_id: Option<MqttString>,
    keep_alive: u16,
    properties_builder: PropertiesBuilder,
    timeout: Option<Duration>,
    packet_identifier: PacketIdentifier,
    username: Option<MqttString>,
    password: Option<Bytes>,
    will: Option<Will>,
}

pub struct ClientBuilder<'a> {
    address: &'a str,
    client_id: Option<MqttString>,
    keep_alive: Option<u16>,
    timeout: Option<Duration>,
    properties_builder: PropertiesBuilder,
    username: Option<MqttString>,
    password: Option<Bytes>,
    will: Option<Will>,
}

pub struct Publisher<'a> {
    dup: bool,
    qos: QoS,
    retain: bool,
    properties_builder: PropertiesBuilder,
    client: &'a mut Client,
}

pub struct Subscriber<'a> {
    qos: QoS,
    nl: bool,
    rap: bool,
    retain: Retain,
    properties_builder: PropertiesBuilder,
    client: &'a mut Client,
}

impl<'a> Client {
    pub fn builder(address: &'a str) -> ClientBuilder {
        ClientBuilder {
            address,
            client_id: None,
            keep_alive: None,
            timeout: None,
            properties_builder: PropertiesBuilder::default(),
            username: None,
            password: None,
            will: None,
        }
    }

    pub async fn connect(&mut self, clean_start: bool) -> Result<ConnAck> {
        let connect = Connect {
            reserved: false,
            clean_start_flag: clean_start,
            keep_alive: self.keep_alive,
            properties: self.properties_builder.to_owned().connect(),
            client_identifier: self.client_id.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            will: self.will.to_owned(),
        };
        trace!("send {:?}", connect);
        self.stream.send(ControlPacket::Connect(connect)).await?;
        self.recv().await.and_then(|packet| match packet {
            ControlPacket::ConnAck(ack) => Ok(ack),
            _ => Err(anyhow!("unexpected: {}", packet)),
        })
    }

    pub async fn connect_reserved(&mut self, clean_start: bool) -> Result<ConnAck> {
        let connect = Connect {
            reserved: true,
            clean_start_flag: clean_start,
            keep_alive: self.keep_alive,
            properties: self.properties_builder.to_owned().connect(),
            client_identifier: self.client_id.clone(),
            username: None,
            password: None,
            will: self.will.to_owned(),
        };
        trace!("send {}", connect);
        self.stream.send(ControlPacket::Connect(connect)).await?;
        self.recv().await.and_then(|packet| match packet {
            ControlPacket::ConnAck(ack) => Ok(ack),
            _ => Err(anyhow!("unexpected: {}", packet)),
        })
    }

    pub async fn send(&mut self, msg: ControlPacket) -> Result<()> {
        self.stream.send(msg).await.map_err(Error::msg)
    }

    pub async fn recv(&mut self) -> Result<ControlPacket, Error> {
        if let Some(time) = self.timeout {
            timeout(time, self.stream.next())
                .await
                .map_err(Error::msg)
                .and_then(|r| r.ok_or_else(|| anyhow!("disconnected")))
                .and_then(|r| r.map_err(Error::msg))
        } else {
            self.stream
                .next()
                .await
                .transpose()
                .map_err(Error::msg)
                .and_then(|r| r.ok_or_else(|| anyhow!("none message")))
                .map_err(Error::msg)
        }
    }

    pub async fn disconnect(&mut self) -> Result<Option<ControlPacket>> {
        self.disconnect_with_reason(ReasonCode::Success).await
    }

    pub async fn disconnect_with_reason(
        &mut self,
        reason_code: ReasonCode,
    ) -> Result<Option<ControlPacket>> {
        let disconnect = Disconnect {
            reason_code,
            properties: Default::default(),
        };

        trace!("send {}", disconnect);
        self.stream
            .send(ControlPacket::Disconnect(disconnect))
            .await?;
        // expected None on socket close
        self.stream.next().await.transpose().map_err(Error::msg)
    }

    pub fn publisher(&mut self) -> Publisher {
        Publisher {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            properties_builder: self.properties_builder.clone(),
            client: self,
        }
    }

    pub fn subscriber(&mut self) -> Subscriber {
        Subscriber {
            qos: QoS::AtMostOnce,
            nl: false,
            rap: false,
            retain: Retain::SendAtTime,
            properties_builder: self.properties_builder.clone(),
            client: self,
        }
    }

    pub async fn publish(
        &mut self,
        qos: QoS,
        topic_name: &str,
        payload: Vec<u8>,
        retain: bool,
    ) -> Result<Option<u16>> {
        let packet_identifier = (qos != QoS::AtMostOnce)
            .then(|| self.packet_identifier.next())
            .flatten();

        let publish = Publish {
            dup: false,
            qos,
            retain,
            topic_name: MqttString::from(topic_name.to_owned()),
            packet_identifier,
            properties: Default::default(),
            payload: Bytes::from(payload),
        };
        trace!("send {}", publish);
        self.stream
            .send(ControlPacket::Publish(publish))
            .await
            .map_err(Error::msg)?;
        Ok(packet_identifier)
    }

    pub async fn subscribe(&mut self, qos: QoS, topic_filter: &str) -> Result<SubAck> {
        let packet_identifier = self.packet_identifier.next();
        let subscribe = Subscribe {
            packet_identifier: packet_identifier.unwrap(),
            properties: SubscribeProperties::default(),
            topic_filters: vec![(
                MqttString::from(topic_filter.to_owned()),
                SubscriptionOptions {
                    qos,
                    nl: false,
                    rap: false,
                    retain: Retain::SendAtTime,
                },
            )],
        };
        trace!("send {}", subscribe);
        self.stream
            .send(ControlPacket::Subscribe(subscribe))
            .await?;
        self.recv().await.and_then(|packet| match packet {
            ControlPacket::SubAck(ack) => Ok(ack),
            _ => Err(anyhow!("unexpected: {}", packet)),
        })
    }

    pub async fn puback(&mut self, packet_identifier: u16) -> Result<()> {
        trace!("send PUBACK packet_identifier: {}", packet_identifier);
        self.stream
            .send(ControlPacket::PubAck(PublishResponse {
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            }))
            .await
            .map_err(Error::msg)
    }

    pub async fn pubrel(&mut self, packet_identifier: u16) -> Result<()> {
        trace!("send PUBREL packet_identifier: {}", packet_identifier);
        self.stream
            .send(ControlPacket::PubRel(PublishResponse {
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            }))
            .await
            .map_err(Error::msg)
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.stream.get_ref().local_addr().unwrap())
    }
}

impl ClientBuilder<'_> {
    pub fn client_id(mut self, value: &'static str) -> Self {
        self.client_id = Some(MqttString::from(value));
        self
    }

    pub fn keep_alive(mut self, value: u16) -> Self {
        self.keep_alive = Some(value);
        self
    }

    pub fn session_expire_interval(mut self, value: u32) -> Self {
        self.properties_builder = self
            .properties_builder
            .session_expire_interval(value)
            .unwrap();
        self
    }

    pub fn receive_maximum(mut self, value: u16) -> Self {
        self.properties_builder = self.properties_builder.receive_maximum(value).unwrap();
        self
    }

    pub fn user_property(mut self, (key, value): (&'static str, &'static str)) -> Self {
        self.properties_builder = self
            .properties_builder
            .user_property((MqttString::from(key), MqttString::from(value)));
        self
    }

    pub fn will(mut self, will: Will) -> Self {
        self.will = Some(will);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn username(mut self, value: &'static str) -> Self {
        self.username = Some(MqttString::from(value));
        self
    }

    pub fn password(mut self, value: &'static [u8]) -> Self {
        self.password = Some(Bytes::from_static(value));
        self
    }

    pub async fn build(self) -> Result<Client> {
        let peer: SocketAddr = self.address.parse()?;
        let socket = if peer.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };

        let stream = socket.connect(peer).await?;
        let stream = Framed::new(stream, MqttCodec::new(None));

        Ok(Client {
            stream,
            client_id: self.client_id,
            keep_alive: self.keep_alive.unwrap_or(0),
            properties_builder: self.properties_builder,
            timeout: self.timeout,
            packet_identifier: Default::default(),
            username: self.username,
            password: self.password,
            will: self.will,
        })
    }
}

impl Publisher<'_> {
    pub fn mark_dup(mut self) -> Self {
        self.dup = true;
        self
    }

    pub fn qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    pub fn mark_retain(mut self) -> Self {
        self.retain = true;
        self
    }

    pub fn payload_format_indicator(mut self, value: u8) -> Self {
        self.properties_builder = self
            .properties_builder
            .payload_format_indicator(value)
            .unwrap();
        self
    }
    pub fn message_expire_interval(mut self, value: u32) -> Self {
        self.properties_builder = self
            .properties_builder
            .message_expire_interval(value)
            .unwrap();
        self
    }
    pub fn content_type(mut self, value: &'static str) -> Self {
        self.properties_builder = self
            .properties_builder
            .content_type(Some(MqttString::from(value)))
            .unwrap();
        self
    }
    pub fn response_topic(mut self, value: &'static str) -> Self {
        self.properties_builder = self
            .properties_builder
            .response_topic(Some(MqttString::from(value)))
            .unwrap();
        self
    }
    pub fn correlation_data(mut self, value: &'static [u8]) -> Self {
        self.properties_builder = self
            .properties_builder
            .correlation_data(Some(Bytes::from_static(value)))
            .unwrap();
        self
    }
    pub fn topic_alias(mut self, value: u16) -> Self {
        self.properties_builder = self.properties_builder.topic_alias(value).unwrap();
        self
    }
    pub fn subscription_identifier(mut self, value: u32) -> Self {
        self.properties_builder = self
            .properties_builder
            .subscription_identifier(value)
            .unwrap();
        self
    }

    pub async fn publish(&mut self, topic_name: &str, payload: Vec<u8>) -> Result<Option<u16>> {
        let packet_identifier = (self.qos != QoS::AtMostOnce)
            .then(|| self.client.packet_identifier.next())
            .flatten();

        let publish = Publish {
            dup: self.dup,
            qos: self.qos,
            retain: self.retain,
            topic_name: MqttString::from(topic_name.to_owned()),
            packet_identifier,
            properties: self.properties_builder.clone().publish(),
            payload: Bytes::from(payload),
        };
        trace!("send {}", publish);
        self.client.send(ControlPacket::Publish(publish)).await?;
        Ok(packet_identifier)
    }

    pub async fn ack(&mut self) -> Result<PublishResponse, Error> {
        self.client.recv().await.and_then(|packet| match packet {
            ControlPacket::PubAck(ack) => {
                self.client.packet_identifier.release(ack.packet_identifier);
                Ok(ack)
            }
            unexpected => Err(anyhow!("unexpected: {}", unexpected)),
        })
    }

    pub async fn rec(&mut self) -> Result<PublishResponse, Error> {
        self.client.recv().await.and_then(|packet| match packet {
            ControlPacket::PubRec(ack) => Ok(ack),
            unexpected => Err(anyhow!("unexpected: {}", unexpected)),
        })
    }

    pub async fn rel(&mut self, packet_identifier: u16, reason_code: ReasonCode) -> Result<()> {
        self.client
            .send(ControlPacket::PubRel(PublishResponse {
                packet_identifier,
                reason_code,
                properties: self.properties_builder.clone().pubres(),
            }))
            .await
    }

    pub async fn comp(&mut self) -> Result<PublishResponse, Error> {
        self.client.recv().await.and_then(|packet| match packet {
            ControlPacket::PubComp(ack) => {
                self.client.packet_identifier.release(ack.packet_identifier);
                Ok(ack)
            }
            unexpected => Err(anyhow!("unexpected: {}", unexpected)),
        })
    }
}

impl Subscriber<'_> {
    pub fn qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    pub fn not_local(mut self) -> Self {
        self.nl = true;
        self
    }

    pub fn retain_as_published(mut self) -> Self {
        self.rap = true;
        self
    }

    pub fn retained_send_at_time(mut self) -> Self {
        self.retain = Retain::SendAtTime;
        self
    }

    pub fn retained_do_not_send(mut self) -> Self {
        self.retain = Retain::DoNotSend;
        self
    }

    pub fn retained_send_at_subscribe(mut self) -> Self {
        self.retain = Retain::SendAtSubscribe;
        self
    }

    pub async fn subscribe(self, topic_filter: &str) -> Result<SubAck> {
        let packet_identifier = self.client.packet_identifier.next();
        let subscribe = Subscribe {
            packet_identifier: packet_identifier.unwrap(),
            properties: self.properties_builder.subscribe(),
            topic_filters: vec![(
                MqttString::from(topic_filter.to_owned()),
                SubscriptionOptions {
                    qos: self.qos,
                    nl: self.nl,
                    rap: self.rap,
                    retain: self.retain,
                },
            )],
        };
        trace!("send {}", subscribe);
        self.client
            .send(ControlPacket::Subscribe(subscribe))
            .await?;
        self.client.recv().await.and_then(|packet| match packet {
            ControlPacket::SubAck(ack) => Ok(ack),
            _ => Err(anyhow!("unexpected: {}", packet)),
        })
    }

    pub async fn recv(&mut self) -> Result<ControlPacket, Error> {
        self.client.recv().await
    }
}

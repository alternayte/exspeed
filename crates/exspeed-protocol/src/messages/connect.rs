use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthType {
    None  = 0x00,
    Token = 0x01,
    MTls  = 0x02,
    Sasl  = 0x03,
}

impl TryFrom<u8> for AuthType {
    type Error = ProtocolError;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            0x00 => Ok(AuthType::None),
            0x01 => Ok(AuthType::Token),
            0x02 => Ok(AuthType::MTls),
            0x03 => Ok(AuthType::Sasl),
            other => Err(ProtocolError::Decode(format!("unknown auth type: 0x{other:02x}"))),
        }
    }
}

/// CONNECT request payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    pub client_id: String,
    pub auth_type: AuthType,
    pub auth_payload: Bytes,
}

impl ConnectRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        let id_bytes = self.client_id.as_bytes();
        dst.put_u16_le(id_bytes.len() as u16);
        dst.extend_from_slice(id_bytes);
        dst.put_u8(self.auth_type as u8);
        dst.extend_from_slice(&self.auth_payload);
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 3 {
            return Err(ProtocolError::Decode("CONNECT payload too short".into()));
        }

        let id_len = src.get_u16_le() as usize;

        if src.remaining() < id_len + 1 {
            return Err(ProtocolError::Decode("CONNECT payload truncated at client_id".into()));
        }

        let id_bytes = src.split_to(id_len);
        let client_id = String::from_utf8(id_bytes.to_vec())
            .map_err(|e| ProtocolError::Decode(format!("invalid client_id UTF-8: {e}")))?;

        let auth_type = AuthType::try_from(src.get_u8())?;
        let auth_payload = src;

        Ok(ConnectRequest {
            client_id,
            auth_type,
            auth_payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_roundtrip_no_auth() {
        let req = ConnectRequest {
            client_id: "my-client".into(),
            auth_type: AuthType::None,
            auth_payload: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = ConnectRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.client_id, "my-client");
        assert_eq!(decoded.auth_type, AuthType::None);
        assert!(decoded.auth_payload.is_empty());
    }

    #[test]
    fn connect_roundtrip_with_token() {
        let req = ConnectRequest {
            client_id: "service-1".into(),
            auth_type: AuthType::Token,
            auth_payload: Bytes::from_static(b"secret-token"),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = ConnectRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.client_id, "service-1");
        assert_eq!(decoded.auth_type, AuthType::Token);
        assert_eq!(decoded.auth_payload, Bytes::from_static(b"secret-token"));
    }

    #[test]
    fn connect_empty_payload_rejected() {
        let result = ConnectRequest::decode(Bytes::new());
        assert!(result.is_err());
    }
}

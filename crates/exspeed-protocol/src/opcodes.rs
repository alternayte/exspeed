use crate::error::ProtocolError;

/// Generates the `OpCode` enum together with its byte conversion and
/// client/server classification, so the three can never drift apart.
macro_rules! opcodes {
    (
        client { $($cname:ident = $cval:literal),* $(,)? }
        server { $($sname:ident = $sval:literal),* $(,)? }
        both { $($bname:ident = $bval:literal),* $(,)? }
    ) => {
        /// Wire protocol operation codes (protocol version 2).
        ///
        /// Client → server requests use `0x01–0x7F` (plus `Ping`); server →
        /// client responses and pushes use `0x80–0xFF`. `0x30`, `0x31` and
        /// `0xA0–0xA6` are reserved (an earlier replication design); servers
        /// replicate over their own protocol on the cluster port.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(u8)]
        pub enum OpCode {
            $($cname = $cval,)*
            $($sname = $sval,)*
            $($bname = $bval,)*
        }

        impl OpCode {
            pub fn is_client_opcode(self) -> bool {
                matches!(self, $(OpCode::$cname)|* $(| OpCode::$bname)*)
            }

            pub fn is_server_opcode(self) -> bool {
                matches!(self, $(OpCode::$sname)|* $(| OpCode::$bname)*)
            }

            pub fn as_u8(self) -> u8 {
                self as u8
            }
        }

        impl TryFrom<u8> for OpCode {
            type Error = ProtocolError;

            fn try_from(byte: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
                match byte {
                    $($cval => Ok(OpCode::$cname),)*
                    $($sval => Ok(OpCode::$sname),)*
                    $($bval => Ok(OpCode::$bname),)*
                    other => Err(ProtocolError::UnknownOpCode(other)),
                }
            }
        }

        #[cfg(test)]
        const ALL_OPCODES: &[OpCode] = &[
            $(OpCode::$cname,)* $(OpCode::$sname,)* $(OpCode::$bname,)*
        ];
    };
}

opcodes! {
    client {
        Connect = 0x01,
        Metadata = 0x03,
        Publish = 0x10,
        PublishBatch = 0x11,
        CreateStream = 0x18,
        UpdateStream = 0x19,
        DeleteStream = 0x1A,
        StreamInfo = 0x1B,
        ListStreams = 0x1C,
        Query = 0x20,
        CreateConsumer = 0x40,
        DeleteConsumer = 0x41,
        ConsumerInfo = 0x42,
        ListConsumers = 0x43,
        SeekConsumer = 0x44,
        Subscribe = 0x50,
        Credit = 0x51,
        Unsubscribe = 0x52,
        Pull = 0x53,
        Ack = 0x54,
        Nack = 0x55,
        Term = 0x56,
        InProgress = 0x57,
        Read = 0x60,
        Ping = 0xF0,
    }
    server {
        Ok = 0x80,
        Error = 0x81,
        Deliver = 0x82,
        Messages = 0x83,
        ReadResult = 0x84,
        Json = 0x85,
        PublishOk = 0x86,
        PublishBatchOk = 0x87,
        ConnectOk = 0x88,
        SubscribeOk = 0x89,
        SubscriptionEnded = 0x8A,
        Pong = 0xF1,
    }
    both {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_opcode_roundtrips() {
        for &code in ALL_OPCODES {
            assert_eq!(OpCode::try_from(code.as_u8()).unwrap(), code);
        }
    }

    #[test]
    fn opcode_values_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for &code in ALL_OPCODES {
            assert!(seen.insert(code.as_u8()), "duplicate value for {code:?}");
        }
    }

    #[test]
    fn unknown_opcode_rejected() {
        let result = OpCode::try_from(0xFF);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("0xff"));
    }

    #[test]
    fn client_server_classification() {
        assert!(OpCode::Connect.is_client_opcode());
        assert!(OpCode::Ping.is_client_opcode());
        assert!(!OpCode::Ping.is_server_opcode());
        assert!(OpCode::Deliver.is_server_opcode());
        assert!(!OpCode::Deliver.is_client_opcode());
    }

    /// The replication opcodes of an earlier design are gone; they are
    /// reserved and decode as unknown.
    #[test]
    fn reserved_opcodes_are_unknown() {
        for b in [0x30u8, 0x31, 0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6] {
            assert!(OpCode::try_from(b).is_err(), "0x{b:02x}");
        }
    }
}

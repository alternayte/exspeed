pub mod exspeed;
#[cfg(feature = "comparison")]
pub mod kafka;
pub mod publisher;

#[derive(Debug, Clone, Copy)]
pub enum Target {
    Exspeed,
    #[cfg(feature = "comparison")]
    Kafka,
}

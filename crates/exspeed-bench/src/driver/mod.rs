pub mod exspeed;
pub mod publisher;
#[cfg(feature = "comparison")]
pub mod kafka;

#[derive(Debug, Clone, Copy)]
pub enum Target {
    Exspeed,
    #[cfg(feature = "comparison")]
    Kafka,
}

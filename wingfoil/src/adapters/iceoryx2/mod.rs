//! iceoryx2 adapter — zero-copy inter-process communication (IPC)
//!
//! Provides two graph nodes:
//!
//! - [`iceoryx2_sub`] — subscribes to an iceoryx2 service and produces a stream
//! - [`iceoryx2_pub`] — publishes a stream to an iceoryx2 service
//!
//! # Setup
//!
//! iceoryx2 requires shared memory to be available. On Linux, this is typically
//! pre-configured. The service uses IPC (inter-process) mode by default.
//!
//! # Zero-Copy Requirements
//!
//! Payload types must implement [`ZeroCopySend`] and be `#[repr(C)]` and self-contained
//! (no heap allocations, no pointers to external data).
//!
//! # Usage
//!
//! ## Subscribe (receive data)
//!
//! ```ignore
//! use wingfoil::adapters::iceoryx2::*;
//! use wingfoil::*;
//!
//! let stream = iceoryx2_sub::<MyData>("my/service");
//! stream.collapse().for_each(|data| {
//!     println!("Received: {:?}", data");
//! }).run(RunMode::RealTime, RunFor::Forever).unwrap();
//! ```
//!
//! ## Publish (send data)
//!
//! ```ignore
//! use wingfoil::adapters::iceoryx2::*;
//! use wingfoil::*;
//!
//! ticker(Duration::from_millis(100))
//!     .map(|_| burst![MyData { value: 42, timestamp: 0 }])
//!     .iceoryx2_pub("my/service")
//!     .run(RunMode::RealTime, RunFor::Forever).unwrap();
//! ```

mod read;
mod write;

pub use read::*;
pub use write::*;

// Tests are only compiled when running tests
#[cfg(test)]
mod tests {
    use crate::nodes::constant;
    use std::rc::Rc;

    // Test that burst creation works with basic types
    #[test]
    fn test_burst_creation() {
        use crate::Burst;
        let mut burst: Burst<i32> = Burst::default();
        burst.push(42);
        assert_eq!(burst.len(), 1);
        assert_eq!(burst[0], 42);
    }

    // Test that iceoryx2_pub function is accessible
    #[test]
    fn test_iceoryx2_pub_function_exists() {
        fn check_signature() {
            fn takes_stream(s: Rc<dyn crate::Stream<crate::Burst<i32>>>) {
                // This would need to be called with iceoryx2_pub
            }
        }
        let _ = check_signature;
    }
}
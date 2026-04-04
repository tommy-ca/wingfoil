//! iceoryx2 publisher (write) implementation

use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::types::{Element, IntoNode, UpStreams};
use crate::{Burst, GraphState, MutableNode, Node, Stream};

use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;

/// Publish a `Burst<T>` stream to an iceoryx2 service.
///
/// # Type Parameters
/// - `T`: Must implement `ZeroCopySend`, `Clone`, `Copy`, `Debug`, `Default`, `'static`
///
/// # Arguments
/// - `upstream`: The stream to publish
/// - `service_name`: The iceoryx2 service name (e.g., "my/service")
///
/// # Returns
/// A node that publishes to the service.
pub fn iceoryx2_pub<T>(upstream: Rc<dyn Stream<Burst<T>>>, service_name: &str) -> Rc<dyn Node>
where
    T: Element + ZeroCopySend + Clone + Copy + Send + 'static,
{
    Iceoryx2Publisher::new(upstream, service_name.to_string()).into_node()
}

struct Iceoryx2Publisher<T>
where
    T: Element + ZeroCopySend + Clone + Copy + Send + 'static,
{
    upstream: Rc<dyn Stream<Burst<T>>>,
    service_name: String,
    publisher: Option<Publisher<ipc::Service, T, ()>>,
    running: Arc<AtomicBool>,
}

impl<T> Iceoryx2Publisher<T>
where
    T: Element + ZeroCopySend + Clone + Copy + Send + 'static,
{
    fn new(upstream: Rc<dyn Stream<Burst<T>>>, service_name: String) -> Self {
        Self {
            upstream,
            service_name,
            publisher: None,
            running: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl<T> MutableNode for Iceoryx2Publisher<T>
where
    T: Element + ZeroCopySend + Clone + Copy + Send + 'static,
{
    fn cycle(&mut self, _state: &mut GraphState) -> anyhow::Result<bool> {
        if let Some(publisher) = &self.publisher {
            let burst = self.upstream.peek_value();
            for data in burst {
                let sample = publisher.loan_uninit()?;
                let sample = sample.write_payload(data);
                sample.send()?;
            }
        }
        Ok(true)
    }

    fn upstreams(&self) -> UpStreams {
        UpStreams::new(vec![self.upstream.clone().as_node()], vec![])
    }

    fn setup(&mut self, _state: &mut GraphState) -> anyhow::Result<()> {
        Ok(())
    }

    fn start(&mut self, _state: &mut GraphState) -> anyhow::Result<()> {
        let node = NodeBuilder::new().create::<ipc::Service>()?;

        let service = node
            .service_builder(&self.service_name.as_str().try_into()?)
            .publish_subscribe::<T>()
            .open_or_create()?;

        self.publisher = Some(service.publisher_builder().create()?);

        Ok(())
    }

    fn stop(&mut self, _state: &mut GraphState) -> anyhow::Result<()> {
        self.running.store(false, Ordering::SeqCst);
        self.publisher = None;
        Ok(())
    }

    fn teardown(&mut self, _state: &mut GraphState) -> anyhow::Result<()> {
        Ok(())
    }
}

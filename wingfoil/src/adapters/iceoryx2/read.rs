//! iceoryx2 subscriber (read) implementation

use std::rc::Rc;
use std::thread;

use crate::channel::{channel_pair, ChannelReceiver, ChannelSender, Message};
use crate::types::{Element, IntoStream};
use crate::{Burst, Stream};

use iceoryx2::prelude::*;

/// Subscribe to an iceoryx2 service and produce a stream of samples.
///
/// # Type Parameters
/// - `T`: Must implement `ZeroCopySend`, `Clone`, `Copy`, `Debug`, `Default`, `'static`
///
/// # Arguments
/// - `service_name`: The iceoryx2 service name (e.g., "my/service")
///
/// # Returns
/// A stream that emits `Burst<T>` - batches of samples received since last cycle.
#[must_use]
pub fn iceoryx2_sub<T>(service_name: &str) -> Rc<dyn Stream<Burst<T>>>
where
    T: Element + ZeroCopySend + Clone + Copy + Send + 'static,
{
    let service_name = service_name.to_string();
    let (sender, receiver) = channel_pair(None);

    // Spawn a thread to run iceoryx2 subscriber
    thread::spawn(move || {
        if let Err(e) = run_subscriber_thread::<T>(service_name, sender) {
            log::error!("iceoryx2 subscriber error: {:?}", e);
        }
    });

    // Create a stream that receives T from the channel
    Iceoryx2ReceiverStream::new(receiver).into_stream()
}

struct Iceoryx2ReceiverStream<T: Element + Send> {
    receiver: ChannelReceiver<T>,
    value: Burst<T>,
    finished: bool,
}

impl<T: Element + Send> Iceoryx2ReceiverStream<T> {
    fn new(receiver: ChannelReceiver<T>) -> Self {
        Self {
            receiver,
            value: Burst::default(),
            finished: false,
        }
    }
}

impl<T: Element + Send> crate::MutableNode for Iceoryx2ReceiverStream<T> {
    fn cycle(&mut self, _state: &mut crate::GraphState) -> anyhow::Result<bool> {
        self.value.clear();

        loop {
            if self.finished {
                break;
            }
            match self.receiver.try_recv() {
                Some(Message::RealtimeValue(value)) => {
                    self.value.push(value);
                }
                Some(Message::EndOfStream) => {
                    self.finished = true;
                }
                Some(Message::Error(err)) => {
                    return Err(anyhow::anyhow!(err));
                }
                None => break,
                _ => {}
            }
        }

        Ok(!self.value.is_empty())
    }

    fn upstreams(&self) -> crate::UpStreams {
        crate::UpStreams::none()
    }
}

impl<T: Element + Send> crate::StreamPeekRef<Burst<T>> for Iceoryx2ReceiverStream<T> {
    fn peek_ref(&self) -> &Burst<T> {
        &self.value
    }
}

fn run_subscriber_thread<T>(
    service_name: String,
    channel_sender: ChannelSender<T>,
) -> anyhow::Result<()>
where
    T: Element + ZeroCopySend + Clone + Copy + Send + 'static,
{
    let node = NodeBuilder::new().create::<ipc::Service>()?;

    let service = node
        .service_builder(&service_name.as_str().try_into()?)
        .publish_subscribe::<T>()
        .open_or_create()?;

    let subscriber = service.subscriber_builder().create()?;

    loop {
        // Receive all available samples
        while let Ok(Some(sample)) = subscriber.receive() {
            let data = *sample;
            let _ = channel_sender.send_message(Message::RealtimeValue(data));
            drop(sample);
        }

        // Small delay to avoid busy loop
        thread::sleep(std::time::Duration::from_millis(1));
    }
}

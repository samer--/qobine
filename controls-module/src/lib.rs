use std::time::Duration;

use tokio::sync::{broadcast, watch};

use crate::tracklist::Tracklist;

pub mod controls;
pub mod models;
pub mod tracklist;

pub type PositionReceiver = watch::Receiver<Duration>;
pub type VolumeReceiver = watch::Receiver<f32>;
pub type AutoPlayReceiver = watch::Receiver<bool>;
pub type StatusReceiver = watch::Receiver<Status>;
pub type TracklistReceiver = watch::Receiver<Tracklist>;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Status {
    Playing,
    Buffering,
    #[default]
    Paused,
}

pub type ExitReceiver = broadcast::Receiver<bool>;
pub type ExitSender = broadcast::Sender<bool>;

pub fn spawn_ctrl_c_handler(exit_sender: &ExitSender) {
    let exit_sender = exit_sender.clone();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = exit_sender.send(true);
        }
    });
}

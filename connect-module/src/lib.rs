use std::sync::Arc;
use std::time::Duration;

use controls_module::{
    PositionReceiver, Status, StatusReceiver, TracklistReceiver, VolumeReceiver,
    controls::{Controls, NewQueueItem},
    tracklist::Tracklist,
};
use player_module::{
    AppResult, AudioQuality, client::StreamClient, database::Database, error::PlayerError,
};
use qconnect_protocol::{
    QueueCommand, QueueCommandType, QueueEventType, QueueServerEvent, QueueVersion,
    RendererCommandType, RendererReport, RendererReportType, RendererServerCommand,
    build_qconnect_outbound_envelope, build_qconnect_renderer_outbound_envelope,
};
use qconnect_transport_ws::{
    NativeWsTransport, TransportEvent, WsTransport, WsTransportConfig, WsTransportError,
};
use serde_json::{Value, json};
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;

const PLAYING_STATE_STOPPED: i64 = 1;
const PLAYING_STATE_PLAYING: i64 = 2;
const PLAYING_STATE_PAUSED: i64 = 3;

const BUFFER_STATE_BUFFERING: i32 = 1;
const BUFFER_STATE_OK: i32 = 2;

const DEVICE_TYPE_COMPUTER: i32 = 5;

const SUBSCRIBE_CHANNELS: [u8; 3] = [0x01, 0x02, 0x03];

struct ConnectState {
    controls: Controls,
    position_receiver: PositionReceiver,
    tracklist_receiver: TracklistReceiver,
    status_receiver: StatusReceiver,
    volume_receiver: VolumeReceiver,
    max_audio_quality: i32,
    queue_version: QueueVersion,
    connect_name: String,
    device_uuid: String,
    session_uuid: Option<String>,
    renderer_joined: bool,
    is_active: bool,
    last_pushed_queue: Option<Vec<u32>>,
}

pub async fn init(
    client: Arc<StreamClient>,
    database: Arc<Database>,
    connect_name: String,
    controls: Controls,
    position_receiver: PositionReceiver,
    tracklist_receiver: TracklistReceiver,
    status_receiver: StatusReceiver,
    volume_receiver: VolumeReceiver,
    max_audio_quality: AudioQuality,
) -> AppResult<()> {
    let (endpoint, jwt) = client.create_qws_token().await?;

    let device_uuid = resolve_device_uuid(&database).await?;

    let mut state = ConnectState {
        controls,
        position_receiver,
        tracklist_receiver,
        status_receiver,
        volume_receiver,
        max_audio_quality: convert_audio_quality(max_audio_quality),
        queue_version: QueueVersion::new(1, 0),
        connect_name,
        device_uuid,
        session_uuid: None,
        renderer_joined: false,
        is_active: false,
        last_pushed_queue: None,
    };

    state.run(&endpoint, &jwt).await
}

async fn resolve_device_uuid(database: &Database) -> AppResult<String> {
    if let Some(uuid) = database.get_connect_device_uuid().await? {
        return Ok(uuid);
    }

    let uuid = Uuid::new_v4().to_string();
    database.set_connect_device_uuid(&uuid).await?;
    Ok(uuid)
}

const fn convert_audio_quality(max_audio_quality: AudioQuality) -> i32 {
    match max_audio_quality {
        AudioQuality::Mp3 => 1,
        AudioQuality::CD => 2,
        AudioQuality::HIFI96 => 3,
        AudioQuality::HIFI192 => 4,
    }
}

fn convert_volume(volume: f32) -> u32 {
    (volume * 100.0).clamp(0.0, 100.0).round() as u32
}

fn device_info_json(friendly_name: &str, device_uuid: &str, max_audio_quality: i32) -> Value {
    json!({
        "device_uuid": device_uuid,
        "friendly_name": friendly_name,
        "brand": "qobine",
        "model": "qobine",
        "serial_number": null,
        "device_type": DEVICE_TYPE_COMPUTER,
        "capabilities": {
            "min_audio_quality": 1,
            "max_audio_quality": max_audio_quality,
            "volume_remote_control": 2,
        },
        "software_version": format!("qobine/{}", env!("CARGO_PKG_VERSION")),
    })
}

fn parse_queue_items(payload: &Value, key: &str) -> Vec<NewQueueItem> {
    let Some(tracks) = payload.get(key).and_then(Value::as_array) else {
        return Vec::new();
    };

    tracks
        .iter()
        .filter_map(|track| {
            let track_id = track.get("track_id").and_then(Value::as_u64)?;
            let queue_id = track
                .get("queue_item_id")
                .and_then(Value::as_u64)
                .unwrap_or(0);

            Some(NewQueueItem {
                track_id: u32::try_from(track_id).ok()?,
                queue_id,
            })
        })
        .collect()
}

fn state_report_payload(
    status: Status,
    position: &Duration,
    tracklist: &Tracklist,
    queue_version: QueueVersion,
) -> Value {
    let playing_state = match status {
        Status::Playing => PLAYING_STATE_PLAYING,
        Status::Buffering | Status::Paused => PLAYING_STATE_PAUSED,
    };

    let buffer_state = match status {
        Status::Playing | Status::Paused => BUFFER_STATE_OK,
        Status::Buffering => BUFFER_STATE_BUFFERING,
    };

    let position_ms = u64::try_from(position.as_millis()).ok();
    let duration_ms = tracklist
        .current_track()
        .map(|x| u64::from(x.duration_seconds).saturating_mul(1000));

    json!({
        "playing_state": playing_state,
        "buffer_state": buffer_state,
        "current_position": position_ms,
        "duration": duration_ms,
        "current_queue_item_id": tracklist.current_queue_id(),
        "next_queue_item_id": tracklist.next_track_queue_id(),
        "queue_version": {
            "major": queue_version.major,
            "minor": queue_version.minor,
        },
    })
}

fn map_ws_err(err: &WsTransportError) -> PlayerError {
    PlayerError::ConnectError {
        error: err.to_string(),
    }
}

impl ConnectState {
    async fn run(&mut self, endpoint: &str, jwt: &str) -> AppResult<()> {
        let mut config = WsTransportConfig::default();
        config.endpoint_url = endpoint.to_string();
        config.jwt_qws = Some(jwt.to_string());
        config.require_jwt = true;
        config.reconnect_idle_retry_ms = 60_000;
        config.subscribe_channels = SUBSCRIBE_CHANNELS.iter().map(|c| vec![*c]).collect();

        let transport = NativeWsTransport::new();
        let mut events = transport.subscribe();

        transport.connect(config).await.map_err(|err| map_ws_err(&err))?;

        self.send_controller_join(&transport).await?;

        let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                event = events.recv() => {
                    match event {
                        Ok(event) => self.handle_transport_event(&transport, event).await,
                        Err(RecvError::Lagged(_)) => {}
                        Err(RecvError::Closed) => break,
                    }
                }
                Ok(()) = self.position_receiver.changed() => {
                    self.report_state(&transport).await;
                }
                Ok(()) = self.tracklist_receiver.changed() => {
                    self.publish_local_queue_if_changed(&transport).await;
                    self.report_state(&transport).await;
                }
                Ok(()) = self.status_receiver.changed() => {
                    self.report_state(&transport).await;
                }
                Ok(()) = self.volume_receiver.changed() => {
                    self.report_volume(&transport).await;
                }
                _ = heartbeat.tick() => {
                    self.report_state(&transport).await;
                }
            }
        }

        Ok(())
    }

    async fn handle_transport_event(&mut self, transport: &NativeWsTransport, event: TransportEvent) {
        match event {
            TransportEvent::InboundQueueServerEvent(event) => {
                self.handle_queue_event(transport, event).await;
            }
            TransportEvent::InboundRendererServerCommand(command) => {
                self.handle_renderer_command(transport, command).await;
            }
            TransportEvent::SessionEstablished => {
                tracing::info!("Qobuz Connect session established");
            }
            TransportEvent::Disconnected => {
                tracing::info!("Qobuz Connect disconnected");
            }
            TransportEvent::CloudError { code, .. } => {
                tracing::warn!("Qobuz cloud error: code={code}");
            }
            TransportEvent::MaxReconnectAttemptsExceeded {
                attempts,
                last_reason,
            } => {
                tracing::warn!("Qobuz Connect reconnect attempts exceeded ({attempts}): {last_reason}");
            }
            _ => {}
        }
    }

    async fn handle_queue_event(&mut self, transport: &NativeWsTransport, event: QueueServerEvent) {
        if let Some(queue_version) = event.queue_version {
            self.queue_version = queue_version;
        }

        match event.event_type {
            QueueEventType::SrvrCtrlSessionState => {
                if let Some(session_uuid) = event.payload.get("session_uuid").and_then(Value::as_str) {
                    if self.session_uuid.as_deref() != Some(session_uuid) {
                        self.session_uuid = Some(session_uuid.to_string());
                        self.send_renderer_join(transport, session_uuid).await;
                    }
                }
            }
            QueueEventType::SrvrCtrlQueueState => {
                let items = parse_queue_items(&event.payload, "tracks");
                let incoming_ids: Vec<u32> = items.iter().map(|item| item.track_id).collect();
                self.last_pushed_queue = Some(incoming_ids);
                self.controls.new_queue(items, false, None);
            }
            QueueEventType::SrvrCtrlQueueTracksLoaded => {
                let items = parse_queue_items(&event.payload, "tracks");
                let incoming_ids: Vec<u32> = items.iter().map(|item| item.track_id).collect();
                let is_echo = self.last_pushed_queue.as_deref() == Some(incoming_ids.as_slice());
                self.last_pushed_queue = Some(incoming_ids);
                let start_index = event
                    .payload
                    .get("queue_position")
                    .and_then(Value::as_u64)
                    .and_then(|x| usize::try_from(x).ok());
                self.controls.new_queue(items, false, start_index);
                if !is_echo {
                    self.controls.play();
                }
            }
            QueueEventType::SrvrCtrlQueueCleared => {
                self.controls.clear_queue();
            }
            _ => {}
        }
    }

    async fn handle_renderer_command(
        &mut self,
        transport: &NativeWsTransport,
        command: RendererServerCommand,
    ) {
        match command.command_type {
            RendererCommandType::SrvrRndrSetState => {
                let playing_state = command.payload.get("playing_state").and_then(Value::as_i64);

                match playing_state {
                    Some(PLAYING_STATE_PLAYING) => self.controls.play(),
                    Some(PLAYING_STATE_STOPPED) | Some(PLAYING_STATE_PAUSED) => self.controls.pause(),
                    _ => {}
                }

                if let Some(position_ms) = command
                    .payload
                    .get("current_position")
                    .and_then(Value::as_u64)
                {
                    self.controls.seek(Duration::from_millis(position_ms));
                }

                if let Some(target_queue_id) = command
                    .payload
                    .get("current_track")
                    .and_then(|t| t.get("queue_item_id"))
                    .and_then(Value::as_u64)
                {
                    let tracklist = self.tracklist_receiver.borrow().clone();
                    if tracklist.current_queue_id() != Some(target_queue_id) {
                        let index = tracklist
                            .queue()
                            .iter()
                            .position(|item| item.queue_id == target_queue_id);

                        if let Some(index) = index {
                            self.controls.skip_to_position(index, true);
                        }
                    }
                }

                self.report_state(transport).await;
            }
            RendererCommandType::SrvrRndrSetVolume => {
                if let Some(volume) = command.payload.get("volume").and_then(Value::as_u64) {
                    let volume = (volume as f32 / 100.0).clamp(0.0, 1.0);
                    self.controls.set_volume(volume);
                }

                self.report_volume(transport).await;
            }
            RendererCommandType::SrvrRndrSetActive => {
                let active = command
                    .payload
                    .get("active")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);

                tracing::info!("Qobuz Connect active renderer: {active}");
                self.is_active = active;

                if active {
                    self.publish_local_queue_if_changed(transport).await;
                    self.report_state(transport).await;
                    self.report_volume(transport).await;
                    self.report_max_quality(transport).await;
                }
            }
            _ => {}
        }
    }

    async fn publish_local_queue_if_changed(&mut self, transport: &NativeWsTransport) {
        if !self.renderer_joined || !self.is_active {
            return;
        }

        let tracklist = self.tracklist_receiver.borrow().clone();
        let track_ids: Vec<u32> = tracklist.queue().iter().map(|item| item.track.id).collect();

        if track_ids.is_empty() {
            return;
        }

        if self.last_pushed_queue.as_deref() == Some(track_ids.as_slice()) {
            return;
        }

        let start_index = tracklist.current_position();

        let command = QueueCommand::new(
            QueueCommandType::CtrlSrvrQueueLoadTracks,
            Uuid::new_v4().to_string(),
            self.queue_version,
            json!({
                "track_ids": track_ids,
                "queue_position": start_index,
                "shuffle_mode": false,
                "shuffle_pivot_index": start_index,
                "context_uuid": Uuid::new_v4().to_string(),
                "autoplay_reset": true,
                "autoplay_loading": false,
            }),
        );

        let envelope = match build_qconnect_outbound_envelope(command) {
            Ok(envelope) => envelope,
            Err(err) => {
                tracing::warn!("Failed to build queue publish: {err}");
                return;
            }
        };

        let count = track_ids.len();
        if let Err(err) = transport.send(envelope).await {
            tracing::warn!("Failed to publish queue: {err}");
            return;
        }

        self.last_pushed_queue = Some(track_ids);
        tracing::info!("Published local queue to Qobuz Connect ({count} tracks)");
    }

    async fn send_controller_join(&self, transport: &NativeWsTransport) -> AppResult<()> {
        let device_info = device_info_json(
            &self.connect_name,
            &self.device_uuid,
            self.max_audio_quality,
        );

        let command = QueueCommand::new(
            QueueCommandType::CtrlSrvrJoinSession,
            Uuid::new_v4().to_string(),
            self.queue_version,
            json!({ "device_info": device_info }),
        );

        let envelope = build_qconnect_outbound_envelope(command)
            .map_err(|err| PlayerError::ConnectError { error: err.to_string() })?;

        transport.send(envelope).await.map_err(|err| map_ws_err(&err))?;

        tracing::info!("Qobuz Connect controller joined");
        Ok(())
    }

    async fn send_renderer_join(&mut self, transport: &NativeWsTransport, session_uuid: &str) {
        if self.renderer_joined {
            return;
        }

        let device_info = device_info_json(
            &self.connect_name,
            &self.device_uuid,
            self.max_audio_quality,
        );

        let queue_version = self.queue_version;

        let report = RendererReport::new(
            RendererReportType::RndrSrvrJoinSession,
            Uuid::new_v4().to_string(),
            queue_version,
            json!({
                "session_uuid": session_uuid,
                "device_info": device_info,
                "is_active": false,
                "reason": 0,
                "initial_state": {
                    "playing_state": PLAYING_STATE_STOPPED,
                    "buffer_state": BUFFER_STATE_OK,
                    "current_position": 0,
                    "duration": 0,
                    "queue_version": {
                        "major": queue_version.major,
                        "minor": queue_version.minor,
                    },
                },
            }),
        );

        let envelope = match build_qconnect_renderer_outbound_envelope(report) {
            Ok(envelope) => envelope,
            Err(err) => {
                tracing::warn!("Failed to build renderer join: {err}");
                return;
            }
        };

        if let Err(err) = transport.send(envelope).await {
            tracing::warn!("Failed to send renderer join: {err}");
            return;
        }

        self.renderer_joined = true;
        tracing::info!("Qobuz Connect renderer joined session {session_uuid}");

        self.report_state(transport).await;
        self.report_volume(transport).await;
        self.report_max_quality(transport).await;
    }

    async fn send_report(
        &self,
        transport: &NativeWsTransport,
        report_type: RendererReportType,
        payload: Value,
    ) {
        let report = RendererReport::new(
            report_type,
            Uuid::new_v4().to_string(),
            self.queue_version,
            payload,
        );

        let envelope = match build_qconnect_renderer_outbound_envelope(report) {
            Ok(envelope) => envelope,
            Err(err) => {
                tracing::warn!("Failed to build renderer report: {err}");
                return;
            }
        };

        if let Err(err) = transport.send(envelope).await {
            tracing::warn!("Failed to send renderer report: {err}");
        }
    }

    async fn report_state(&self, transport: &NativeWsTransport) {
        if !self.renderer_joined {
            return;
        }

        let position = { *self.position_receiver.borrow() };
        let status = { *self.status_receiver.borrow() };
        let tracklist = self.tracklist_receiver.borrow().clone();

        let payload = state_report_payload(status, &position, &tracklist, self.queue_version);
        self.send_report(transport, RendererReportType::RndrSrvrStateUpdated, payload)
            .await;
    }

    async fn report_volume(&self, transport: &NativeWsTransport) {
        if !self.renderer_joined {
            return;
        }

        let volume = convert_volume(*self.volume_receiver.borrow());
        self.send_report(
            transport,
            RendererReportType::RndrSrvrVolumeChanged,
            json!({ "volume": volume }),
        )
        .await;
    }

    async fn report_max_quality(&self, transport: &NativeWsTransport) {
        if !self.renderer_joined {
            return;
        }

        self.send_report(
            transport,
            RendererReportType::RndrSrvrMaxAudioQualityChanged,
            json!({ "max_audio_quality": self.max_audio_quality }),
        )
        .await;
    }
}

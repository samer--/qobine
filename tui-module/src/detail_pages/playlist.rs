use controls_module::{controls::Controls, models::Playlist};
use player_module::{AppResult, client::StreamClient};
use ratatui::{crossterm::event::KeyCode, prelude::*, widgets::Paragraph};

use crate::{
    app::{FavoriteIds, NotificationList, Output},
    ui::{block, format_seconds, tab_bar},
    widgets::track_list::{TrackList, TrackListEvent},
};

pub struct PlaylistOverlay {
    shuffle: bool,
    tracks: TrackList,
    title: String,
    id: u32,
    is_owned: bool,
    owner: String,
    duration_seconds: u32,
}

impl PlaylistOverlay {
    pub fn new(playlist: Playlist) -> Self {
        Self {
            shuffle: false,
            tracks: TrackList::new(playlist.tracks),
            title: playlist.title,
            id: playlist.id,
            is_owned: playlist.is_owned,
            owner: playlist.owner.name,
            duration_seconds: playlist.duration_seconds,
        }
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, favorites: &FavoriteIds) {
        let header_height = 4;
        let outer_block = block(Some(&self.title));

        frame.render_widget(&outer_block, area);

        let inner = outer_block.inner(area);

        let [header_area, tracks_area, controls_area] = Layout::vertical([
            Constraint::Length(header_height),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .areas(inner);

        self.render_header(frame, header_area);

        self.tracks.render(
            tracks_area,
            frame.buffer_mut(),
            true,
            true,
            favorites.tracks(),
        );

        let selected_control = usize::from(self.shuffle);

        let controls = tab_bar(["Play", "Shuffle"].into(), selected_control);

        frame.render_widget(controls, controls_area);
    }

    pub async fn handle_event(
        &mut self,
        code: KeyCode,
        client: &StreamClient,
        controls: &Controls,
        notifications: &mut NotificationList,
    ) -> AppResult<Output> {
        match code {
            KeyCode::Left | KeyCode::Right | KeyCode::Char('h' | 'l') => {
                self.shuffle = !self.shuffle;
                Ok(Output::Consumed)
            }

            KeyCode::Char('D') => {
                self.delete_selected_track(client).await?;
                Ok(Output::Consumed)
            }

            KeyCode::Char('u') => {
                self.move_selected_track_up(client).await?;
                Ok(Output::Consumed)
            }

            KeyCode::Char('d') => {
                self.move_selected_track_down(client).await?;
                Ok(Output::Consumed)
            }

            KeyCode::Esc => Ok(Output::PopOverlay),

            _ => {
                self.tracks
                    .handle_events(
                        code,
                        client,
                        controls,
                        notifications,
                        TrackListEvent::Playlist(self.id, self.shuffle),
                    )
                    .await
            }
        }
    }

    fn render_header(&mut self, frame: &mut Frame, area: Rect) {
        let information = vec![
            Line::from(Span::styled(self.title.clone(), Style::new().bold())),
            Line::from(self.owner.clone()),
            Line::default(),
            Line::from(Span::styled(
                format!(
                    "{} tracks · {}",
                    self.tracks.filter().len(),
                    format_seconds(self.duration_seconds),
                ),
                Style::new().dim(),
            )),
        ];

        frame.render_widget(Paragraph::new(information), area);
    }

    async fn delete_selected_track(&mut self, client: &StreamClient) -> AppResult<()> {
        if !self.is_owned {
            return Ok(());
        }

        let Some(index) = self.tracks.selected() else {
            return Ok(());
        };

        let Some(playlist_track_id) = self
            .tracks
            .get(index)
            .and_then(|track| track.playlist_track_id)
        else {
            return Ok(());
        };

        client
            .playlist_delete_track(self.id, &[playlist_track_id])
            .await?;

        self.tracks.remove_at_index(index);

        Ok(())
    }

    async fn move_selected_track_up(&mut self, client: &StreamClient) -> AppResult<()> {
        if !self.is_owned {
            return Ok(());
        }

        let Some(index) = self.tracks.selected() else {
            return Ok(());
        };

        // Prevent `index - 1` from underflowing.
        let Some(new_index) = index.checked_sub(1) else {
            return Ok(());
        };

        self.move_selected_track(index, new_index, client).await
    }

    async fn move_selected_track_down(&mut self, client: &StreamClient) -> AppResult<()> {
        if !self.is_owned {
            return Ok(());
        }

        let Some(index) = self.tracks.selected() else {
            return Ok(());
        };

        let Some(new_index) = index.checked_add(1) else {
            return Ok(());
        };

        // Prevent moving beyond the final track.
        if new_index >= self.tracks.filter().len() {
            return Ok(());
        }

        self.move_selected_track(index, new_index, client).await
    }

    async fn move_selected_track(
        &mut self,
        current_index: usize,
        new_index: usize,
        client: &StreamClient,
    ) -> AppResult<()> {
        let Some(playlist_track_id) = self
            .tracks
            .get(current_index)
            .and_then(|track| track.playlist_track_id)
        else {
            return Ok(());
        };

        client
            .update_playlist_track_position(new_index, self.id, playlist_track_id)
            .await?;

        self.tracks
            .move_index_to_new_index(current_index, new_index);

        self.tracks.select_index(new_index);

        Ok(())
    }
}

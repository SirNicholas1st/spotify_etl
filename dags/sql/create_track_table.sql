CREATE TABLE IF NOT EXISTS track_table (
    play_id int IDENTITY(1, 1),
    album_id int, 
    played_at TIMESTAMP(6),
    track varchar(255),
    track_len_s int,
    FOREIGN KEY (album_id) REFERENCES album_table(album_id)
);
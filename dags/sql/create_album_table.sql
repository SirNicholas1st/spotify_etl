CREATE TABLE IF NOT EXISTS album_table (
    album_id int IDENTITY(1, 1) PRIMARY KEY,
    artist_id int,
    album varchar(255),
    album_release_date DATE,
    album_total_tracks int,
    FOREIGN KEY (artist_id) REFERENCES artist_table(artist_id)
);
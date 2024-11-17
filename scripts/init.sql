CREATE DATABASE music;
\c music;

-- Table for tracks
CREATE TABLE tracks (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    artist VARCHAR(255) NOT NULL,
    songwriters TEXT,
    duration VARCHAR(50),
    genres TEXT,
    album VARCHAR(255),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Table for users
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    gender VARCHAR(50),
    favorite_genres TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Table for listen_history
CREATE TABLE listen_history (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    track_id INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (track_id) REFERENCES tracks(id)
);

-- Create indexes for better query performance
CREATE INDEX idx_tracks_artist ON tracks(artist);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_listen_history_user_id ON listen_history(user_id);
CREATE INDEX idx_listen_history_track_id ON listen_history(track_id);
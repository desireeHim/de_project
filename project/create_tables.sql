CREATE TABLE IF NOT EXISTS article ( 
    ID SERIAL PRIMARY KEY,
    article_id VARCHAR(25),
    title VARCHAR(255), 
    doi VARCHAR(100),
    update_date VARCHAR(25),
    journal_ref VARCHAR(100),
    category_id VARCHAR(100),
    url VARCHAR(150),
    UNIQUE (article_id, update_date)
);

CREATE TABLE IF NOT EXISTS category ( 
    ID SERIAL PRIMARY KEY,
    name VARCHAR(255),
    UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS author (
    ID SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(100),
    UNIQUE (first_name, last_name)
);

CREATE TABLE IF NOT EXISTS article_author ( 
    ID SERIAL PRIMARY KEY,
    article_id VARCHAR(100),
    author_id VARCHAR(100),
    UNIQUE (article_id, author_id)
);

CREATE TABLE IF NOT EXISTS article ( 
    ID SERIAL PRIMARY KEY,
    article_id VARCHAR(25),
    title VARCHAR(255), 
    doi VARCHAR(255),
    update_date VARCHAR(25),
    journal_ref VARCHAR(255),
    category_id INTEGER,
    journal_id INTEGER,
    url VARCHAR(150),
    type VARCHAR(150),
    reference_count INTEGER,
    is_referenced_by_count INTEGER,
    references_doi VARCHAR(150) ARRAY,
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
    author_id INTEGER,
    UNIQUE (article_id, author_id)
);

CREATE TABLE IF NOT EXISTS journal (
    ID SERIAL PRIMARY KEY,
    issn VARCHAR(100),
    name VARCHAR(255),
    UNIQUE (issn)
)

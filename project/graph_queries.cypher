// Co-authorship prediction

MATCH (author1:Author)<-[:AUTHORED_BY]-(a:Article)-[:AUTHORED_BY]->(author2:Author)
WHERE
author1 <> author2 // ensuring that authors are different
// removing symmetric paire: keep (Adams, Brown), ignore (Brown, Adams)
AND author1.last_name < author2.last_name 
WITH (author1.first_name + " " + author1.last_name) AS author1_name,
     (author2.first_name + " " + author2.last_name) AS author2_name,
     COUNT(DISTINCT a) AS articles_coauthored
WHERE articles_coauthored > 1
RETURN author1_name, author2_name, articles_coauthored
ORDER BY articles_coauthored DESC;

// Community analysis

// create a GDS graph with all the Author and Article nodes as well as an AUTHORED_BY relationship between them

CALL gds.graph.project(
  'article_author',
  ['Article', 'Author'],
  {
    AUTHORED_BY: {
      type: 'AUTHORED_BY',
      orientation: 'UNDIRECTED'
    }
  }
);


// execute a Louvain algorithm and set the ‘communityId’ property of Author nodes with the resulting Community IDs

CALL gds.louvain.stream('article_author')
YIELD nodeId, communityId
WITH nodeId, communityId
MATCH (a) WHERE id(a) = nodeId
SET a.communityId = communityId;

// retrieve a list of top-5 author communities by a number of authors in them

MATCH (author:Author)
WITH author.communityId as communityId
RETURN communityId, count(*) as authors_count
ORDER BY authors_count desc
LIMIT 5;


// an example of one of the biggest communities 

MATCH (article:Article)-[:AUTHORED_BY]->(author:Author)
WHERE author.communityId = 15364
RETURN *;


// which category most authors belong to

MATCH (c:Category)<-[:BELONGS_TO]-(article:Article)-[:AUTHORED_BY]->(author:Author)
WHERE author.communityId = 15364
RETURN c.name as category, count(distinct article.article_id) as articles_count;
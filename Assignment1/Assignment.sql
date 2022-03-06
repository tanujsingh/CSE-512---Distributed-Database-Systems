
/*
Creating the table and loading the dataset
*/
DROP TABLE IF EXISTS ratings;
CREATE TABLE ratings (userid INT, temp1 VARCHAR(10),  movieid INT , temp3 VARCHAR(10),  rating REAL, temp5 VARCHAR(10), timestamp INT);
COPY ratings FROM 'test_data1.txt' DELIMITER ':';
ALTER TABLE ratings DROP COLUMN temp1, DROP COLUMN temp3, DROP COLUMN temp5 DROP COLUMN timestamp;
-- Do not change the above code except the path to the dataset.
-- make sure to change the path back to default provided path before you submit it.

-- Part A
/* Write the queries for Part A*/

SELECT * FROM ratings;

SELECT * FROM ratings 
         WHERE rating>=2 AND rating<=3;

SELECT userid, movieid FROM ratings WHERE rating = 4 
         ORDER BY userid DESC;

SELECT * FROM ratings 
         WHERE rating = (SELECT MAX(rating) FROM ratings);

SELECT userid, COUNT(rating) AS reviews FROM ratings 
                           GROUP BY (userid) ORDER BY userid ASC;
-- DELETE FROM ratings WHERE rating <=1;
-- INSERT INTO ratings(userid, movieid, rating) VALUES (2, 220, 4.5);

-- Part B
/* Create the fragmentations for Part B1 */

DROP TABLE IF EXISTS F1;

CREATE TABLE F1 AS 
SELECT * FROM ratings WHERE rating<=3;

DROP TABLE IF EXISTS F2;

CREATE TABLE F2 AS 
SELECT * FROM ratings WHERE rating>=2.5 AND rating<4;

DROP TABLE IF EXISTS F3;

CREATE TABLE F3 AS 
SELECT * FROM ratings WHERE rating>=4;

/* Write reconstruction query/queries for Part B1 */

DROP TABLE IF EXISTS ratings;

CREATE TABLE ratings AS SELECT userid, movieid, rating
   FROM F1
   UNION
   SELECT userid, movieid, rating
   FROM F2
   UNION
   SELECT userid, movieid, rating
   FROM F3
   ORDER BY 1 ASC;

/* Write your explanation as a comment */
/* 
1) The fragmentation of the table is done horizontally with rating as the criteria of fragmentation.
2) The rows are divided such that  - 
   a) F1 contains all ratings less than 3 (3 inclusive). 
   b) F2 contains all ratings between 2.5 (2.5 inclusive) and 4 (non-inclusive). 
   c) F3 is contains all ratings more than 4. 
3) This is allow completeness since all values in ratings can also be found in F1 and F2 and F3 (combined). 
4) The ratings table can be reconstructed from F1, F2 and F3 on using Union operator on F1, F2 and F3.
5) But since data points in F1 and F2 overlap for rating = 2.5, disjoint property is not satisfied.
*/

/* Create the fragmentations for Part B2 */

DROP TABLE IF EXISTS F1;

CREATE TABLE F1 AS 
SELECT userid FROM ratings;

DROP TABLE IF EXISTS F2;

CREATE TABLE F2 AS 
SELECT movieid FROM ratings;

DROP TABLE IF EXISTS F3;

CREATE TABLE F3 AS 
SELECT userid, rating FROM ratings;

/* Write your explanation as a comment */
/* 
1) The fragmentation of the table is done vertically with taking userid as one fragment, movieid as one fragment.
2) The rows are divided such that  - 
   a) F1 contains list of all userid from ratings table. 
   b) F2 contains list of all movieid from ratings table. 
   c) F3 is contains userid and rating from ratings table.
3) This is allow completeness since all values in ratings can also be found in F1 and F2 and F3 (combined).
4) Disjoint property is also satisfied since no two data points are present in F1,F2 and F3.
5) But since their is no common attribute (key) and relational operator to join the table, reconstruction won't be possible.
*/
/* Create the fragmentations for Part B3 */

DROP TABLE IF EXISTS F1;

CREATE TABLE F1 AS 
SELECT * FROM ratings WHERE rating<=1.5;

DROP TABLE IF EXISTS F2;

CREATE TABLE F2 AS 
SELECT * FROM ratings WHERE rating>1.5 AND rating<=3;

DROP TABLE IF EXISTS F3;

CREATE TABLE F3 AS 
SELECT * FROM ratings WHERE rating>3;

/* Write reconstruction query/queries for Part B3 */

DROP TABLE IF EXISTS ratings;

CREATE TABLE ratings AS 
   SELECT * FROM F1
   UNION
   SELECT * FROM F2
   UNION
   SELECT * FROM F3
   ORDER BY 1 ASC;

/* Write your explanation as a comment */
/*
1) The fragmentation of the table is done horizontally with rating as the criteria of fragmentation.
2) The rows are divided such that 
   a) F1 contains all ratings less than 1.5 (1.5 inclusive). 
   b) F2 contains all ratings between 1.5 (1.5 non-inclusive) and 3 (3 inclusive).
   c) The third fragment F3 contains all ratings more than 3.
3) This is allow completeness since all values in ratings can also be found in f1 and f2 and f3 (combined).
4) The ratings table can be reconstructed from f1, f2 and f3 on using Union operator on f1, f2 and f3.
5) Also, there is no overlap of the fragmented tables, disjoint property is also satisfied.

*/
-- Part C
/* Write the queries for Part C */

-- Queries for Fragment F1
SELECT * FROM F1;

SELECT * FROM F1 WHERE rating>=1 AND rating<=1.5;

SELECT userid, movieid FROM F1 WHERE rating = 1 
         ORDER BY userid DESC;

SELECT * FROM F1 
         WHERE rating = (SELECT MAX(rating) FROM F1);

SELECT userid, COUNT(rating) AS reviews FROM F1 
                           GROUP BY (userid) ORDER BY userid ASC;

-- Queries for Fragment F2
SELECT * FROM F2;

SELECT * FROM F2 WHERE rating>=1.5 AND rating<3;

SELECT userid, movieid FROM F2 WHERE rating = 2.5 
         ORDER BY userid DESC;

SELECT * FROM F2 
         WHERE rating = (SELECT MAX(rating) FROM F2);

SELECT userid, COUNT(rating) AS reviews FROM F2 
                           GROUP BY (userid) ORDER BY userid ASC;

-- Queries for Fragment F3
SELECT * FROM F3;

SELECT * FROM F3 WHERE rating>=3.5 AND rating<5;

SELECT userid, movieid FROM F3 WHERE rating = 5 
         ORDER BY userid DESC;

SELECT * FROM F3 
         WHERE rating = (SELECT MAX(rating) FROM F3);

SELECT userid, COUNT(rating) AS reviews FROM F3 
                           GROUP BY (userid) ORDER BY userid ASC;

------ PART C - ALTERNATE WAY -------
-- Running Queries on Fragments to get the same output as PART A (Lower Network Cost)

-- Query1
SELECT * FROM (
	SELECT * FROM F1
	UNION
	SELECT * FROM F2
	UNION 
	SELECT * FROM F3
	) q;

--Query2
SELECT * FROM (
	SELECT * FROM F1 WHERE rating>=2 AND rating<=3
	UNION
	SELECT * FROM F2 WHERE rating>=2 AND rating<=3
	UNION
	SELECT * FROM F3 WHERE rating>=2 AND rating<=3
	) q ;
	
--Query3
SELECT userid, movieid FROM (
	SELECT userid, movieid FROM F1 WHERE rating = 4
	UNION
	SELECT userid, movieid FROM F2 WHERE rating = 4
	UNION
	SELECT userid, movieid FROM F3 WHERE rating = 4
) q ORDER BY userid DESC;

--Query4
SELECT * FROM (
	SELECT * FROM F1 WHERE rating = (SELECT MAX(rating) FROM F1)
	UNION
	SELECT * FROM F2 WHERE rating = (SELECT MAX(rating) FROM F2)
	UNION
	SELECT * FROM F3 WHERE rating = (SELECT MAX(rating) FROM F3)
	) q WHERE rating = 5;
	
--Query5
SELECT userid, SUM(reviews) AS reviews FROM (
	SELECT userid, COUNT(rating) as reviews FROM F1 GROUP BY userid
	UNION
	SELECT userid, COUNT(rating) as reviews FROM F2 GROUP BY userid
	UNION
	SELECT userid, COUNT(rating) as reviews FROM F3 GROUP BY userid
	) q GROUP BY userid ORDER BY userid ASC;

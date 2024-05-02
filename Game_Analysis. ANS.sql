
use game_analysis;

-- Problem Statement - Game Analysis dataset
-- 1) Players play a game divided into 3-levels (L0,L1 and L2)
-- 2) Each level has 3 difficulty levels (Low,Medium,High)
-- 3) At each level,players have to kill the opponents using guns/physical fight
-- 4) Each level has multiple stages at each difficulty level.
-- 5) A player can only play L1 using its system generated L1_code.
-- 6) Only players who have played Level1 can possibly play Level2 
--    using its system generated L2_code.
-- 7) By default a player can play L0.
-- 8) Each player can login to the game using a Dev_ID.
-- 9) Players can earn extra lives at each stage in a level.

alter table pd modify L1_Status varchar(30);
alter table pd modify L2_Status varchar(30);
alter table pd modify P_ID int primary key;
alter table pd drop myunknowncolumn;

alter table ld drop myunknowncolumn;
alter table ld change timestamp start_datetime datetime;
alter table ld modify Dev_Id varchar(10);
alter table ld modify Difficulty varchar(15);
alter table ld add primary key(P_ID,Dev_id,start_datetime);

-- pd (P_ID,PName,L1_status,L2_Status,L1_code,L2_Code)
-- ld (P_ID,Dev_ID,start_time,stages_crossed,level,difficulty,kill_count,
-- headshots_count,score,lives_earned)


-- Q1) Extract P_ID,Dev_ID,PName and Difficulty_level of all players 
-- at level 0
SELECT 
level_details2.P_ID,
level_details2.Dev_Id,
player_details.PName,
level_details2.Difficulty
FROM 
game_analysis.level_details2
INNER JOIN player_details
ON
player_details.P_ID =level_details2.P_ID
WHERE level_details2.level = 0
ORDER BY PName;


-- Q2) Find Level1_code wise Avg_Kill_Count where lives_earned is 2 and atleast
--    3 stages are crossed

SELECT 
L1_Code, 
AVG (Kill_Count)
FROM game_analysis.player_details, game_analysis.level_details2
WHERE level_details2.Lives_Earned =2 AND Stages_crossed >=3
GROUP BY L1_Code;


-- Q3) Find the total number of stages crossed at each diffuculty level
-- where for Level2 with players use zm_series devices. Arrange the result
-- in decsreasing order of total number of stages crossed.

SELECT 
SUM(Stages_crossed) AS 'total_number_of_stages_crossed',
Difficulty
FROM game_analysis.level_details2
LEFT JOIN player_details
ON player_details.P_ID = level_details2.P_ID
WHERE level_details2.Level= 2
AND level_details2.Dev_Id LIKE "zm%"
GROUP BY  Difficulty
ORDER BY 'total_number_of_stages_crossed' DESC;


-- Q4) Extract P_ID and the total number of unique dates for those players 
-- who have played games on multiple days.

SELECT 
P_ID,
COUNT(DISTINCT(start_datetime)) AS number_of_unique_dates
FROM game_analysis.level_details2
GROUP BY P_ID;



-- Q5) Find P_ID and level wise sum of kill_counts where kill_count
-- is greater than avg kill count for the Medium difficulty.

SELECT
P_ID,
Level,
SUM(Kill_Count) AS sum_of_kill_counts
FROM game_analysis.level_details2
WHERE Kill_Count > ( SELECT AVG(Kill_Count) FROM level_details2 WHERE Difficulty = "medium")
GROUP BY P_ID,Level;


-- Q6)  Find Level and its corresponding Level code wise sum of lives earned 
-- excluding level 0. Arrange in asecending order of level.

SELECT
Level,
L1_Code,
SUM(Lives_Earned)
FROM game_analysis.level_details2, game_analysis.player_details
WHERE level_details2.Level <> 0
GROUP BY Level, L1_Code
ORDER BY Level asc;


 
-- Q7) Find Top 3 score based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well. 

WITH new_table AS 
(SELECT 
Score,
Dev_Id,
Difficulty,
row_number() OVER(partition by Dev_Id order by Score ASC) AS Ranked
FROM level_details2)
SELECT Dev_Id,Score, Ranked from new_table
where Ranked <= 3;


-- Q8) Find first_login datetime for each device id

SELECT 
Dev_Id,
MIN(start_datetime)
FROM level_details2
GROUP BY Dev_Id;



-- Q9) Find Top 5 score based on each difficulty level and Rank them in 
-- increasing order using Rank. Display dev_id as well.
WITH new_table as
(SELECT
Score,
Difficulty,
Dev_Id,
rank() OVER(PARTITION BY Difficulty ORDER BY Score ASC) AS Ranked
FROM level_details2)
SELECT Score, Difficulty,Dev_Id, Ranked FROM new_table
WHERE Ranked <=5;



-- Q10) Find the device ID that is first logged in(based on start_datetime) 
-- for each player(p_id). Output should contain player id, device id and 
-- first login datetime.

SELECT
Dev_Id,
MIN(start_datetime),
P_ID
FROM level_details2
GROUP BY P_ID, Dev_Id, start_datetime
ORDER BY start_datetime ASC;



-- Q11) For each player and date, how many kill_count played so far by the player. That is, the total number of games played -- by the player until that date.
-- a) window function

SELECT distinct
P_ID,
CAST(start_datetime as Date) AS 'Date',
SUM(Kill_Count) OVER( partition by P_ID, CAST(start_datetime AS Date) ORDER BY CAST(start_datetime AS Date))
as 'sum_kill_count'
FROM level_details2
ORDER by P_ID, 'Date';

-- b) without window function

SELECT 
P_ID,
CAST(start_datetime as Date) AS 'Date',
SUM(Kill_Count)
FROM level_details2
GROUP BY P_ID, CAST(start_datetime as Date)
ORDER BY P_ID, 'Date';



-- Q12) Find the cumulative sum of stages crossed over a start_datetime 

WITH stage_progress AS
(SELECT
P_ID,
Stages_crossed,
start_datetime,
row_number() OVER( partition by P_ID order by start_datetime DESC) AS rn 
FROM level_details2)
SELECT 
P_ID, SUM(Stages_crossed),start_datetime FROM stage_progress
GROUP BY P_ID,start_datetime;


-- Q13) Find the cumulative sum of an stages crossed over a start_datetime 
-- for each player id but exclude the most recent start_datetime

WITH stage_progress as
(SELECT
P_ID,
Stages_crossed,
start_datetime,
row_number() OVER( partition by P_ID order by start_datetime DESC) AS rn 
FROM level_details2)
SELECT 
P_ID, SUM(Stages_crossed),start_datetime FROM stage_progress
Where rn > 1
GROUP BY P_ID,start_datetime;






-- Q14) Extract top 3 highest sum of score for each device id and the corresponding player_id

SELECT
Dev_Id,
P_ID,
SUM(Score) AS 'sum_score',
row_number() OVER( partition by Dev_Id ORDER BY sum(Score) ASC) AS 'Ranked'
FROM level_details2
WHERE 'Ranked' <= 3
GROUP BY P_ID,Dev_Id;





-- Q15) Find players who scored more than 50% of the avg score scored by sum of 
-- scores for each player_id

Select 
P_ID,
SUM(Score)
FROM level_details2
GROUP BY P_ID
having SUM(Score) > 0.5*( SELECT avg(Score) FROM level_details2);




-- Q16) Create a stored procedure to find top n headshots_count based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well.

DELIMITER $
CREATE PROCEDURE topN_headshot_counts()
BEGIN
SELECT
Dev_Id, 
Headshots_Count,
Difficulty,
row_number() OVER(partition by Dev_Id order by Headshots_Count) AS 'RANKED'
FROM level_details2
Where 'RANKED' <= 6;
END$
DELIMITER ;
CALL topN_headshot_counts() ;




-- Q17) Create a function to return sum of Score for a given player_id.

SELECT
P_ID,
SUM(Score) AS sum_of_score
FROM level_details2
GROUP BY P_ID;

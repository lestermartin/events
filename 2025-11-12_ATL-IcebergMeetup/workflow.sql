
-- FRED FLINTSTONE DIARY

SELECT starburst.ai.prompt(
  'Does Fred Flintstone like to send and receive mail?',
  'gpt-5-nano');


drop schema mycloud_us_east_1.fftest cascade;

create schema mycloud_us_east_1.fftest;
use mycloud_us_east_1.fftest;




-- create a table for diary entries
CREATE TABLE diary_entries (
    owner varchar,
    entry_date timestamp,
    entry_text varchar
);
show create table diary_entries;



-- add initial entries 
INSERT INTO diary_entries (owner, entry_date, entry_text)
VALUES
  ('Fred Flintstone', date_add('day', -99, current_timestamp), 'It is a new year and today I decided to start keeping a diary. Some may imagine that life is pretty boring here in Bedrock, but my family and friends make sure life is NEVER boring! As today is a Monday, I''m off to work at the quarry. I work there everyday until 5pm. Despite it being physical work, I do enjoy it. I was happy to play with Dino after work.'),
  ('Fred Flintstone', date_add('day', -98, current_timestamp), 'Ok, keeping a diary is something I''ll need to get used to. It seems I missed yesterday, but I''m still here. It is mid-week and I''m finally jotting down my notes this Wednesday evening. Barney and I just got back from our Loyal Order of Water Buffaloes meeting. Barney is running for Vice President this year and I fully support him.'),
  ('Fred Flintstone', date_add('day', -97, current_timestamp), 'Ahh... the weekend is finally here. I''ve been so busy (and well... I guess I simply forgot as well) to enter my thoughts into my daily journal. Off to the drive in movie tonight.'),
  ('Fred Flintstone', date_add('day', -96, current_timestamp), 'The weekends always go so fast. Family & friends continue to be the thing I spend my time on. I''m thinking I need a new hobby, too.'),
  ('Fred Flintstone', date_add('day', -95, current_timestamp), 'I have decided to try sending & receiving postcards as a new hobby. I found this website called Postcrossings that looks pretty interesting. The site says that over 800,000 people enjoy this hobby. Postcrossers, that''s what these hobbyists are called, have sent over 78 million postcards so far.'),
  ('Fred Flintstone', date_add('day', -94, current_timestamp), 'I was able to send 3 postcards today to people the system provided addresses to me for. This really is the hobby for me.'),
  ('Fred Flintstone', date_add('day', -93, current_timestamp), 'Fun Thursday night getting 2 more postcards in the mail.  I am loving this hobby even though Wilma thinks I''m nuts.'),
  ('Fred Flintstone', date_add('day', -92, current_timestamp), 'I went hiking over the weekend for one of my existing hobbies; waterfall viewing. I saw two of the largest ones in Bedrock; Big Falls and Prettybig Falls. I was even able to buy a few postcards at the gift shop.'),
  ('Fred Flintstone', date_add('day', -91, current_timestamp), 'I had so much fun looking at waterfalls that I visited the artificial one we have at the mall; Shopping Shoal. Silly name, but I did find out that shoals were usually rocks in rough water that paddlers enjoy traversing through.'),
  ('Fred Flintstone', date_add('day', -90, current_timestamp), 'This   was   a    second   journal entry for Jan 15. It shows    some   extra   spacing    that can    be cleaned up.'),
  ('Fred Flintstone', date_add('day', -89, current_timestamp), 'Woke up, fell out of bed. Dragged a comb across my head. Found my way downstairs and drank a cup and looking up, I noticed I was late. Found my coat and grabbed my hat. Made the bus in seconds flat. Found my way upstairs and had a smoke and somebody spoke and I went into a dream.'),
  ('Fred Flintstone', date_add('day', -88, current_timestamp), 'Woke up, fell out of bed. Dragged a comb across my head. Found my way downstairs and drank a cup and looking up, I noticed I was late. Found my coat and grabbed my hat. Made the bus in seconds flat. Found my way upstairs and had a smoke and somebody spoke and I went into a dream.'),
  ('Fred Flintstone', date_add('day', -87, current_timestamp), 'These last days have been a complete blur. So busy just life that I haven''t been able to focus on my new hobby of mailing postcards.');
select * from diary_entries;



-- prep to store vector embeddings 
--  'lakeside' right in the Iceberg table
ALTER TABLE diary_entries 
  ADD COLUMN entry_text_embeddings array(double);


-- create and persist the embeddings
UPDATE diary_entries
   SET entry_text_embeddings = 
         starburst.ai.generate_embedding(
             entry_text, 'text-embedding-3-small'
         )
 WHERE entry_text_embeddings IS NULL;
select * from diary_entries;


-- STEP 1

-- Retrieve the top 5 most relevant journal
--  entries based on semantic similarity
WITH vector_search AS(
    SELECT
        owner, entry_date, entry_text,
        cosine_similarity(
            starburst.ai.generate_embedding(
                'Does Fred Flintstone like to send and receive mail?', 
                'text-embedding-3-small'
            ),
            entry_text_embeddings
        ) AS similarity_score
    FROM diary_entries
    ORDER BY similarity_score DESC
    LIMIT 5
)
SELECT * FROM vector_search;




-- STEPS 1, 2

-- Retrieve the top 5 most relevant journal
--  entries based on semantic similarity
WITH vector_search AS(
    SELECT
        owner, entry_date, entry_text,
        cosine_similarity(
            starburst.ai.generate_embedding(
                'Does Fred Flintstone like to send and receive mail?', 
                'text-embedding-3-small'
            ),
            entry_text_embeddings
        ) AS similarity_score
    FROM diary_entries
    ORDER BY similarity_score DESC
    LIMIT 5
),
-- Augment the results by converting  
--  them into a JSON object
json_results AS (
SELECT CAST(map_agg(to_iso8601(entry_date), json_object(
    key 'journal entry date' VALUE entry_date,
    key 'journal entry text' VALUE entry_text)) AS JSON) AS json_data
FROM
    vector_search
)
SELECT * FROM json_results;


-- STEPS 1, 2, 3

-- Retrieve the top 5 most relevant journal
--  entries based on semantic similarity
WITH vector_search AS(
    SELECT
        owner, entry_date, entry_text,
        cosine_similarity(
            starburst.ai.generate_embedding(
                'Does Fred Flintstone like to send and receive mail?', 
                'text-embedding-3-small'
            ),
            entry_text_embeddings
        ) AS similarity_score
    FROM diary_entries
    ORDER BY similarity_score DESC
    LIMIT 5
),
-- Augment the results by converting  
--  them into a JSON object
json_results AS (
SELECT CAST(map_agg(to_iso8601(entry_date), json_object(
    key 'journal entry date' VALUE entry_date,
    key 'journal entry text' VALUE entry_text)) AS JSON) AS json_data
FROM
    vector_search
)
-- Generate an augmented response 
--  using the LLM
SELECT
    starburst.ai.prompt(concat(
        'Using the list of journal entries provided in JSON, ',
        'Does Fred Flintstone like to send and receive mail?',
        json_format(json_data)), 'gpt-5-nano')
FROM json_results;


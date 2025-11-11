-- https://docs.starburst.io/latest/starburst-ai/functions-ai.html


SELECT starburst.ai.prompt(
  'What underlying technology does Starburst use?',
  'gpt-5-nano');




-- The following is a reply to the post at https://www.reddit.com/r/dataengineering/comments/13ftlcp/experiences_with_trino_what_am_i_missing/
-- Trino/Presto/Athena whatever the branding is, is the most impressive data processing technology I've seen in my career. The only downside is that SQL is its only interface, but honestly I love it so much. I've been lucky enough to meet some of engineers that worked on it and they're some of the best engineers I've met in my career. If I was going to a Greenfield project, I'd 100% want this as the main part of our stack.

SELECT starburst.ai.analyze_sentiment(
    'I love Starburst', 
    'gpt-5-nano');

SELECT starburst.ai.analyze_sentiment(
    'Trino/Presto/Athena whatever the branding is, is the most impressive data processing technology I have seen in my career. The only downside is that SQL is its only interface, but honestly I love it so much. I have been lucky enough to meet some of engineers that worked on it and they are some of the best engineers I have met in my career. If I was going to a Greenfield project, I would 100% want this as the main part of our stack.', 
    'gpt-5-nano');

SELECT starburst.ai.classify(
    'Trino/Presto/Athena whatever the branding is, is the most impressive data processing technology I have seen in my career. The only downside is that SQL is its only interface, but honestly I love it so much. I have been lucky enough to meet some of engineers that worked on it and they are some of the best engineers I have met in my career. If I was going to a Greenfield project, I would 100% want this as the main part of our stack.', 
    ARRAY['spam', 'not spam'], 
    'gpt-5-nano');

SELECT starburst.ai.fix_grammar(
    'Trino/Presto/Athena whatever the branding is, is the most impressive data processing technology I have seen in my career. The only downside is that SQL is its only interface, but honestly I love it so much. I have been lucky enough to meet some of engineers that worked on it and they are some of the best engineers I have met in my career. If I was going to a Greenfield project, I would 100% want this as the main part of our stack.', 
    'gpt-5-nano');


SELECT starburst.ai.translate(
    'Trino/Presto/Athena whatever the branding is, is the most impressive data processing technology I have seen in my career. The only downside is that SQL is its only interface, but honestly I love it so much. I have been lucky enough to meet some of engineers that worked on it and they are some of the best engineers I have met in my career. If I was going to a Greenfield project, I would 100% want this as the main part of our stack.', 
    'es', 'gpt-5-nano');

SELECT starburst.ai.translate(
    'Trino/Presto/Athena whatever the branding is, is the most impressive data processing technology I have seen in my career. The only downside is that SQL is its only interface, but honestly I love it so much. I have been lucky enough to meet some of engineers that worked on it and they are some of the best engineers I have met in my career. If I was going to a Greenfield project, I would 100% want this as the main part of our stack.', 
    'zh-TW', 'gpt-5-nano');

SELECT starburst.ai.mask(
    'Contact me at 555-1234 or visit us at 123 Main St.',
    ARRAY['phone', 'address'], 
    'gpt-5-nano');





CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(255) NOT NULL,
  password VARCHAR(255) NOT NULL
);

COPY public.users (id, username, password) FROM STDIN WITH CSV;
9,user9,password9
10,user10,password10
11,user11,password11
12,user12,password12
\.

CREATE TABLE posts (
  id SERIAL PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  body TEXT NOT NULL,
  user_id INT REFERENCES users(id)
);

COPY public.posts (id, title, body, user_id) FROM STDIN WITH CSV;
17,title17,body17,12
18,title18,body18,1
19,title19,body19,9
20,title20,body20,2
21,title21,body21,6
22,title22,body22,9
23,title23,body23,11
24,title24,body24,3
\.

CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  body TEXT NOT NULL,
  post_id INT REFERENCES posts(id),
  user_id INT REFERENCES users(id)
);

COPY public.comments (id, body, post_id, user_id) FROM STDIN WITH CSV;
25,comment25,17,9
26,comment26,14,12
27,comment27,15,11
28,comment28,10,9
29,comment29,11,10
30,comment30,8,10
31,comment31,1,12
32,comment32,2,10
33,comment33,6,11
34,comment34,7,9
35,comment35,12,9
36,comment36,1,11
\.

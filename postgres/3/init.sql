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
1,title17,body17,12
2,title18,body18,10
3,title19,body19,11
4,title20,body20,10
5,title21,body21,11
6,title22,body22,9
7,title23,body23,9
8,title24,body24,12
\.

CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  body TEXT NOT NULL,
  post_id INT REFERENCES posts(id),
  user_id INT REFERENCES users(id)
);

COPY public.comments (id, body, post_id, user_id) FROM STDIN WITH CSV;
1,comment25,1,9
2,comment26,2,12
3,comment27,3,11
4,comment28,4,9
5,comment29,5,10
6,comment30,6,10
7,comment31,7,12
8,comment32,8,10
9,comment33,1,11
10,comment34,2,9
11,comment35,3,9
12,comment36,4,11
\.

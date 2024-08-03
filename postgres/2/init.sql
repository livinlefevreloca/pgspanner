CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(255) NOT NULL,
  password VARCHAR(255) NOT NULL
);

COPY public.users (id, username, password) FROM STDIN WITH CSV;
5,user5,password5
6,user6,password6
7,user7,password7
8,user8,password8
\.

CREATE TABLE posts (
  id SERIAL PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  body TEXT NOT NULL,
  user_id INT REFERENCES users(id)
);

COPY public.posts (id, title, body, user_id) FROM STDIN WITH CSV;
9,title9,body9,1
10,title10,body10,6
11,title11,body11,3
12,title12,body12,8
13,title13,body13,20
14,title14,body14,23
15,title15,body15,19
16,title16,body16,11
\.

CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  body TEXT NOT NULL,
  post_id INT REFERENCES posts(id),
  user_id INT REFERENCES users(id)
);

COPY public.comments (id, body, post_id, user_id) FROM STDIN WITH CSV;
13,comment13,1,2
14,comment14,2,3
15,commen15,3,1
16,commen16,4,4
17,comment17,5,7
18,comment18,6,7
19,comment19,7,6
20,comment20,8,8
21,comment21,1,11
22,comment22,2,10
23,comment23,3,12
24,comment24,4,9
\.

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
1,title9,body9,5
2,title10,body10,6
3,title11,body11,7
4,title12,body12,8
5,title13,body13,7
6,title14,body14,8
7,title15,body15,6
8,title16,body16,5
\.

CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  body TEXT NOT NULL,
  post_id INT REFERENCES posts(id),
  user_id INT REFERENCES users(id)
);

COPY public.comments (id, body, post_id, user_id) FROM STDIN WITH CSV;
1,comment13,1,6
2,comment14,2,8
3,commen15,3,7
4,commen16,4,5
5,comment17,5,7
6,comment18,6,7
7,comment19,7,6
8,comment20,8,5
9,comment21,1,6
10,comment22,2,7
11,comment23,3,8
12,comment24,4,8
\.

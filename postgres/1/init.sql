CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(255) NOT NULL,
  password VARCHAR(255) NOT NULL
);

COPY public.users (id, username, password) FROM STDIN WITH CSV;
1,user1,password1
2,user2,password2
3,user3,password3
4,user4,password4
\.

CREATE TABLE posts (
  id SERIAL PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  body TEXT NOT NULL,
  user_id INT REFERENCES users(id)
);

COPY public.posts (id, title, body, user_id) FROM STDIN WITH CSV;
1,title1,body1,1
2,title2,body2,2
3,title3,body3,3
4,title4,body4,4
5,title5,body5,1
6,title6,body6,2
7,title7,body7,3
8,title8,body8,4
\.

CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  body TEXT NOT NULL,
  post_id INT REFERENCES posts(id),
  user_id INT REFERENCES users(id)
);

COPY public.comments (id, body, post_id, user_id) FROM STDIN WITH CSV;
1,comment1,1,1
2,comment2,2,2
3,comment3,3,3
4,comment4,4,4
5,comment5,5,1
6,comment6,6,2
7,comment7,7,3
8,comment8,8,4
9,comment9,1,1
10,comment10,2,2
11,comment11,3,3
12,comment12,4,4
\.

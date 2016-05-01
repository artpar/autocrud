# autocrud with permissions
Make rest api for a database for curd with linux type permission (eg 755) at row level

Use this to expose CURD api for all the tables in your database

Works with
- Dropwizard
- Spring
- any other jersey server


IMPORTANT
this is not for guest users in your application
A filter should authenticate and insert a request property 

https://github.com/artpar/autocrud/blob/master/src/test/java/io/artpar/test/InsertDummyUserFilter.java

Examples:

Dropwizard
- https://github.com/artpar/autocrud/blob/master/src/test/java/io/artpar/test/TestService.java

Spring boot
- https://github.com/artpar/autocrud-springboot-example


Things to note:

1. This will modify your db to add couple of column required to main permissions
2. Every table should have these columns (will be created if not exists)
- id int(11) auto_increment primary key
- reference_id varchar(50) unique default uuid
- status
- created_at
- updated_at
- permission int(4) not null default 755
- user_id int(11) id of the user to which this belongs (by default the one who created at)
- usergroup_id int(11) of the usergroup to which this belongs (be default the group id of the user who created it)

3. An additional table by the name 'world' will be created to maintain the table permissions (table will also have user and usergroup owners)
- This will also be created if not exists.
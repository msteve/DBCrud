# DBCrud
Re-usable Java Class for performing CRUD database operations

for example for (select * from assets) you can use 

String table="assets";

ArrayList<HashMap<String, String>> resultList = new DbHandler().select(table);

The select method is overloaded,

e.g if you want to specify the columns use [ArrayList<String> cols]

and where clause in form of the hash map HashMap<String, String> where

E.g where.put("name","building"); = where name ="building"


So the expression can be 

ArrayList<HashMap<String, String>> resultList = new DbHandler().select(table,cols,where);

For  Update

String table="assets";
HashMap<String, String> set=new HashMap<String, String> ();
set.put("name","some name");

HashMap<String, String> where=new HashMap<String, String> ();
where.put("name","some name");

int update=new DbHandler().update(table,set,where);

There are case where you would want to evaluate some statements at the Sql Server level e.g where registration_date=getdate(),
then here in the where clause you can pass it as,
where.put(DbHandler.SQL_KEYWORD,"registration_date=getdate()");

And for if you feel like you already have a full SQl statement,
then you can use
ArrayList<HashMap<String, String>> result=selectQuery(sql,whereParams);

Enjoy

# DBCrud
Re-usable Java Class for performing CRUD database operations

for example for select * (select * from assets) use can use 

String table="assets";

ArrayList<HashMap<String, String>> resultList = new DbHandler().select(table);

The select method is overloaded,

e.g if you want to specify the columns use [ArrayList<String> cols]

and where clause in form of the hash map HashMap<String, String> where

E.g where.put("name","building"); = where name ="building"


So the expression can be 

ArrayList<HashMap<String, String>> resultList = new DbHandler().select(table,cols,where);

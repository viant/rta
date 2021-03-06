CREATE TABLE IF NOT EXISTS RTA_JN (ID INTEGER PRIMARY KEY, IP TEXT, BATCH_ID TEXT, STATUS INTEGER,TEMP_TABLE_NAME TEXT,CREATED DATETIME,UPDATED DATETIME);
CREATE TABLE IF NOT EXISTS INVENTORY (PRODUCT_ID INTEGER, NAME TEXT, QUANTITY INTEGER,PRICE FLOAT,UPDATED DATETIME, PRIMARY KEY (`PRODUCT_ID`,`NAME` ));
CREATE TABLE IF NOT EXISTS INVENTORY_127001_10000000000 (PRODUCT_ID INTEGER, NAME TEXT, QUANTITY INTEGER,PRICE FLOAT,UPDATED TEXT);
CREATE TABLE IF NOT EXISTS INVENTORY_127001_10000000001 (PRODUCT_ID INTEGER, NAME TEXT, QUANTITY INTEGER,PRICE FLOAT,UPDATED TEXT);
DELETE FROM  RTA_JN WHERE 1 = 1;
DELETE FROM  INVENTORY WHERE 1 = 1;
DELETE FROM  INVENTORY_127001_10000000000 WHERE 1 = 1;
DELETE FROM  INVENTORY_127001_10000000001 WHERE 1 = 1;
INSERT INTO RTA_JN(ID,IP,BATCH_ID,STATUS,TEMP_TABLE_NAME,CREATED,UPDATED) VALUES(1,'127.0.0.1','aaaa-bbbb-cccc-11111-22222',1,'INVENTORY_127001_10000000000','2022-03-03','2022-03-11');
INSERT INTO RTA_JN(ID,IP,BATCH_ID,STATUS,TEMP_TABLE_NAME,CREATED,UPDATED) VALUES(2,'127.0.0.1','aaaa-bbbb-cccc-11111-22223',1,'INVENTORY_127001_10000000001','2022-03-03','2022-03-12');
INSERT INTO INVENTORY_127001_10000000000(PRODUCT_ID,NAME,QUANTITY,PRICE,UPDATED) VALUES(1,'name1',12,0.55,'2022-03-11');
INSERT INTO INVENTORY_127001_10000000000(PRODUCT_ID,NAME,QUANTITY,PRICE,UPDATED) VALUES(2,'name1',13,0.65,'2022-03-11');
INSERT INTO INVENTORY_127001_10000000000(PRODUCT_ID,NAME,QUANTITY,PRICE,UPDATED) VALUES(7,'name2',25,6.651,'2022-03-11');
INSERT INTO INVENTORY_127001_10000000001(PRODUCT_ID,NAME,QUANTITY,PRICE,UPDATED) VALUES(1,'name1',6,0.75,'2022-03-12');



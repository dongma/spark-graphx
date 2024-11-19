// <= 1.将Neo4j点、投资边csv数据集上传到Hadoop文件系统中:
// 根目录下文件列表:   hadoop fs -ls /
// 在hadoop根目录下创建dataset文件夹:  hadoop fs -mkdir /dataset
//
// hdfs上传点、边数据文件: hadoop fs -put example-data/neo4j_dataset/company.csv  /dataset
// hadoop fs -put example-data/neo4j_dataset/person.csv  /dataset
// hadoop fs -put example-data/neo4j_dataset/pinvest_rel.csv  /dataset
// hadoop fs -put example-data/neo4j_dataset/cinvest_rel.csv  /dataset
// hdfs查看文件内容: hadoop fs -cat /dataset/company.csv
// hdfs删除文件: hadoop fs -rm /dataset/company.csv

// 2.Neo4j Node import: 导入Person人员节点，Company企业节点｜数据集在example-data/neo4j_dataset目录下
LOAD CSV WITH HEADERS FROM "file:/data/ent_dataset/person.csv" AS row
merge (person:Person {id:row.id}) ON CREATE set person.keyNo = row.keyNo,
  person.name = row.name, person.type = row.type;

LOAD CSV WITH HEADERS FROM "file:/data/ent_dataset/company.csv" AS row
merge (company:Company {id:row.id}) ON CREATE set company.keyNo = row.keyNo,
  company.name = row.name, company.label = row.label, company.status = row.status;

/**
  * => Neo4j Relation import: 导入投资、任职、法人的关系边，Note：在使用graphx进行图计算时, VertexId
  * 只能为Long类型. 在数据清洗时，应用VertexId替换掉目前的字符串作为唯一标识。
  */
LOAD CSV WITH HEADERS FROM "file:/data/ent_dataset/legal_rel.csv" AS row
match (legal:Person {id: row.from})
match (company:Company {id: row.to})
MERGE (legal)-[:LEGAL {role: row.role}]->(company);

LOAD CSV WITH HEADERS FROM "file:/data/ent_dataset/manager_rel.csv" AS row
match (staff:Person {id: row.from})
match (company:Company {id: row.to})
MERGE (staff)-[:MANAGER {role: row.role}]->(company);

LOAD CSV WITH HEADERS FROM "file:/data/ent_dataset/pinvest_rel.csv" AS row
match (invest:Person {id: row.from})
match (company:Company {id: row.to})
MERGE (invest)-[:P_INVEST {rate: row.rate}]->(company);

LOAD CSV WITH HEADERS FROM "file:/data/ent_dataset/cinvest_rel.csv" AS row
match (invest:Company {id: row.from})
match (company:Company {id: row.to})
MERGE (invest)-[:C_INVEST {holder: row.holder}]->(company);
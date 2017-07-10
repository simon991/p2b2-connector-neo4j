# Neo4j Connector

## 1. Setup of Neo4j database:

Install Neo4J ([documentation](https://neo4j.com/docs/operations-manual/current/installation/linux/debian/))
- make sure you have Java 8

```console
wget -O - https://debian.neo4j.org/neotechnology.gpg.key | sudo apt-key add -
echo 'deb https://debian.neo4j.org/repo stable/' | sudo tee /etc/apt/sources.list.d/neo4j.list
sudo apt-get update
sudo apt-get install neo4j
```

- make sure the nofile limit is high enough:
```console
root soft nofile 40000
root hard nofile 40000
```

## 2. First steps with Neo4j
### Start the database service
```console
sudo service neo4j start
```

- go to localhost:7474 (initial password is "neo4j")
- you will be asked to change the password. Change the password to "p2b2"


### Cypher Query Language
Cypher is Neo4j’s open graph query language. Cypher’s syntax provides a familiar way to match patterns of nodes and relationships in the graph.
- Use the input field in the web administration tool to do the following exercises:
- Create a node:
```console
CREATE (:Account { address: '0x482e3a38', value: 56})
CREATE (:Contract { address: '0x4326e3a38', value: 56})
```

- Create a constraint on the Account lable (this also creates an index on the Account address)
```console
CREATE CONSTRAINT ON (account:Account) ASSERT account.address IS UNIQUE;
CREATE CONSTRAINT ON (contract:Contract) ASSERT contract.address IS UNIQUE;
```

- If we would not have an index from the unique constraint above, we could create it this way to ensure their quick lookup when creating relationships in the next step.
```console
CREATE INDEX ON :Account(address);
CREATE INDEX ON :Contract(address);
```

- Create an edge:
```console
MATCH (a:Account {address:'0x482e3a38'}), (c:Contract {address:'0x4326e3a38'})
CREATE (a)-[:Transaction { value: 2 }]->(c)
```

- Create an index on the Transactions' ID
```console
TODO
```
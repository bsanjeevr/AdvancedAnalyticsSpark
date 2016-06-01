# AdvancedAnalyticsSpark

Advanced Spark Analytics

####Data Sets
#### Cintro1: Data Linkage
Raw download using eclipse in Windows: Donation.zip [http://bit.ly/1Aoywaq] 
Unzip and just using block 1 csv since running on local machine

### HDFS or Linux based
#### Cintro1: Data Linkage
`$ mkdir linkage`
`$ cd linkage/`
`$ curl -o donation.zip http://bit.ly/1Aoywaq`
`$ unzip donation.zip`
`$ unzip 'block_*.zip'`

### Putting the data in HDFS
#### Cintro1: Data Linkage
`$ hadoop fs -mkdir linkage`
`$ hadoop fs -put block_*.csv linkage`


#### Running mode
Using Spark-hadoop assembley jar in eclipse built 1.5.3 on scala 2.10

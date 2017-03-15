1. Spark works in the 'local[2]' mode.
2. It's possible to change some properties in receiver.properties, for example:
   # kafka broker hostname and port
   broker=127.0.0.1:6667
   #kafka topic name
   topic=alerts
   # how long to monitor 
   monitoringPeriodInSec=120
   # windowSizeInSec should be 60*60
   windowSizeInSec=3600
3. The jpcap library located in /lib/jpcap.jar is used for ip packets capturing. 
   Please, copy jpcap.dll into c:/Windows/System32

4. To start the program, type ' sbt run '.


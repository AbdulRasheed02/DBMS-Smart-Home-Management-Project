create table SENSOR(
Time int(11) unsigned,
TotalStartTime int(11) unsigned,
Total float,
Yesterday float,
Today float,
Period int,
Power float,
ApparentPower float,
ReactivePower float,
Factor float,
Voltage float,
Current float
);

create table STATE(
Time int(11) unsigned,
Uptime int(11) unsigned,
Uptimesec int,
Heap int,
SleepMode text,
Sleep int,
LoadAvg float,
MqttCount int,
HeapUsed int,
Objects int,
POWER varchar(20),
AP int,
SSId text,
BSSId text,
Channel int,
Mode text,
RSSI int,
`Signal` int,
LinkCount int,
Downtime int(11) unsigned
);
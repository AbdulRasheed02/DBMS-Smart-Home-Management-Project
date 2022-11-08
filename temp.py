import datetime

uptime = "8T23:09:17"

# The format we receive is
indexOfT = uptime.index('T')
day = int(uptime[:indexOfT])
hour = int(uptime[indexOfT+1:indexOfT+3])
minute = int(uptime[indexOfT+4:indexOfT+6])
second = int(uptime[indexOfT+7:])

totalUptimeinSeconds = int(datetime.timedelta(
    days=day, hours=hour, minutes=minute, seconds=second).total_seconds())

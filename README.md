# NSE-Data
## Table Structure
<p align="center">
    <img src =https://github.com/Ayush19-01/NSE-Data/blob/main/Table%20Structure.png>
</p>

## When the market is closed there is no data to found

<p align="center">
    <img src =https://github.com/Ayush19-01/NSE-Data/blob/main/creation%20and%20404.png>
</p>

## Top 25 gainers for One day and last 30 days
### Query
```mysql
select * from available_securities as ass,bhav  where ass.isinNumber=bhav.isinNumber AND timestamp=%s order by gainlos DESC limit 25
```

### Output

<p align="center">
    <img src =https://github.com/Ayush19-01/NSE-Data/blob/main/output%20format.png>
</p>



## Top 25 Gainer from last 30 Days
### Query 
``` mysql
select * from available_securities AT inner join (select A.isinNumber,((B.close - A.close)*100/A.close) as Gain from (select * from bhav where timestamp=%s) A inner join (select * from bhav where timestamp= %s) B on A.isinNumber=B.isinNumber) BT on AT.isinNumber=BT.isinNumber order by BT.Gain DESC limit 25
```

### Output
<p align="center">
    <img src =https://github.com/Ayush19-01/NSE-Data/blob/main/gainslast30.png>
</p>


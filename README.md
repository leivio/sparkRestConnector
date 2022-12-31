# sparkRestConnector
Rest connector developed in scale for spark
> Simple spark connector for reading data from rest [ get ] api.

**Example of use**

```
val df = spark
           .read
           .option("url", "https://gorest.co.in/public/v2/users")
           .format("com.leivio.myrestconnector")
           .load("")
display(df)
```
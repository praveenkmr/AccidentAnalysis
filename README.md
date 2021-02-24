# AccidentAnalysis
This repository contain Accident Analysis pipeline build in Pyspark.


#### Building for source

```sh
make build
```

#### Executing particular pipeline

```sh
spark-submit \
--py-files jobs.zip,shared.zip \
--files config.json \
main.py --job <job_id>
```




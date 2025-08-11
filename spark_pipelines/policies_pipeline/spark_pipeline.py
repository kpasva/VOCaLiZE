import argparse, os
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, sum as ssum

        def infer_reader(path: str):
            p = path.lower()
            if p.endswith('.csv') or p.endswith('.tsv'):
                return lambda spark: spark.read.option('header','true').option('inferSchema','true').option('delimiter','	' if p.endswith('.tsv') else ',').csv(path)
            if p.endswith('.json'):
                return lambda spark: spark.read.json(path)
            if p.endswith('.parquet'):
                return lambda spark: spark.read.parquet(path)
            if p.endswith('.xlsx') or p.endswith('.xls'):
                # Requires spark-excel if needed; fall back to CSV users
                raise RuntimeError('Excel not supported directly in PySpark without extra packages. Convert to CSV first.')
            return lambda spark: spark.read.csv(path, header=True, inferSchema=True)

        def main():
            parser = argparse.ArgumentParser()
            parser.add_argument('--input', required=True)
            parser.add_argument('--output', required=True)
            args = parser.parse_args()

            spark = SparkSession.builder.appName('VOCaLiZE_Pipeline').getOrCreate()
            reader = infer_reader(args.input)
            df = reader(spark)

            # Basic DQ
            rows = df.count()
            cols = len(df.columns)
            null_counts = {c: df.filter(col(c).isNull() | col(c).eqNullSafe(None)).count() for c in df.columns}
            dup_count = df.count() - df.dropDuplicates().count()
            print(f"Rows: {rows}, Columns: {cols}, Duplicates: {dup_count}")
            print('Null counts per column:', null_counts)
            df.printSchema()

            # Simple curation: drop duplicates
            df_cur = df.dropDuplicates()
            out = os.path.join(args.output, 'parquet')
            df_cur.write.mode('overwrite').parquet(out)

            # Write a DQ report
            report_path = os.path.join(args.output, 'dq_report.txt')
            with open(report_path, 'w') as f:
                f.write(f"Rows: {rows}
Columns: {cols}
Duplicates: {dup_count}
")
                for k, v in null_counts.items():
                    f.write(f"nulls.{k}={v}
")

            spark.stop()

        if __name__ == '__main__':
            main()
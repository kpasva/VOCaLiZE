import argparse, os, pandas as pd
from pathlib import Path

def dq_for_sample(pipeline_dir: Path) -> str:
    sample = pipeline_dir / 'tests' / 'sample.csv'
    if not sample.exists():
        # create a tiny sample if missing
        sample.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({'id':[1,2,2],'value':[10,None,30]}).to_csv(sample, index=False)
    df = pd.read_csv(sample)
    rows, cols = df.shape
    nulls = int(df.isna().sum().sum())
    dups = int(df.duplicated().sum())
    return f"Rows={rows}, Cols={cols}, Nulls={nulls}, Duplicates={dups}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--pipeline', required=True)
    args = ap.parse_args()
    summary = dq_for_sample(Path('spark_pipelines')/args.pipeline)
    print(summary)
    with open('dq_report_ci.txt','w') as f:
        f.write(summary)

if __name__ == '__main__':
    main()